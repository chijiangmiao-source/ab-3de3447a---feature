package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// runRestartSuite exercises crash/container-rebuild recovery over real
// HTTP: it starts its own app subprocess against a configurable data
// directory, kills it (SIGKILL, no graceful flush) with events waiting,
// restarts it and replays every causal scenario against the recovered
// state. A second restart with a corrupted WAL tail proves torn tails are
// rolled back to the last consistent state while rounds stay operable.
func runRestartSuite() {
	bin := os.Getenv("APP_BIN")
	if bin == "" {
		bin = "/usr/local/bin/app"
	}
	if _, err := os.Stat(bin); err != nil {
		fatalf("app binary not found at %s (set APP_BIN): %v", bin, err)
	}
	dataDir := os.Getenv("RESTART_DATA_DIR")
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "interlock-restart-data")
	}
	addr := os.Getenv("RESTART_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:18080"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fatalf("data dir %s: %v", dataDir, err)
	}
	old := baseURL
	baseURL = "http://" + addr
	defer func() { baseURL = old }()

	fmt.Printf("[restart] data directory: %s\n", dataDir)
	fmt.Printf("[restart] app binary:     %s\n", bin)

	fmt.Println("[R0] 首次启动（空数据目录）")
	app := startApp(bin, dataDir, addr)
	if err := waitHealthy(); err != nil {
		fatalf("initial start: %v", err)
	}

	fmt.Println("[R1] 建立轮次并制造：已放行 + 已拒绝 + 等待项")
	var created roundView
	code, err := doJSON("POST", "/api/rounds", map[string]any{
		"id":       "rec",
		"consoles": []string{"alpha", "beta", "gamma"},
		"valves":   map[string]string{"GV1": "closed", "GV2": "closed"},
	}, &created)
	check("POST /api/rounds → 201（创建已持久化）", err == nil && code == 201, fmt.Sprint(code, err))

	g1 := event{EventID: "rec-g1", Console: "gamma", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, out, err := submit("rec", g1)
	check("rec-g1 → 预条件拒绝（裁决将持久化）", err == nil && code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))

	g2 := event{EventID: "rec-g2", Console: "gamma", Seq: 2, Deps: map[string]int{"gamma": 1},
		Valve: "GV2", Expected: "closed", NewState: "open"}
	code, out, err = submit("rec", g2)
	check("rec-g2 → applied（阀门变更将持久化）", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))

	// p 依赖 gamma:3，崩溃前不会满足；它必须作为等待项熬过重启。
	p := event{EventID: "rec-pending", Console: "alpha", Seq: 1, Deps: map[string]int{"gamma": 3},
		Valve: "GV2", Expected: "open", NewState: "closed"}
	code, out, err = submit("rec", p)
	check("rec-pending → 202 waiting（等待项将持久化）", err == nil && code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ := getRound("rec")
	check("崩溃前状态：GV2=open, gamma=2, 一个等待项",
		st.Valves["GV2"] == "open" && st.Frontier["gamma"] == 2 && len(st.Pending) == 1,
		fmt.Sprintf("valves=%+v frontier=%+v pending=%d", st.Valves, st.Frontier, len(st.Pending)))

	fmt.Println("[R2] SIGKILL 杀掉进程（模拟进程退出/容器重建，无优雅落盘）")
	killApp(app)
	app = nil

	fmt.Println("[R3] 同数据目录重启 → 等待项/前沿/阀门/裁决原因完整恢复")
	app = startApp(bin, dataDir, addr)
	if err := waitHealthy(); err != nil {
		fatalf("restart: %v", err)
	}
	code, err = doJSON("GET", "/healthz", nil, nil)
	check("重启后 GET /healthz → 200", err == nil && code == 200, fmt.Sprint(code, err))
	req, _ := http.NewRequest("GET", baseURL+"/", nil)
	if resp, err := client.Do(req); err != nil {
		check("重启后 GET / → 200 控制台页面", false, err.Error())
	} else {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		check("重启后 GET / → 200 控制台页面", resp.StatusCode == 200 && strings.Contains(string(b), "联锁"))
	}
	code, body, err := doJSONRaw("GET", "/api/rounds", nil)
	check("重启后 GET /api/rounds 持续可用且轮次还在",
		err == nil && code == 200 && strings.Contains(string(body), `"rec"`),
		fmt.Sprintf("code=%d body=%s err=%v", code, string(body), err))

	st, err = getRound("rec")
	check("重启后阀门恢复 GV1=closed,GV2=open", err == nil && st.Valves["GV1"] == "closed" && st.Valves["GV2"] == "open",
		fmt.Sprintf("%+v", st.Valves))
	check("重启后前沿恢复 alpha=0,beta=0,gamma=2",
		st.Frontier["alpha"] == 0 && st.Frontier["beta"] == 0 && st.Frontier["gamma"] == 2,
		fmt.Sprintf("%+v", st.Frontier))
	check("重启后等待项仍在且缺失依赖 gamma>=3",
		len(st.Pending) == 1 && st.Pending[0].Event.EventID == "rec-pending" && st.Pending[0].Missing["gamma"] == 3,
		fmt.Sprintf("%+v", st.Pending))
	recG1 := findLog(st, "rec-g1")
	recG2 := findLog(st, "rec-g2")
	check("重启后既有结论恢复：g1 预条件拒绝（含原因）", recG1 != nil && recG1.Status == "rejected_precondition" &&
		strings.Contains(recG1.Reason, "precondition"), fmt.Sprintf("%+v", recG1))
	check("重启后既有结论恢复：g2 放行（含原因）", recG2 != nil && recG2.Status == "applied" &&
		strings.Contains(recG2.Reason, "released"), fmt.Sprintf("%+v", recG2))

	fmt.Println("[R4] 恢复前已裁决标识重投 → 原结论；复用标识改载荷 → 409")
	code, out, _ = submit("rec", g1)
	check("重投 rec-g1 → 稳定返回预条件拒绝", code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	code, out, _ = submit("rec", g2)
	check("重投 rec-g2 → 稳定返回 applied", code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound("rec")
	check("重投未重复裁决（日志仍为 2 条）", len(st.Log) == 2, fmt.Sprintf("log=%d", len(st.Log)))
	code, out, _ = submit("rec", p)
	check("等待项重投 → 仍 waiting", code == 202 && out.Status == "waiting", out.Status)
	changed := g2
	changed.NewState = "closed"
	code, eout, _ := submitErr("rec", changed)
	check("重启后标识复用改载荷 → 409", code == 409 && strings.Contains(eout.Reason, "different payload"),
		fmt.Sprintf("code=%d %s", code, eout.Reason))

	fmt.Println("[R5] 重启前后的等待事件争用同一旧状态 → 稳定顺序裁决")
	// 争用方在重启之后提交，与崩溃前等待的 rec-pending 同批可放行。
	c := event{EventID: "rec-cont", Console: "beta", Seq: 1, Deps: map[string]int{"gamma": 3},
		Valve: "GV2", Expected: "open", NewState: "closed"}
	code, out, _ = submit("rec", c)
	check("rec-cont（重启后提交）→ waiting", code == 202 && out.Status == "waiting", out.Status)
	g3 := event{EventID: "rec-g3", Console: "gamma", Seq: 3, Deps: map[string]int{"gamma": 2},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	code, out, err = submit("rec", g3)
	check("补交前驱 rec-g3 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound("rec")
	win, lose := findLog(st, "rec-cont"), findLog(st, "rec-pending")
	check("同批较小标识 rec-cont 放行（跨重启争用）", win != nil && win.Status == "applied",
		fmt.Sprintf("%+v", win))
	check("较大标识 rec-pending 稳定预条件拒绝", lose != nil && lose.Status == "rejected_precondition",
		fmt.Sprintf("%+v", lose))
	check("GV2 由胜者关闭", st.Valves["GV2"] == "closed", st.Valves["GV2"])
	code, out, _ = submit("rec", p)
	check("败方重投 → 原拒绝结论", code == 200 && out.Status == "rejected_precondition", out.Status)

	fmt.Println("[R6] 补交前驱后等待链连续放行，败方后继不被阻塞")
	succ := event{EventID: "rec-succ", Console: "alpha", Seq: 2,
		Deps:  map[string]int{"alpha": 1, "beta": 1, "gamma": 3},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, out, err = submit("rec", succ)
	check("败方后继 rec-succ → applied（因果位置已推进）", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound("rec")
	check("最终前沿 alpha=2,beta=1,gamma=3",
		st.Frontier["alpha"] == 2 && st.Frontier["beta"] == 1 && st.Frontier["gamma"] == 3,
		fmt.Sprintf("%+v", st.Frontier))
	preCrash := st

	fmt.Println("[R7] 再次 SIGKILL → 损坏 WAL 尾部 → 重启回退到最后一致状态")
	killApp(app)
	app = nil
	if err := appendGarbageToWAL(dataDir); err != nil {
		fatalf("corrupt wal: %v", err)
	}
	app = startApp(bin, dataDir, addr)
	if err := waitHealthy(); err != nil {
		fatalf("restart after corruption: %v", err)
	}
	st, err = getRound("rec")
	check("损坏尾部被回退：轮次保留且健康入口可用", err == nil, fmt.Sprint(err))
	check("回退后阀门为最后一致状态 GV1=closed,GV2=closed",
		st.Valves["GV1"] == "closed" && st.Valves["GV2"] == "closed",
		fmt.Sprintf("%+v want %+v", st.Valves, preCrash.Valves))
	check("回退后前沿不变 alpha=2,beta=1,gamma=3",
		st.Frontier["alpha"] == 2 && st.Frontier["beta"] == 1 && st.Frontier["gamma"] == 3,
		fmt.Sprintf("%+v", st.Frontier))
	check("回退后既有裁决全部保留（6 条）", len(st.Log) == 6,
		fmt.Sprintf("log=%d want=6", len(st.Log)))

	g4 := event{EventID: "rec-g4", Console: "gamma", Seq: 4, Deps: map[string]int{"gamma": 3},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	code, out, err = submit("rec", g4)
	check("回退后轮次可继续操作：rec-g4 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s err=%v", code, out.Status, err))

	fmt.Println("[R8] 第三次重启确认状态持续可恢复")
	killApp(app)
	app = startApp(bin, dataDir, addr)
	if err := waitHealthy(); err != nil {
		fatalf("final restart: %v", err)
	}
	code, out, _ = submit("rec", g4)
	check("再次重启后重投 rec-g4 → 原结论 applied", code == 200 && out.Status == "applied", out.Status)
	st, _ = getRound("rec")
	check("最终 GV1=open", st.Valves["GV1"] == "open", st.Valves["GV1"])
	killApp(app)
	app = nil

	if failures > 0 {
		fmt.Printf("\nRESTART VERIFY FAILED: %d check(s) failed\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nRESTART VERIFY OK: durability and recovery checks passed")
}

func startApp(bin, dataDir, addr string) *exec.Cmd {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "DATA_DIR="+dataDir, "LISTEN_ADDR="+addr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatalf("start app: %v", err)
	}
	return cmd
}

// killApp simulates a hard crash / container rebuild: SIGKILL with no
// grace period, so only fsynced WAL frames can survive.
func killApp(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// appendGarbageToWAL appends a non-frame byte tail to one round log,
// exactly what a torn write or sector corruption looks like at restart.
func appendGarbageToWAL(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			path := filepath.Join(dataDir, e.Name())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			_, werr := f.Write([]byte("VILW\x00\x00\x00\x05garbage-tail-without-checksum"))
			cerr := f.Close()
			if werr != nil {
				return werr
			}
			return cerr
		}
	}
	return fmt.Errorf("no .wal file found in %s", dataDir)
}

// doJSONRaw performs a JSON request and returns the raw response body.
func doJSONRaw(method, path string, body any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, baseURL+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, data, err
	}
	return resp.StatusCode, data, nil
}

func fatalf(format string, args ...any) {
	fmt.Printf("FATAL  "+format+"\n", args...)
	os.Exit(1)
}
