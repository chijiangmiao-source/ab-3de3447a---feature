// Command verify runs the acceptance smoke suite against a live service:
// it replays the causal scenarios (waiting on a cross-console dependency,
// cascade release, stable contention arbitration, idempotent replays and
// rejection classes) over the real HTTP API, then corrupts the commit-log
// tail, restarts the service process and replays the recovery scenarios
// (restored valves/frontier/waiting items/verdict reasons, stable
// conclusions on resubmission, waiting chain released by a late
// predecessor, stable arbitration across the restart). It exits non-zero
// on failure.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vacuum-interlock/internal/store"
)

var baseURL = "http://localhost:8080"

var failures int

func check(name string, ok bool, detail ...string) {
	if ok {
		fmt.Printf("  PASS  %s\n", name)
		return
	}
	failures++
	fmt.Printf("  FAIL  %s  %s\n", name, strings.Join(detail, " "))
}

type event struct {
	EventID  string         `json:"event_id"`
	Console  string         `json:"console"`
	Seq      int            `json:"seq"`
	Deps     map[string]int `json:"deps"`
	Valve    string         `json:"valve"`
	Expected string         `json:"expected_old"`
	NewState string         `json:"new_state"`
}

type record struct {
	Event    event  `json:"event"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
	Consumed bool   `json:"consumed"`
}

type pendingView struct {
	Event   event          `json:"event"`
	Missing map[string]int `json:"missing"`
	Reason  string         `json:"reason"`
}

type roundView struct {
	ID       string            `json:"id"`
	Consoles []string          `json:"consoles"`
	Valves   map[string]string `json:"valves"`
	Frontier map[string]int    `json:"frontier"`
	Pending  []pendingView     `json:"pending"`
	Log      []record          `json:"log"`
}

type submitResp struct {
	EventID  string     `json:"event_id"`
	Status   string     `json:"status"`
	Reason   string     `json:"reason"`
	Consumed bool       `json:"consumed"`
	Round    *roundView `json:"round"`
}

type errResp struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

var client = &http.Client{Timeout: 5 * time.Second}

func doJSON(method, path string, body any, out any) (int, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, baseURL+path, rdr)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode %s: %w", data, err)
		}
	}
	return resp.StatusCode, nil
}

func submit(round string, e event) (int, submitResp, error) {
	var out submitResp
	code, err := doJSON("POST", "/api/rounds/"+round+"/events", e, &out)
	return code, out, err
}

func submitErr(round string, e event) (int, errResp, error) {
	var out errResp
	code, err := doJSON("POST", "/api/rounds/"+round+"/events", e, &out)
	return code, out, err
}

func getRound(id string) (roundView, error) {
	var out roundView
	_, err := doJSON("GET", "/api/rounds/"+id, nil, &out)
	return out, err
}

func findLog(v roundView, eventID string) *record {
	for i := range v.Log {
		if v.Log[i].Event.EventID == eventID {
			return &v.Log[i]
		}
	}
	return nil
}

func countLog(v roundView, eventID string) int {
	n := 0
	for _, r := range v.Log {
		if r.Event.EventID == eventID {
			n++
		}
	}
	return n
}

func statusOf(v roundView, eventID string) string {
	if rec := findLog(v, eventID); rec != nil {
		return rec.Status
	}
	return "<missing>"
}

func pendingMissing(v roundView, eventID, console string, seq int) bool {
	for _, p := range v.Pending {
		if p.Event.EventID == eventID {
			return p.Missing[console] == seq && p.Reason != ""
		}
	}
	return false
}

func containsString(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func waitHealthy() error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if err == nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service at %s did not become healthy within 60s", baseURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// walContains reports whether the commit log already holds the needle —
// true only when the corresponding state was persisted before the success
// response reached us.
func walContains(path, needle string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return bytes.Contains(data, []byte(needle))
}

func walSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

// waitWalSize polls until the commit log has exactly the wanted size:
// after a restart this is how the recovery rollback of the corrupt tail
// becomes observable.
func waitWalSize(path string, want int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if walSize(path) == want {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// appendGarbage simulates a torn write at crash time: bytes that look like
// the start of a frame but were never completed, followed by junk.
func appendGarbage(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte("VIW1\x00\x00\x00\x00torn-frame-never-finished")); err != nil {
		return err
	}
	return f.Sync()
}

func main() {
	if v := os.Getenv("APP_URL"); v != "" {
		baseURL = strings.TrimRight(v, "/")
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}
	wal := filepath.Join(dataDir, store.FileName)
	// 每次验收使用全新的轮次标识：数据目录中可能躺着以往运行恢复的轮次。
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	accRound := "acc-" + run
	recRound := "rec-" + run

	fmt.Printf("[verify] waiting for service at %s ...\n", baseURL)
	if err := waitHealthy(); err != nil {
		fmt.Println("FAIL  health endpoint:", err)
		os.Exit(1)
	}

	fmt.Println("[1] 健康入口与页面")
	code, err := doJSON("GET", "/healthz", nil, nil)
	check("GET /healthz → 200", err == nil && code == 200, fmt.Sprint(code, err))
	req, _ := http.NewRequest("GET", baseURL+"/", nil)
	resp, err := client.Do(req)
	pageOK := false
	if err == nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		pageOK = resp.StatusCode == 200 && strings.Contains(string(b), "联锁")
	}
	check("GET / → 200 控制台页面", pageOK, fmt.Sprint(err))

	fmt.Println("[2] 建立轮次（3 控制台 + 2 阀门）")
	var created roundView
	code, err = doJSON("POST", "/api/rounds", map[string]any{
		"id":       accRound,
		"consoles": []string{"alpha", "beta", "gamma"},
		"valves":   map[string]string{"GV1": "closed", "GV2": "closed"},
	}, &created)
	check("POST /api/rounds → 201", err == nil && code == 201, fmt.Sprint(code, err))
	check("初始前沿全为 0", created.Frontier["alpha"] == 0 && created.Frontier["beta"] == 0 && created.Frontier["gamma"] == 0,
		fmt.Sprintf("%+v", created.Frontier))

	fmt.Println("[3] 依赖另一控制台的事件先到达 → 显示等待")
	b1 := event{EventID: "acc-b-001", Console: "beta", Seq: 1,
		Deps:  map[string]int{"alpha": 1, "beta": 0, "gamma": 0},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	code, out, err := submit(accRound, b1)
	check("acc-b-001 → 202 waiting", err == nil && code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s err=%v", code, out.Status, err))
	check("等待原因指出缺失 alpha", strings.Contains(out.Reason, "alpha"), out.Reason)
	st, err := getRound(accRound)
	check("等待项可见且缺失依赖为 alpha>=1", err == nil && len(st.Pending) == 1 && st.Pending[0].Missing["alpha"] == 1,
		fmt.Sprintf("%+v", st.Pending))
	check("等待期间阀门与前沿不变", st.Valves["GV1"] == "closed" && st.Frontier["beta"] == 0,
		fmt.Sprintf("valves=%+v frontier=%+v", st.Valves, st.Frontier))
	code, out, _ = submit(accRound, b1)
	check("等待中重投同一事件 → 仍为 waiting", code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s", code, out.Status))

	fmt.Println("[4] 补齐前驱 → 连续放行")
	a1 := event{EventID: "acc-a-001", Console: "alpha", Seq: 1,
		Deps:  map[string]int{"alpha": 0, "beta": 0, "gamma": 0},
		Valve: "GV2", Expected: "closed", NewState: "open"}
	code, out, err = submit(accRound, a1)
	check("acc-a-001 → 200 applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("级联放行 acc-b-001（GV1 已打开）", out.Round != nil && out.Round.Valves["GV1"] == "open",
		fmt.Sprintf("valves=%+v", out.Round.Valves))
	check("前沿推进到 alpha=1,beta=1", out.Round != nil && out.Round.Frontier["alpha"] == 1 && out.Round.Frontier["beta"] == 1,
		fmt.Sprintf("frontier=%+v", out.Round.Frontier))
	check("等待项清空", out.Round != nil && len(out.Round.Pending) == 0)
	cascaded := findLog(*out.Round, "acc-b-001")
	check("裁决记录中 acc-b-001 为 applied", cascaded != nil && cascaded.Status == "applied")
	code, out, _ = submit(accRound, b1)
	check("放行后重投 acc-b-001 → 返回既有结论 applied", code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound(accRound)
	check("重投未造成重复消费", countLog(st, "acc-b-001") == 1, fmt.Sprintf("log entries=%d", countLog(st, "acc-b-001")))

	fmt.Println("[5] 并发争用同一旧状态 → 较小事件标识成功")
	a2 := event{EventID: "acc-a-010", Console: "alpha", Seq: 2,
		Deps:  map[string]int{"alpha": 1, "beta": 1, "gamma": 1},
		Valve: "GV2", Expected: "open", NewState: "closed"}
	b2 := event{EventID: "acc-b-010", Console: "beta", Seq: 2,
		Deps:  map[string]int{"alpha": 1, "beta": 1, "gamma": 1},
		Valve: "GV2", Expected: "open", NewState: "closed"}
	code, out, _ = submit(accRound, a2)
	check("acc-a-010 等待 gamma 前驱", code == 202 && out.Status == "waiting", out.Status)
	code, out, _ = submit(accRound, b2)
	check("acc-b-010 等待 gamma 前驱", code == 202 && out.Status == "waiting", out.Status)
	g1 := event{EventID: "acc-g-001", Console: "gamma", Seq: 1,
		Deps:  map[string]int{"gamma": 0},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, out, err = submit(accRound, g1)
	check("acc-g-001 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound(accRound)
	recA, recB := findLog(st, "acc-a-010"), findLog(st, "acc-b-010")
	check("较小标识 acc-a-010 放行", recA != nil && recA.Status == "applied",
		fmt.Sprintf("acc-a-010=%v", recA))
	check("较大标识 acc-b-010 稳定预条件拒绝", recB != nil && recB.Status == "rejected_precondition",
		fmt.Sprintf("acc-b-010=%v", recB))
	check("GV2 由胜者关闭", st.Valves["GV2"] == "closed", st.Valves["GV2"])
	check("败方因果位置同样推进 beta=2", st.Frontier["beta"] == 2, fmt.Sprintf("%+v", st.Frontier))

	fmt.Println("[6] 竞争事件的后继不被拒绝结果阻塞")
	code, out, _ = submit(accRound, b2)
	check("重投 acc-b-010 → 稳定返回预条件拒绝", code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	b3 := event{EventID: "acc-b-020", Console: "beta", Seq: 3,
		Deps:  map[string]int{"alpha": 2, "beta": 2, "gamma": 1},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	code, out, err = submit(accRound, b3)
	check("beta 后继 acc-b-020 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("GV1 重新打开", out.Round != nil && out.Round.Valves["GV1"] == "open")

	fmt.Println("[7] 拒绝类别（均不得改变阀门状态）")
	reuse := a1
	reuse.Valve = "GV1" // 同一标识 acc-a-001，载荷变化
	code, eout, _ := submitErr(accRound, reuse)
	check("标识复用而载荷变化 → 409", code == 409 && strings.Contains(eout.Reason, "different payload"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	gap := event{EventID: "acc-a-100", Console: "alpha", Seq: 5, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr(accRound, gap)
	check("跳号 → 400 且说明期望序号", code == 400 && strings.Contains(eout.Reason, "gap"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	stale := event{EventID: "acc-a-101", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr(accRound, stale)
	check("过期序号 → 400", code == 400 && strings.Contains(eout.Reason, "stale"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	ghost := event{EventID: "acc-x-001", Console: "delta", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr(accRound, ghost)
	check("未知控制台 → 400", code == 400 && strings.Contains(eout.Reason, "unknown console"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	future := event{EventID: "acc-a-102", Console: "alpha", Seq: 3,
		Deps: map[string]int{"alpha": 3}, Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr(accRound, future)
	check("未来依赖（依赖自身未来序号）→ 400", code == 400 && strings.Contains(eout.Reason, "future dependency"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	ghostDep := event{EventID: "acc-a-103", Console: "alpha", Seq: 3,
		Deps: map[string]int{"delta": 1}, Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr(accRound, ghostDep)
	check("依赖未知控制台 → 400", code == 400 && strings.Contains(eout.Reason, "unknown console"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	badValve := event{EventID: "acc-a-104", Console: "alpha", Seq: 3, Deps: map[string]int{},
		Valve: "GVX", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr(accRound, badValve)
	check("未知阀门 → 400", code == 400 && strings.Contains(eout.Reason, "unknown valve"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	code, _, _ = submitErr("no-such-round", a1)
	check("未知轮次 → 404", code == 404, fmt.Sprintf("code=%d", code))

	fmt.Println("[8] 最终因果状态可观察且未被拒绝操作污染")
	st, err = getRound(accRound)
	check("GET 轮次状态 → 200", err == nil, fmt.Sprint(err))
	check("前沿 alpha=2,beta=3,gamma=1",
		st.Frontier["alpha"] == 2 && st.Frontier["beta"] == 3 && st.Frontier["gamma"] == 1,
		fmt.Sprintf("%+v", st.Frontier))
	check("阀门 GV1=open,GV2=closed", st.Valves["GV1"] == "open" && st.Valves["GV2"] == "closed",
		fmt.Sprintf("%+v", st.Valves))
	check("无遗留等待项", len(st.Pending) == 0, fmt.Sprintf("%+v", st.Pending))

	fmt.Println("[9] 持久化：成功响应返回前完整因果状态已落盘")
	code, err = doJSON("POST", "/api/rounds", map[string]any{
		"id":       recRound,
		"consoles": []string{"delta", "epsilon", "zeta", "iota"},
		"valves":   map[string]string{"GV5": "closed", "GV6": "closed"},
	}, &created)
	check("建立恢复轮次 → 201", err == nil && code == 201, fmt.Sprint(code, err))
	check("轮次创建在应答前已落盘", walContains(wal, recRound), "commit log missing round id")

	e1 := event{EventID: "rec-e-001", Console: "epsilon", Seq: 1,
		Deps:  map[string]int{"delta": 1},
		Valve: "GV6", Expected: "closed", NewState: "open"}
	code, out, err = submit(recRound, e1)
	check("rec-e-001 依赖 delta 前驱 → 202 waiting", err == nil && code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("等待事件在应答前已落盘", walContains(wal, "rec-e-001"), "commit log missing event")

	e2 := event{EventID: "rec-e-002", Console: "delta", Seq: 1,
		Deps:  map[string]int{},
		Valve: "GV5", Expected: "open", NewState: "closed"}
	code, out, err = submit(recRound, e2)
	check("rec-e-002 预条件不符 → rejected_precondition（前沿照样推进）",
		err == nil && code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("级联放行 rec-e-001（GV6 打开）且 GV5 未被拒绝事件改动",
		out.Round != nil && out.Round.Valves["GV6"] == "open" && out.Round.Valves["GV5"] == "closed",
		fmt.Sprintf("valves=%+v", out.Round.Valves))

	e3 := event{EventID: "rec-e-003", Console: "epsilon", Seq: 2,
		Deps:  map[string]int{"zeta": 1},
		Valve: "GV5", Expected: "open", NewState: "closed"}
	code, out, _ = submit(recRound, e3)
	check("rec-e-003 等待 zeta 前驱", code == 202 && out.Status == "waiting", out.Status)
	e4 := event{EventID: "rec-e-004", Console: "zeta", Seq: 1,
		Deps:  map[string]int{"delta": 2},
		Valve: "GV6", Expected: "open", NewState: "closed"}
	code, out, _ = submit(recRound, e4)
	check("rec-e-004 等待 delta#2", code == 202 && out.Status == "waiting", out.Status)
	e7 := event{EventID: "rec-e-007", Console: "iota", Seq: 1,
		Deps:  map[string]int{"delta": 2},
		Valve: "GV5", Expected: "open", NewState: "closed"}
	code, out, _ = submit(recRound, e7)
	check("rec-e-007 等待 delta#2（与 rec-e-003 争用 GV5 同一旧状态）", code == 202 && out.Status == "waiting", out.Status)
	check("全部等待项在重启前已落盘", walContains(wal, "rec-e-007"), "commit log missing event")

	before, err := getRound(recRound)
	check("重启前：3 个等待项、2 条裁决记录", err == nil && len(before.Pending) == 3 && len(before.Log) == 2,
		fmt.Sprintf("pending=%d log=%d err=%v", len(before.Pending), len(before.Log), err))
	walSizeBefore := walSize(wal)
	check("提交日志存在且非空", walSizeBefore > 0, fmt.Sprintf("size=%d", walSizeBefore))

	fmt.Println("[10] 截断/损坏尾部 + 进程重启 → 回退到最后一致状态")
	check("模拟崩溃撕裂写入（向提交日志追加损坏尾部）", appendGarbage(wal) == nil)
	code, err = doJSON("POST", "/admin/restart", nil, nil)
	check("POST /admin/restart → 200", err == nil && code == 200, fmt.Sprint(code, err))
	check("重启后服务恢复健康", waitHealthy() == nil)
	check("损坏尾部已回退到最后一致提交", waitWalSize(wal, walSizeBefore, 15*time.Second),
		fmt.Sprintf("before=%d now=%d", walSizeBefore, walSize(wal)))
	check("恢复后服务持续健康", waitHealthy() == nil)

	code, err = doJSON("GET", "/healthz", nil, nil)
	check("健康入口重启后可用", err == nil && code == 200, fmt.Sprint(code, err))
	resp, err = client.Get(baseURL + "/")
	pageOK = false
	if err == nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		pageOK = resp.StatusCode == 200 && strings.Contains(string(b), "联锁")
	}
	check("控制台页面重启后可用", pageOK, fmt.Sprint(err))

	after, err := getRound(recRound)
	check("恢复后轮次可重新打开", err == nil, fmt.Sprint(err))
	check("阀门恢复 GV5=closed,GV6=open", after.Valves["GV5"] == "closed" && after.Valves["GV6"] == "open",
		fmt.Sprintf("%+v", after.Valves))
	check("前沿恢复 delta=1,epsilon=1,zeta=0,iota=0",
		after.Frontier["delta"] == 1 && after.Frontier["epsilon"] == 1 &&
			after.Frontier["zeta"] == 0 && after.Frontier["iota"] == 0,
		fmt.Sprintf("%+v", after.Frontier))
	check("等待项与缺失依赖恢复（含等待原因）", len(after.Pending) == 3 &&
		pendingMissing(after, "rec-e-003", "zeta", 1) &&
		pendingMissing(after, "rec-e-004", "delta", 2) &&
		pendingMissing(after, "rec-e-007", "delta", 2),
		fmt.Sprintf("%+v", after.Pending))
	recE1 := findLog(after, "rec-e-001")
	check("已消费的 rec-e-001 未被重新裁决（仍 applied）", recE1 != nil && recE1.Status == "applied",
		fmt.Sprintf("%+v", recE1))
	recE2 := findLog(after, "rec-e-002")
	check("rec-e-002 的预条件拒绝结论与裁决原因恢复",
		recE2 != nil && recE2.Status == "rejected_precondition" && strings.Contains(recE2.Reason, "precondition"),
		fmt.Sprintf("%+v", recE2))
	var list struct {
		Rounds []string `json:"rounds"`
	}
	code, err = doJSON("GET", "/api/rounds", nil, &list)
	check("轮次列表接口恢复且包含全部既有轮次", err == nil && code == 200 &&
		containsString(list.Rounds, recRound) && containsString(list.Rounds, accRound),
		fmt.Sprintf("rounds=%v", list.Rounds))

	fmt.Println("[11] 重启后重放：既有结论稳定，等待链连续放行")
	code, out, _ = submit(recRound, e1)
	check("重投已放行的 rec-e-001 → 原结论 applied", code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	code, out, _ = submit(recRound, e2)
	check("重投已拒绝的 rec-e-002 → 原结论 rejected_precondition", code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	code, out, _ = submit(recRound, e3)
	check("重投等待中的 rec-e-003 → 仍 waiting", code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound(recRound)
	check("重投未造成重复消费", countLog(st, "rec-e-001") == 1 && countLog(st, "rec-e-002") == 1)
	reuseChanged := e2
	reuseChanged.NewState = "open"
	code, eout, _ = submitErr(recRound, reuseChanged)
	check("重启后复用标识改载荷 → 仍 409", code == 409 && strings.Contains(eout.Reason, "different payload"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))

	e5 := event{EventID: "rec-e-005", Console: "delta", Seq: 2,
		Deps:  map[string]int{},
		Valve: "GV5", Expected: "closed", NewState: "open"}
	code, out, err = submit(recRound, e5)
	check("补交前驱 rec-e-005 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound(recRound)
	check("原等待链连续放行后清空", len(st.Pending) == 0, fmt.Sprintf("%+v", st.Pending))
	check("rec-e-004 放行（GV6 关闭）", statusOf(st, "rec-e-004") == "applied", statusOf(st, "rec-e-004"))
	check("rec-e-003 放行（争用 GV5 胜于较大标识）", statusOf(st, "rec-e-003") == "applied", statusOf(st, "rec-e-003"))
	check("rec-e-007 稳定预条件拒绝", statusOf(st, "rec-e-007") == "rejected_precondition", statusOf(st, "rec-e-007"))
	check("前沿推进 delta=2,epsilon=2,zeta=1,iota=1",
		st.Frontier["delta"] == 2 && st.Frontier["epsilon"] == 2 && st.Frontier["zeta"] == 1 && st.Frontier["iota"] == 1,
		fmt.Sprintf("%+v", st.Frontier))
	check("阀门 GV5=closed,GV6=closed", st.Valves["GV5"] == "closed" && st.Valves["GV6"] == "closed",
		fmt.Sprintf("%+v", st.Valves))
	code, out, _ = submit(recRound, e7)
	check("重投 rec-e-007 → 稳定返回预条件拒绝", code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("重启后的新提交继续落盘", walSize(wal) > walSizeBefore, fmt.Sprintf("size=%d", walSize(wal)))

	e8 := event{EventID: "rec-e-008", Console: "zeta", Seq: 2,
		Deps:  map[string]int{"delta": 3},
		Valve: "GV5", Expected: "closed", NewState: "open"}
	code, out, _ = submit(recRound, e8)
	check("rec-e-008 等待 delta#3", code == 202 && out.Status == "waiting", out.Status)
	e9 := event{EventID: "rec-e-009", Console: "iota", Seq: 2,
		Deps:  map[string]int{"delta": 3},
		Valve: "GV5", Expected: "closed", NewState: "open"}
	code, out, _ = submit(recRound, e9)
	check("rec-e-009 等待 delta#3", code == 202 && out.Status == "waiting", out.Status)
	e10 := event{EventID: "rec-e-010", Console: "delta", Seq: 3,
		Deps:  map[string]int{},
		Valve: "GV6", Expected: "closed", NewState: "open"}
	code, out, err = submit(recRound, e10)
	check("rec-e-010 → applied 并触发同批争用裁决", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound(recRound)
	check("重启后争用：较小标识 rec-e-008 放行", statusOf(st, "rec-e-008") == "applied", statusOf(st, "rec-e-008"))
	check("重启后争用：rec-e-009 稳定预条件拒绝", statusOf(st, "rec-e-009") == "rejected_precondition", statusOf(st, "rec-e-009"))
	check("最终阀门 GV5=open,GV6=open", st.Valves["GV5"] == "open" && st.Valves["GV6"] == "open",
		fmt.Sprintf("%+v", st.Valves))

	a3 := event{EventID: "acc-a-200", Console: "alpha", Seq: 3,
		Deps:  map[string]int{},
		Valve: "GV2", Expected: "closed", NewState: "open"}
	code, out, err = submit(accRound, a3)
	check("重启前建立的既有轮次在重启后可继续消费", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound(accRound)
	check("既有轮次前沿继续推进 alpha=3", st.Frontier["alpha"] == 3, fmt.Sprintf("%+v", st.Frontier))

	if failures > 0 {
		fmt.Printf("\nVERIFY FAILED: %d check(s) failed\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nVERIFY OK: all acceptance checks passed")
}
