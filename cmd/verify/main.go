// Command verify runs the acceptance smoke suite against a live service:
// it replays the causal scenarios (waiting on a cross-console dependency,
// cascade release, stable contention arbitration, idempotent replays and
// rejection classes) over the real HTTP API and exits non-zero on failure.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
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

func main() {
	if v := os.Getenv("APP_URL"); v != "" {
		baseURL = strings.TrimRight(v, "/")
	}
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
		"id":       "acceptance",
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
	code, out, err := submit("acceptance", b1)
	check("acc-b-001 → 202 waiting", err == nil && code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s err=%v", code, out.Status, err))
	check("等待原因指出缺失 alpha", strings.Contains(out.Reason, "alpha"), out.Reason)
	st, err := getRound("acceptance")
	check("等待项可见且缺失依赖为 alpha>=1", err == nil && len(st.Pending) == 1 && st.Pending[0].Missing["alpha"] == 1,
		fmt.Sprintf("%+v", st.Pending))
	check("等待期间阀门与前沿不变", st.Valves["GV1"] == "closed" && st.Frontier["beta"] == 0,
		fmt.Sprintf("valves=%+v frontier=%+v", st.Valves, st.Frontier))
	code, out, _ = submit("acceptance", b1)
	check("等待中重投同一事件 → 仍为 waiting", code == 202 && out.Status == "waiting",
		fmt.Sprintf("code=%d status=%s", code, out.Status))

	fmt.Println("[4] 补齐前驱 → 连续放行")
	a1 := event{EventID: "acc-a-001", Console: "alpha", Seq: 1,
		Deps:  map[string]int{"alpha": 0, "beta": 0, "gamma": 0},
		Valve: "GV2", Expected: "closed", NewState: "open"}
	code, out, err = submit("acceptance", a1)
	check("acc-a-001 → 200 applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("级联放行 acc-b-001（GV1 已打开）", out.Round != nil && out.Round.Valves["GV1"] == "open",
		fmt.Sprintf("valves=%+v", out.Round.Valves))
	check("前沿推进到 alpha=1,beta=1", out.Round != nil && out.Round.Frontier["alpha"] == 1 && out.Round.Frontier["beta"] == 1,
		fmt.Sprintf("frontier=%+v", out.Round.Frontier))
	check("等待项清空", out.Round != nil && len(out.Round.Pending) == 0)
	cascaded := findLog(*out.Round, "acc-b-001")
	check("裁决记录中 acc-b-001 为 applied", cascaded != nil && cascaded.Status == "applied")
	code, out, _ = submit("acceptance", b1)
	check("放行后重投 acc-b-001 → 返回既有结论 applied", code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound("acceptance")
	check("重投未造成重复消费", countLog(st, "acc-b-001") == 1, fmt.Sprintf("log entries=%d", countLog(st, "acc-b-001")))

	fmt.Println("[5] 并发争用同一旧状态 → 较小事件标识成功")
	a2 := event{EventID: "acc-a-010", Console: "alpha", Seq: 2,
		Deps:  map[string]int{"alpha": 1, "beta": 1, "gamma": 1},
		Valve: "GV2", Expected: "open", NewState: "closed"}
	b2 := event{EventID: "acc-b-010", Console: "beta", Seq: 2,
		Deps:  map[string]int{"alpha": 1, "beta": 1, "gamma": 1},
		Valve: "GV2", Expected: "open", NewState: "closed"}
	code, out, _ = submit("acceptance", a2)
	check("acc-a-010 等待 gamma 前驱", code == 202 && out.Status == "waiting", out.Status)
	code, out, _ = submit("acceptance", b2)
	check("acc-b-010 等待 gamma 前驱", code == 202 && out.Status == "waiting", out.Status)
	g1 := event{EventID: "acc-g-001", Console: "gamma", Seq: 1,
		Deps:  map[string]int{"gamma": 0},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, out, err = submit("acceptance", g1)
	check("acc-g-001 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	st, _ = getRound("acceptance")
	recA, recB := findLog(st, "acc-a-010"), findLog(st, "acc-b-010")
	check("较小标识 acc-a-010 放行", recA != nil && recA.Status == "applied",
		fmt.Sprintf("acc-a-010=%v", recA))
	check("较大标识 acc-b-010 稳定预条件拒绝", recB != nil && recB.Status == "rejected_precondition",
		fmt.Sprintf("acc-b-010=%v", recB))
	check("GV2 由胜者关闭", st.Valves["GV2"] == "closed", st.Valves["GV2"])
	check("败方因果位置同样推进 beta=2", st.Frontier["beta"] == 2, fmt.Sprintf("%+v", st.Frontier))

	fmt.Println("[6] 竞争事件的后继不被拒绝结果阻塞")
	code, out, _ = submit("acceptance", b2)
	check("重投 acc-b-010 → 稳定返回预条件拒绝", code == 200 && out.Status == "rejected_precondition",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	b3 := event{EventID: "acc-b-020", Console: "beta", Seq: 3,
		Deps:  map[string]int{"alpha": 2, "beta": 2, "gamma": 1},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	code, out, err = submit("acceptance", b3)
	check("beta 后继 acc-b-020 → applied", err == nil && code == 200 && out.Status == "applied",
		fmt.Sprintf("code=%d status=%s", code, out.Status))
	check("GV1 重新打开", out.Round != nil && out.Round.Valves["GV1"] == "open")

	fmt.Println("[7] 拒绝类别（均不得改变阀门状态）")
	reuse := a1
	reuse.Valve = "GV1" // 同一标识 acc-a-001，载荷变化
	code, eout, _ := submitErr("acceptance", reuse)
	check("标识复用而载荷变化 → 409", code == 409 && strings.Contains(eout.Reason, "different payload"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	gap := event{EventID: "acc-a-100", Console: "alpha", Seq: 5, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr("acceptance", gap)
	check("跳号 → 400 且说明期望序号", code == 400 && strings.Contains(eout.Reason, "gap"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	stale := event{EventID: "acc-a-101", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr("acceptance", stale)
	check("过期序号 → 400", code == 400 && strings.Contains(eout.Reason, "stale"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	ghost := event{EventID: "acc-x-001", Console: "delta", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr("acceptance", ghost)
	check("未知控制台 → 400", code == 400 && strings.Contains(eout.Reason, "unknown console"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	future := event{EventID: "acc-a-102", Console: "alpha", Seq: 3,
		Deps: map[string]int{"alpha": 3}, Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr("acceptance", future)
	check("未来依赖（依赖自身未来序号）→ 400", code == 400 && strings.Contains(eout.Reason, "future dependency"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	ghostDep := event{EventID: "acc-a-103", Console: "alpha", Seq: 3,
		Deps: map[string]int{"delta": 1}, Valve: "GV1", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr("acceptance", ghostDep)
	check("依赖未知控制台 → 400", code == 400 && strings.Contains(eout.Reason, "unknown console"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	badValve := event{EventID: "acc-a-104", Console: "alpha", Seq: 3, Deps: map[string]int{},
		Valve: "GVX", Expected: "open", NewState: "closed"}
	code, eout, _ = submitErr("acceptance", badValve)
	check("未知阀门 → 400", code == 400 && strings.Contains(eout.Reason, "unknown valve"),
		fmt.Sprintf("code=%d reason=%s", code, eout.Reason))
	code, _, _ = submitErr("no-such-round", a1)
	check("未知轮次 → 404", code == 404, fmt.Sprintf("code=%d", code))

	fmt.Println("[8] 最终因果状态可观察且未被拒绝操作污染")
	st, err = getRound("acceptance")
	check("GET /api/rounds/acceptance → 200", err == nil, fmt.Sprint(err))
	check("前沿 alpha=2,beta=3,gamma=1",
		st.Frontier["alpha"] == 2 && st.Frontier["beta"] == 3 && st.Frontier["gamma"] == 1,
		fmt.Sprintf("%+v", st.Frontier))
	check("阀门 GV1=open,GV2=closed", st.Valves["GV1"] == "open" && st.Valves["GV2"] == "closed",
		fmt.Sprintf("%+v", st.Valves))
	check("无遗留等待项", len(st.Pending) == 0, fmt.Sprintf("%+v", st.Pending))

	if failures > 0 {
		fmt.Printf("\nVERIFY FAILED: %d check(s) failed\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nVERIFY OK: all acceptance checks passed")
}
