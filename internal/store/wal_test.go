package store

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"vacuum-interlock/internal/causality"
)

func newTempStore(t *testing.T) (*causality.Engine, *Store, string) {
	t.Helper()
	dir := t.TempDir()
	eng := causality.NewEngine()
	st, err := Open(dir, eng)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	eng.AttachStore(st)
	return eng, st, dir
}

func reopen(t *testing.T, dir string) (*causality.Engine, *Store) {
	t.Helper()
	eng := causality.NewEngine()
	st, err := Open(dir, eng)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	eng.AttachStore(st)
	return eng, st
}

func mkEvent(id, console string, seq int, deps map[string]int, valve, expected, newState string) causality.Event {
	return causality.Event{EventID: id, Console: console, Seq: seq, Deps: deps,
		Valve: valve, Expected: expected, NewState: newState}
}

// 基本持久化：创建轮次、等待、补齐前驱、级联放行，重启后状态与裁决完整恢复。
func TestPersistRoundAndRecover(t *testing.T) {
	eng, st, dir := newTempStore(t)
	if _, err := eng.CreateRound("r1", []string{"alpha", "beta"},
		map[string]string{"GV1": "closed", "GV2": "closed"}); err != nil {
		t.Fatal(err)
	}
	b1 := mkEvent("b1", "beta", 1, map[string]int{"alpha": 1}, "GV1", "closed", "open")
	if rec, err := eng.Submit("r1", b1); err != nil || rec.Status != causality.StatusWaiting {
		t.Fatalf("waiting: %v %v", rec, err)
	}
	a1 := mkEvent("a1", "alpha", 1, map[string]int{}, "GV2", "closed", "open")
	if rec, err := eng.Submit("r1", a1); err != nil || rec.Status != causality.StatusApplied {
		t.Fatalf("applied: %v %v", rec, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	view, err := eng2.View("r1")
	if err != nil {
		t.Fatalf("round lost after recovery: %v", err)
	}
	if view.Valves["GV1"] != "open" || view.Valves["GV2"] != "open" {
		t.Fatalf("valves not recovered: %+v", view.Valves)
	}
	if view.Frontier["alpha"] != 1 || view.Frontier["beta"] != 1 {
		t.Fatalf("frontier not recovered: %+v", view.Frontier)
	}
	if len(view.Pending) != 0 {
		t.Fatalf("pending should be empty: %+v", view.Pending)
	}
	verdicts := map[string]causality.Status{}
	for _, r := range view.Log {
		verdicts[r.Event.EventID] = r.Status
	}
	if verdicts["a1"] != causality.StatusApplied || verdicts["b1"] != causality.StatusApplied {
		t.Fatalf("verdicts not recovered: %+v", verdicts)
	}
	// 已消费事件重投 → 原结论，不重复消费
	rec, err := eng2.Submit("r1", b1)
	if err != nil || rec.Status != causality.StatusApplied {
		t.Fatalf("replay after restart must return applied: %v %v", rec, err)
	}
	view2, _ := eng2.View("r1")
	if n := len(view2.Log); n != 2 {
		t.Fatalf("replay must not duplicate verdicts, log=%d", n)
	}
	// 标识复用改载荷 → 冲突
	changed := b1
	changed.NewState = "closed"
	var ce *causality.ConflictError
	if _, err := eng2.Submit("r1", changed); !errors.As(err, &ce) {
		t.Fatalf("id reuse after restart must conflict, got %v", err)
	}
	// 恢复后可继续操作：后继事件正常放行
	a2 := mkEvent("a2", "alpha", 2, map[string]int{"alpha": 1, "beta": 1}, "GV1", "open", "closed")
	if rec, err := eng2.Submit("r1", a2); err != nil || rec.Status != causality.StatusApplied {
		t.Fatalf("successor after restart: %v %v", rec, err)
	}
}

// 等待项本身必须持久化：只有 accepted 帧、重启后仍为 waiting，可补交前驱。
func TestWaitingSurvivesRestart(t *testing.T) {
	eng, st, dir := newTempStore(t)
	eng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	b1 := mkEvent("b1", "beta", 1, map[string]int{"alpha": 1}, "GV1", "closed", "open")
	if rec, _ := eng.Submit("r1", b1); rec.Status != causality.StatusWaiting {
		t.Fatalf("expected waiting, got %s", rec.Status)
	}
	st.Close()

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	view, _ := eng2.View("r1")
	if len(view.Pending) != 1 || view.Pending[0].Event.EventID != "b1" {
		t.Fatalf("pending not recovered: %+v", view.Pending)
	}
	if view.Frontier["beta"] != 0 || view.Valves["GV1"] != "closed" {
		t.Fatalf("waiting state must not advance causal state: %+v", view)
	}
	// 重投等待事件 → 仍 waiting（幂等）
	if rec, err := eng2.Submit("r1", b1); err != nil || rec.Status != causality.StatusWaiting {
		t.Fatalf("resubmit waiting event: %v %v", rec, err)
	}
	// 补交前驱 → 等待链连续放行
	a1 := mkEvent("a1", "alpha", 1, map[string]int{}, "GV1", "closed", "open")
	eng2.Submit("r1", a1)
	view, _ = eng2.View("r1")
	if view.Valves["GV1"] != "open" || view.Frontier["beta"] != 1 || len(view.Pending) != 0 {
		t.Fatalf("chain did not release after recovery: %+v", view)
	}
}

// 崩溃发生在提交批次写入途中：整个未完成批次回滚，等待项保留且可继续。
func TestTruncatedCommittedBatchRollsBack(t *testing.T) {
	eng, st, dir := newTempStore(t)
	eng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	b1 := mkEvent("b1", "beta", 1, map[string]int{"alpha": 1}, "GV1", "closed", "open")
	eng.Submit("r1", b1) // accepted frame durable
	st.Close()

	// 追加一个被腰斩的 committed 帧（模拟写盘途中进程退出/容器重建）
	wf, err := os.OpenFile(onlyWal(t, dir), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	good, err := encodeFrame(kindCommitted, struct {
		Entries []causality.CommittedEntry `json:"entries"`
	}{Entries: []causality.CommittedEntry{{
		Event:  mkEvent("a1", "alpha", 1, map[string]int{}, "GV1", "closed", "open"),
		Status: causality.StatusApplied, ValveAfter: "open",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Write(good[:len(good)/2]); err != nil { // half a frame
		t.Fatal(err)
	}
	wf.Sync()
	wf.Close()

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	view, _ := eng2.View("r1")
	if len(view.Pending) != 1 || view.Frontier["alpha"] != 0 || view.Valves["GV1"] != "closed" {
		t.Fatalf("torn batch must roll back fully: %+v", view)
	}
	// 文件物理截断到最后一致位置；补交前驱后链照常放行
	a1 := mkEvent("a1", "alpha", 1, map[string]int{}, "GV1", "closed", "open")
	if rec, err := eng2.Submit("r1", a1); err != nil || rec.Status != causality.StatusApplied {
		t.Fatalf("resubmit after rollback: %v %v", rec, err)
	}
	view, _ = eng2.View("r1")
	if view.Valves["GV1"] != "open" || len(view.Pending) != 0 {
		t.Fatalf("chain must release after rollback: %+v", view)
	}
}

// 校验和损坏（位翻转）→ 该帧及其后内容被拒绝，回退到最后一致状态。
func TestCorruptedChecksumRollsBack(t *testing.T) {
	eng, st, dir := newTempStore(t)
	eng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	e1 := mkEvent("e1", "alpha", 1, map[string]int{}, "GV1", "closed", "open")
	eng.Submit("r1", e1)
	e2 := mkEvent("e2", "beta", 1, map[string]int{}, "GV1", "open", "closed")
	eng.Submit("r1", e2)
	st.Close()

	path := onlyWal(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 翻转最后一帧载荷中的一个字节（e2 的 new_state 首字节）
	pos := len(data) - checksumLen - 2
	data[pos] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	view, _ := eng2.View("r1")
	// 最后一个批次回滚：e1 仍在，e2 的裁决消失，beta 前沿为 0；阀门保留 e1 的 open
	verdicts := map[string]bool{}
	for _, r := range view.Log {
		verdicts[r.Event.EventID] = true
	}
	if !verdicts["e1"] || verdicts["e2"] {
		t.Fatalf("only the corrupt batch must roll back: %+v", verdicts)
	}
	if view.Frontier["beta"] != 0 || view.Valves["GV1"] != "open" {
		t.Fatalf("state after checksum rollback wrong: %+v", view)
	}
	// 被回滚的提交可以原样重新提交并得到裁决
	if rec, err := eng2.Submit("r1", e2); err != nil || rec.Status != causality.StatusApplied {
		t.Fatalf("resubmit after checksum rollback: %v %v", rec, err)
	}
}

// 语义伪造：校验通过但裁决内容与引擎重算不一致（标记 applied 而旧状态不符）
// → 帧被拒绝并回滚，绝不接纳不可证明完整的提交。
func TestSemanticForgeryRollsBack(t *testing.T) {
	eng, st, dir := newTempStore(t)
	eng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	st.Close()

	// 伪造：事件期望旧状态 open（实际 closed），却记录为 applied
	forged := encodeFrameOrDie(t, kindCommitted, struct {
		Entries []causality.CommittedEntry `json:"entries"`
	}{Entries: []causality.CommittedEntry{{
		Event:  mkEvent("evil", "alpha", 1, map[string]int{}, "GV1", "open", "closed"),
		Status: causality.StatusApplied, ValveAfter: "closed",
	}}})
	f, err := os.OpenFile(onlyWal(t, dir), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(forged); err != nil {
		t.Fatal(err)
	}
	f.Close()

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	view, _ := eng2.View("r1")
	if len(view.Log) != 0 || view.Valves["GV1"] != "closed" || view.Frontier["alpha"] != 0 {
		t.Fatalf("forged frame must be rejected: %+v", view)
	}
	// 轮次仍然可继续操作
	good := mkEvent("good", "alpha", 1, map[string]int{}, "GV1", "closed", "open")
	if rec, err := eng2.Submit("r1", good); err != nil || rec.Status != causality.StatusApplied {
		t.Fatalf("round must remain operable after forgery rollback: %v %v", rec, err)
	}
}

// accepted 帧伪造（事件在该前沿其实可消费，不应处于 waiting）→ 回滚。
func TestAcceptedFrameSemanticallyWrongRollsBack(t *testing.T) {
	eng, st, dir := newTempStore(t)
	eng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	st.Close()
	// alpha#1 无依赖，本该立即消费，却被写成 accepted(waiting)
	raw := encodeFrameOrDie(t, kindAccepted, mkEvent("x", "alpha", 1, map[string]int{}, "GV1", "closed", "open"))
	f, err := os.OpenFile(onlyWal(t, dir), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(raw)
	f.Close()

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	view, _ := eng2.View("r1")
	if len(view.Pending) != 0 || len(view.Log) != 0 {
		t.Fatalf("non-waiting accepted frame must be rejected: %+v %+v", view.Pending, view.Log)
	}
}

// 多个轮次各自独立恢复，且空数据目录可正常启动。
func TestMultipleRoundsAndEmptyDir(t *testing.T) {
	dir := t.TempDir()
	eng := causality.NewEngine()
	st, err := Open(dir, eng)
	if err != nil {
		t.Fatal(err)
	}
	eng.AttachStore(st)
	eng.CreateRound("round-a", []string{"a", "b"}, map[string]string{"V": "closed"})
	eng.CreateRound("round-b", []string{"c", "d"}, map[string]string{"V": "closed"})
	eng.Submit("round-a", mkEvent("a1", "a", 1, map[string]int{}, "V", "closed", "open"))
	st.Close()

	eng2, st2 := reopen(t, dir)
	defer st2.Close()
	ids := eng2.ListRounds()
	if len(ids) != 2 || ids[0] != "round-a" || ids[1] != "round-b" {
		t.Fatalf("rounds not recovered: %v", ids)
	}
	view, _ := eng2.View("round-a")
	if view.Valves["V"] != "open" {
		t.Fatalf("round-a state lost: %+v", view.Valves)
	}
	view, _ = eng2.View("round-b")
	if view.Valves["V"] != "closed" {
		t.Fatalf("round-b state leaked: %+v", view.Valves)
	}

	// 空目录也能打开
	empty := t.TempDir()
	eng3 := causality.NewEngine()
	st3, err := Open(empty, eng3)
	if err != nil {
		t.Fatalf("empty dir: %v", err)
	}
	st3.Close()
}

// 轮次文件名对特殊 id 安全（无路径穿越）。
func TestFileNameSafe(t *testing.T) {
	for _, id := range []string{"../escape", "a/b", "..", "", "正常轮次", "very-long-id-" + repeat("x", 80)} {
		name := fileNameFor(id)
		if filepath.Base(name) != name {
			t.Fatalf("unsafe file name for %q: %q", id, name)
		}
		if len(name) > 60 {
			t.Fatalf("file name too long for %q: %d", id, len(name))
		}
	}
	if fileNameFor("r1") == fileNameFor("r2") {
		t.Fatal("distinct round ids must map to distinct files")
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func onlyWal(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var wals []string
	for _, e := range entries {
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".wal" {
			wals = append(wals, filepath.Join(dir, e.Name()))
		}
	}
	if len(wals) != 1 {
		t.Fatalf("expected exactly one wal file, got %v", wals)
	}
	return wals[0]
}

func encodeFrameOrDie(t *testing.T, kind byte, payload any) []byte {
	t.Helper()
	b, err := encodeFrame(kind, payload)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// 健全性：帧长字段使用大端序，解析/编码对称。
func TestFrameRoundTrip(t *testing.T) {
	e := mkEvent("z", "alpha", 1, map[string]int{"beta": 2}, "GV1", "closed", "open")
	raw := encodeFrameOrDie(t, kindAccepted, e)
	if string(raw[:4]) != magic {
		t.Fatal("magic wrong")
	}
	if binary.BigEndian.Uint32(raw[4:8]) == 0 {
		t.Fatal("length zero")
	}
	frames, end, err := readFramesFromBytes(t, raw)
	if err != nil || len(frames) != 1 || end != int64(len(raw)) {
		t.Fatalf("round trip: %v frames=%d end=%d len=%d", err, len(frames), end, len(raw))
	}
	if frames[0].kind != kindAccepted {
		t.Fatalf("kind=%d", frames[0].kind)
	}
}

func readFramesFromBytes(t *testing.T, data []byte) ([]frame, int64, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.wal")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return readFrames(path)
}
