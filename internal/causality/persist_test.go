package causality

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"vacuum-interlock/internal/store"
)

func openEngineAt(t *testing.T, dir string) *Engine {
	t.Helper()
	ng, err := OpenEngine(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return ng
}

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, store.FileName))
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	return info.Size()
}

// 恢复后阀门、前沿、等待项与裁决原因完整重现，已消费事件不被重新裁决。
func TestRecoveryRestoresCompleteCausalState(t *testing.T) {
	dir := t.TempDir()
	ng := openEngineAt(t, dir)
	if _, err := ng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"GV1": "closed", "GV2": "closed"}); err != nil {
		t.Fatal(err)
	}
	waiting := Event{EventID: "e-b-1", Console: "beta", Seq: 1,
		Deps: map[string]int{"alpha": 1}, Valve: "GV1", Expected: "closed", NewState: "open"}
	if rec, err := ng.Submit("r1", waiting); err != nil || rec.Status != StatusWaiting {
		t.Fatalf("waiting submit: %v %v", rec, err)
	}
	rejected := Event{EventID: "e-a-1", Console: "alpha", Seq: 1,
		Deps: map[string]int{}, Valve: "GV2", Expected: "open", NewState: "closed"}
	if rec, err := ng.Submit("r1", rejected); err != nil || rec.Status != StatusRejectedPrecondition {
		t.Fatalf("rejected submit: %v %v", rec, err)
	}
	// e-a-1 推进前沿后 e-b-1 级联放行
	if err := ng.Close(); err != nil {
		t.Fatal(err)
	}

	ng2 := openEngineAt(t, dir)
	defer ng2.Close()
	view, err := ng2.View("r1")
	if err != nil {
		t.Fatalf("round must survive the restart: %v", err)
	}
	if view.Valves["GV1"] != "open" || view.Valves["GV2"] != "closed" {
		t.Fatalf("valves not restored: %+v", view.Valves)
	}
	if view.Frontier["alpha"] != 1 || view.Frontier["beta"] != 1 {
		t.Fatalf("frontier not restored: %+v", view.Frontier)
	}
	if len(view.Pending) != 0 {
		t.Fatalf("pending not restored: %+v", view.Pending)
	}
	if len(view.Log) != 2 || view.Log[0].Status != StatusRejectedPrecondition || view.Log[1].Status != StatusApplied {
		t.Fatalf("verdicts not restored: %+v", view.Log)
	}
	if view.Log[0].Reason == "" || view.Log[1].Reason == "" {
		t.Fatalf("verdict reasons not restored: %+v", view.Log)
	}
}

// 恢复时仍有等待事件：补交前驱后原等待链按既有规则连续放行。
func TestRecoveryContinuesWaitingChain(t *testing.T) {
	dir := t.TempDir()
	ng := openEngineAt(t, dir)
	if _, err := ng.CreateRound("r", []string{"a", "b", "c"}, map[string]string{"V1": "closed", "V2": "closed"}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, ng, "r", Event{EventID: "e-b-1", Console: "b", Seq: 1,
		Deps: map[string]int{"a": 1}, Valve: "V1", Expected: "closed", NewState: "open"})
	mustSubmit(t, ng, "r", Event{EventID: "e-c-1", Console: "c", Seq: 1,
		Deps: map[string]int{"b": 1}, Valve: "V2", Expected: "open", NewState: "closed"})
	if err := ng.Close(); err != nil {
		t.Fatal(err)
	}

	ng2 := openEngineAt(t, dir)
	defer ng2.Close()
	view, err := ng2.View("r")
	if err != nil || len(view.Pending) != 2 {
		t.Fatalf("waiting items must survive the restart: %+v %v", view.Pending, err)
	}
	rec, err := ng2.Submit("r", Event{EventID: "e-a-1", Console: "a", Seq: 1,
		Deps: map[string]int{}, Valve: "V2", Expected: "closed", NewState: "open"})
	if err != nil || rec.Status != StatusApplied {
		t.Fatalf("predecessor after recovery: %v %v", rec, err)
	}
	view, _ = ng2.View("r")
	if len(view.Pending) != 0 || view.Valves["V1"] != "open" || view.Valves["V2"] != "closed" {
		t.Fatalf("waiting chain must release after recovery: %+v", view)
	}
	verdicts := map[string]Status{}
	for _, r := range view.Log {
		verdicts[r.Event.EventID] = r.Status
	}
	if verdicts["e-a-1"] != StatusApplied || verdicts["e-b-1"] != StatusApplied || verdicts["e-c-1"] != StatusApplied {
		t.Fatalf("chain verdicts wrong: %+v", verdicts)
	}
}

// 恢复前已放行/已预条件拒绝的标识重投返回原结论；复用标识改载荷仍冲突。
func TestIdempotencyAndConflictAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	ng := openEngineAt(t, dir)
	if _, err := ng.CreateRound("r", []string{"a", "b"}, map[string]string{"V": "closed"}); err != nil {
		t.Fatal(err)
	}
	applied := Event{EventID: "e1", Console: "a", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "closed", NewState: "open"}
	rejected := Event{EventID: "e2", Console: "b", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "closed", NewState: "open"}
	mustSubmit(t, ng, "r", applied)
	mustSubmit(t, ng, "r", rejected) // V 已是 open → 预条件拒绝
	if err := ng.Close(); err != nil {
		t.Fatal(err)
	}

	ng2 := openEngineAt(t, dir)
	defer ng2.Close()
	if rec := mustSubmit(t, ng2, "r", applied); rec.Status != StatusApplied || !rec.Consumed {
		t.Fatalf("applied verdict must be stable across recovery, got %s", rec.Status)
	}
	if rec := mustSubmit(t, ng2, "r", rejected); rec.Status != StatusRejectedPrecondition {
		t.Fatalf("rejected verdict must be stable across recovery, got %s", rec.Status)
	}
	view, _ := ng2.View("r")
	if len(view.Log) != 2 {
		t.Fatalf("replays must not consume again: %+v", view.Log)
	}
	changed := applied
	changed.NewState = "closed"
	var ce *ConflictError
	if _, err := ng2.Submit("r", changed); !errors.As(err, &ce) {
		t.Fatalf("id reuse with changed payload must conflict after recovery, got %v", err)
	}
}

// 成功响应返回时完整因果状态已持久化：不关闭引擎，另一实例即可读到。
func TestStateDurableBeforeSuccessReturns(t *testing.T) {
	dir := t.TempDir()
	ng := openEngineAt(t, dir)
	defer ng.Close()
	if _, err := ng.CreateRound("r", []string{"a", "b"}, map[string]string{"V": "closed"}); err != nil {
		t.Fatal(err)
	}
	ng2 := openEngineAt(t, dir)
	defer ng2.Close()
	if _, err := ng2.View("r"); err != nil {
		t.Fatalf("round must be durable before create returns: %v", err)
	}
	mustSubmit(t, ng, "r", Event{EventID: "e1", Console: "a", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "closed", NewState: "open"})
	ng3 := openEngineAt(t, dir)
	defer ng3.Close()
	view, err := ng3.View("r")
	if err != nil || view.Frontier["a"] != 1 || view.Valves["V"] != "open" {
		t.Fatalf("consumed event must be durable before submit returns: %+v %v", view, err)
	}
}

// 自动轮次标识在恢复后继续递增，不与既有轮次冲突。
func TestGeneratedRoundIDsResumeAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	ng := openEngineAt(t, dir)
	v1, err := ng.CreateRound("", []string{"a", "b"}, map[string]string{"V": "closed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ng.Close(); err != nil {
		t.Fatal(err)
	}
	ng2 := openEngineAt(t, dir)
	defer ng2.Close()
	v2, err := ng2.CreateRound("", []string{"a", "b"}, map[string]string{"V": "closed"})
	if err != nil {
		t.Fatalf("auto id after recovery must not collide: %v", err)
	}
	if v2.ID == v1.ID {
		t.Fatalf("auto id %q reused after recovery", v2.ID)
	}
}

// 落盘失败：成功响应不得返回，内存状态整体回滚，重试不受污染。
func TestPersistFailureLeavesStateUntouched(t *testing.T) {
	fs := &failingStore{fail: true}
	ng := NewEngine()
	ng.clog = fs
	if _, err := ng.CreateRound("r", []string{"a", "b"}, map[string]string{"V": "closed"}); err == nil {
		t.Fatal("create must fail when the state cannot be persisted")
	}
	if ids := ng.ListRounds(); len(ids) != 0 {
		t.Fatalf("failed create must not register the round: %v", ids)
	}
	fs.fail = false
	if _, err := ng.CreateRound("r", []string{"a", "b"}, map[string]string{"V": "closed"}); err != nil {
		t.Fatalf("retry after the store recovers must succeed: %v", err)
	}
	fs.fail = true
	e := Event{EventID: "e1", Console: "a", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit("r", e); err == nil {
		t.Fatal("submit must fail when the state cannot be persisted")
	}
	view, _ := ng.View("r")
	if view.Frontier["a"] != 0 || view.Valves["V"] != "closed" || len(view.Log) != 0 {
		t.Fatalf("failed submit must leave the round untouched: %+v", view)
	}
	fs.fail = false
	if rec, err := ng.Submit("r", e); err != nil || rec.Status != StatusApplied {
		t.Fatalf("retry must succeed and consume: %v %v", rec, err)
	}
}

type failingStore struct{ fail bool }

func (f *failingStore) Append([]byte) error {
	if f.fail {
		return errors.New("commit log unavailable")
	}
	return nil
}

func (f *failingStore) Close() error { return nil }

// 损坏尾部：恢复回退到最后一致提交，轮次保持可继续操作。
func TestCorruptTailRollsBackToConsistentState(t *testing.T) {
	dir := t.TempDir()
	ng := openEngineAt(t, dir)
	if _, err := ng.CreateRound("r", []string{"a", "b"}, map[string]string{"V": "closed"}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, ng, "r", Event{EventID: "e1", Console: "a", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "closed", NewState: "open"})
	if err := ng.Close(); err != nil {
		t.Fatal(err)
	}
	size := walSize(t, dir)

	// 模拟崩溃时的撕裂写
	f, err := os.OpenFile(filepath.Join(dir, store.FileName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("VIW1\x00\x00torn-tail")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	ng2 := openEngineAt(t, dir)
	defer ng2.Close()
	view, err := ng2.View("r")
	if err != nil || view.Frontier["a"] != 1 || view.Valves["V"] != "open" {
		t.Fatalf("state must roll back to the last consistent commit: %+v %v", view, err)
	}
	if got := walSize(t, dir); got != size {
		t.Fatalf("corrupt tail must be truncated: %d != %d", got, size)
	}
	if rec, err := ng2.Submit("r", Event{EventID: "e2", Console: "b", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "open", NewState: "closed"}); err != nil || rec.Status != StatusApplied {
		t.Fatalf("round must stay operable after rollback: %v %v", rec, err)
	}
}
