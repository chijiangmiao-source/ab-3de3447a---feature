package causality

import (
	"errors"
	"testing"
)

func newEngineWithRound(t *testing.T, consoles []string, valves map[string]string) (*Engine, string) {
	t.Helper()
	ng := NewEngine()
	view, err := ng.CreateRound("r1", consoles, valves)
	if err != nil {
		t.Fatalf("create round: %v", err)
	}
	return ng, view.ID
}

func mustSubmit(t *testing.T, ng *Engine, round string, e Event) *Record {
	t.Helper()
	rec, err := ng.Submit(round, e)
	if err != nil {
		t.Fatalf("submit %s: %v", e.EventID, err)
	}
	return rec
}

func TestRoundValidation(t *testing.T) {
	ng := NewEngine()
	if _, err := ng.CreateRound("x", []string{"only"}, map[string]string{"V": "closed"}); err == nil {
		t.Fatal("1 console must be rejected")
	}
	if _, err := ng.CreateRound("x", []string{"a", "b", "c", "d", "e", "f"}, map[string]string{"V": "closed"}); err == nil {
		t.Fatal("6 consoles must be rejected")
	}
	if _, err := ng.CreateRound("x", []string{"a", "b"}, nil); err == nil {
		t.Fatal("round without valves must be rejected")
	}
	if _, err := ng.CreateRound("x", []string{"a", "a"}, map[string]string{"V": "closed"}); err == nil {
		t.Fatal("duplicate consoles must be rejected")
	}
	if _, err := ng.CreateRound("x", []string{"a", "b"}, map[string]string{"V": "closed"}); err != nil {
		t.Fatalf("valid round: %v", err)
	}
	if _, err := ng.CreateRound("x", []string{"a", "b"}, map[string]string{"V": "closed"}); err == nil {
		t.Fatal("duplicate round id must be rejected")
	}
	var ce *ConflictError
	if _, err := ng.CreateRound("x", []string{"a", "b"}, map[string]string{"V": "closed"}); !errors.As(err, &ce) {
		t.Fatalf("duplicate round id must be a conflict, got %v", err)
	}
}

// 验收场景：先提交依赖另一控制台的事件而显示等待，补齐前驱后连续放行。
func TestWaitingThenCascadeRelease(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed", "GV2": "closed"})

	b1 := Event{EventID: "e-b-1", Console: "beta", Seq: 1,
		Deps: map[string]int{"alpha": 1, "beta": 0}, Valve: "GV1", Expected: "closed", NewState: "open"}
	rec := mustSubmit(t, ng, id, b1)
	if rec.Status != StatusWaiting {
		t.Fatalf("expected waiting, got %s", rec.Status)
	}
	view, _ := ng.View(id)
	if view.Valves["GV1"] != "closed" || view.Frontier["beta"] != 0 {
		t.Fatalf("waiting event must not change state: %+v", view)
	}
	if len(view.Pending) != 1 || view.Pending[0].Missing["alpha"] != 1 {
		t.Fatalf("pending view wrong: %+v", view.Pending)
	}

	a1 := Event{EventID: "e-a-1", Console: "alpha", Seq: 1,
		Deps: map[string]int{"alpha": 0, "beta": 0}, Valve: "GV2", Expected: "closed", NewState: "open"}
	if rec := mustSubmit(t, ng, id, a1); rec.Status != StatusApplied {
		t.Fatalf("expected applied, got %s", rec.Status)
	}
	view, _ = ng.View(id)
	if view.Valves["GV1"] != "open" || view.Valves["GV2"] != "open" {
		t.Fatalf("cascade did not release the waiting event: %+v", view.Valves)
	}
	if view.Frontier["alpha"] != 1 || view.Frontier["beta"] != 1 {
		t.Fatalf("frontier wrong: %+v", view.Frontier)
	}
	if len(view.Pending) != 0 {
		t.Fatalf("pending must be drained: %+v", view.Pending)
	}
	// 重投已放行事件 → 返回既有结论
	rec = mustSubmit(t, ng, id, b1)
	if rec.Status != StatusApplied || !rec.Consumed {
		t.Fatalf("replay must return the existing conclusion, got %s", rec.Status)
	}
}

func TestIdempotentReplayAndConflict(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	e := Event{EventID: "e1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	first := mustSubmit(t, ng, id, e)
	if first.Status != StatusApplied {
		t.Fatalf("expected applied, got %s", first.Status)
	}
	again := mustSubmit(t, ng, id, e)
	if again != first {
		t.Fatal("identical resubmission must return the existing record")
	}
	view, _ := ng.View(id)
	if len(view.Log) != 1 {
		t.Fatalf("replay must not consume twice, log=%d", len(view.Log))
	}
	changed := e
	changed.NewState = "closed"
	var ce *ConflictError
	if _, err := ng.Submit(id, changed); !errors.As(err, &ce) {
		t.Fatalf("id reuse with changed payload must conflict, got %v", err)
	}
	view, _ = ng.View(id)
	if view.Valves["GV1"] != "open" {
		t.Fatalf("conflict must not change valve state: %+v", view.Valves)
	}
}

func TestSequenceGapRejectedThenRecoverable(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	gap := Event{EventID: "e2", Console: "alpha", Seq: 2, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	var ve *ValidationError
	if _, err := ng.Submit(id, gap); !errors.As(err, &ve) {
		t.Fatalf("gap must be a validation rejection, got %v", err)
	}
	view, _ := ng.View(id)
	if view.Frontier["alpha"] != 0 || view.Valves["GV1"] != "closed" {
		t.Fatalf("gap rejection must not change state: %+v", view)
	}
	// 跳号拒绝不是终态：补齐前驱后同一事件可重新提交并被消费
	e1 := Event{EventID: "e1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	mustSubmit(t, ng, id, e1)
	rec := mustSubmit(t, ng, id, gap)
	if !rec.Consumed {
		t.Fatalf("after filling the gap the event must be consumed, got %s", rec.Status)
	}
}

func TestStaleSeqRejected(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	mustSubmit(t, ng, id, Event{EventID: "e1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"})
	stale := Event{EventID: "e2", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	var ve *ValidationError
	if _, err := ng.Submit(id, stale); !errors.As(err, &ve) {
		t.Fatalf("stale seq must be rejected, got %v", err)
	}
}

func TestUnknownConsoleValveAndRound(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	base := Event{EventID: "e1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit("no-such-round", base); !errors.Is(err, ErrRoundNotFound) {
		t.Fatalf("unknown round: %v", err)
	}
	bad := base
	bad.Console = "ghost"
	var ve *ValidationError
	if _, err := ng.Submit(id, bad); !errors.As(err, &ve) {
		t.Fatalf("unknown console must be rejected, got %v", err)
	}
	bad = base
	bad.Valve = "GVX"
	if _, err := ng.Submit(id, bad); !errors.As(err, &ve) {
		t.Fatalf("unknown valve must be rejected, got %v", err)
	}
	view, _ := ng.View(id)
	if view.Valves["GV1"] != "closed" || view.Frontier["alpha"] != 0 {
		t.Fatalf("rejections must not change state: %+v", view)
	}
}

func TestFutureDependencyRejected(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	var ve *ValidationError
	// 依赖自身控制台的未来序号
	selfFuture := Event{EventID: "e1", Console: "alpha", Seq: 2,
		Deps: map[string]int{"alpha": 2}, Valve: "GV1", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit(id, selfFuture); !errors.As(err, &ve) {
		t.Fatalf("self future dependency must be rejected, got %v", err)
	}
	// 依赖未知控制台
	unknownDep := Event{EventID: "e2", Console: "alpha", Seq: 1,
		Deps: map[string]int{"ghost": 1}, Valve: "GV1", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit(id, unknownDep); !errors.As(err, &ve) {
		t.Fatalf("dependency on unknown console must be rejected, got %v", err)
	}
	// 负依赖
	neg := Event{EventID: "e3", Console: "alpha", Seq: 1,
		Deps: map[string]int{"beta": -1}, Valve: "GV1", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit(id, neg); !errors.As(err, &ve) {
		t.Fatalf("negative dependency must be rejected, got %v", err)
	}
	view, _ := ng.View(id)
	if view.Frontier["alpha"] != 0 || view.Valves["GV1"] != "closed" {
		t.Fatalf("future-dependency rejections must not change state: %+v", view)
	}
}

// 预条件不符也须拒绝并推进因果位置，后继不被永久阻塞。
func TestPreconditionRejectedButFrontierAdvances(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"GV1": "closed"})
	bad := Event{EventID: "e1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	rec := mustSubmit(t, ng, id, bad)
	if rec.Status != StatusRejectedPrecondition || !rec.Consumed {
		t.Fatalf("expected consumed precondition rejection, got %s", rec.Status)
	}
	view, _ := ng.View(id)
	if view.Frontier["alpha"] != 1 {
		t.Fatalf("precondition rejection must advance the frontier: %+v", view.Frontier)
	}
	if view.Valves["GV1"] != "closed" {
		t.Fatalf("precondition rejection must not change the valve: %+v", view.Valves)
	}
	// 后继事件不被拒绝结果阻塞
	next := Event{EventID: "e2", Console: "alpha", Seq: 2, Deps: map[string]int{"alpha": 1},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	if rec := mustSubmit(t, ng, id, next); rec.Status != StatusApplied {
		t.Fatalf("successor must not be blocked by the rejection, got %s", rec.Status)
	}
}

// 并发争用同一旧状态：同批可放行项按事件标识稳定裁决，较小者成功。
func TestContentionStableArbitration(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta", "gamma"},
		map[string]string{"GV1": "closed", "GV2": "closed"})
	mustSubmit(t, ng, id, Event{EventID: "a1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"})
	mustSubmit(t, ng, id, Event{EventID: "b1", Console: "beta", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "open", NewState: "closed"})
	// 两个控制台争用 GV2 的同一旧状态，都等待 gamma 的前驱
	contA := Event{EventID: "contend-aaa", Console: "alpha", Seq: 2,
		Deps: map[string]int{"gamma": 1}, Valve: "GV2", Expected: "closed", NewState: "open"}
	contB := Event{EventID: "contend-bbb", Console: "beta", Seq: 2,
		Deps: map[string]int{"gamma": 1}, Valve: "GV2", Expected: "closed", NewState: "open"}
	if rec := mustSubmit(t, ng, id, contA); rec.Status != StatusWaiting {
		t.Fatalf("contA should wait, got %s", rec.Status)
	}
	if rec := mustSubmit(t, ng, id, contB); rec.Status != StatusWaiting {
		t.Fatalf("contB should wait, got %s", rec.Status)
	}
	// 前驱到达，同批放行：contend-aaa < contend-bbb，较小标识成功
	mustSubmit(t, ng, id, Event{EventID: "g1", Console: "gamma", Seq: 1, Deps: map[string]int{},
		Valve: "GV1", Expected: "closed", NewState: "open"})
	view, _ := ng.View(id)
	verdicts := map[string]Status{}
	for _, rec := range view.Log {
		verdicts[rec.Event.EventID] = rec.Status
	}
	if verdicts["contend-aaa"] != StatusApplied {
		t.Fatalf("smaller event id must win, got %s", verdicts["contend-aaa"])
	}
	if verdicts["contend-bbb"] != StatusRejectedPrecondition {
		t.Fatalf("larger event id must be stably precondition-rejected, got %s", verdicts["contend-bbb"])
	}
	if view.Valves["GV2"] != "open" {
		t.Fatalf("winner must have applied its transition: %+v", view.Valves)
	}
	if view.Frontier["beta"] != 2 {
		t.Fatalf("loser's causal position must still advance: %+v", view.Frontier)
	}
	// 竞争失败方的后继不被拒绝结果阻塞
	succ := Event{EventID: "b2", Console: "beta", Seq: 3, Deps: map[string]int{"beta": 2},
		Valve: "GV1", Expected: "open", NewState: "closed"}
	if rec := mustSubmit(t, ng, id, succ); rec.Status != StatusApplied {
		t.Fatalf("successor of the rejected event must not be blocked, got %s", rec.Status)
	}
	// 重投失败事件 → 稳定返回同一拒绝结论
	if rec := mustSubmit(t, ng, id, contB); rec.Status != StatusRejectedPrecondition {
		t.Fatalf("rejection must be stable on replay, got %s", rec.Status)
	}
}

// 同一因果位置被两个等待事件竞争时，另一个被确定性地拒绝。
func TestSupersededPending(t *testing.T) {
	ng, id := newEngineWithRound(t, []string{"alpha", "beta"}, map[string]string{"V1": "closed", "V2": "closed"})
	e1 := Event{EventID: "dup-1", Console: "alpha", Seq: 1,
		Deps: map[string]int{"beta": 1}, Valve: "V1", Expected: "closed", NewState: "open"}
	e2 := Event{EventID: "dup-2", Console: "alpha", Seq: 1,
		Deps: map[string]int{"beta": 1}, Valve: "V1", Expected: "closed", NewState: "open"}
	mustSubmit(t, ng, id, e1)
	mustSubmit(t, ng, id, e2)
	mustSubmit(t, ng, id, Event{EventID: "b1", Console: "beta", Seq: 1, Deps: map[string]int{},
		Valve: "V2", Expected: "closed", NewState: "open"})
	view, _ := ng.View(id)
	verdicts := map[string]Status{}
	for _, rec := range view.Log {
		verdicts[rec.Event.EventID] = rec.Status
	}
	if verdicts["dup-1"] != StatusApplied || verdicts["dup-2"] != StatusRejectedStale {
		t.Fatalf("superseded arbitration wrong: %+v", verdicts)
	}
	if len(view.Pending) != 0 {
		t.Fatalf("pending must be empty: %+v", view.Pending)
	}
}

// 到达顺序不得改变应有的因果状态：同一批事件按不同跨控制台顺序提交，
// 最终阀门、前沿与每个事件的裁决必须一致。
func TestArrivalOrderIndependence(t *testing.T) {
	consoles := []string{"A", "B"}
	valves := map[string]string{"V1": "closed", "V2": "closed", "V3": "closed"}
	events := []Event{
		{EventID: "a1", Console: "A", Seq: 1, Deps: map[string]int{}, Valve: "V1", Expected: "closed", NewState: "open"},
		{EventID: "a2", Console: "A", Seq: 2, Deps: map[string]int{"B": 1}, Valve: "V2", Expected: "closed", NewState: "open"},
		{EventID: "b1", Console: "B", Seq: 1, Deps: map[string]int{}, Valve: "V3", Expected: "closed", NewState: "open"},
		{EventID: "b2", Console: "B", Seq: 2, Deps: map[string]int{"A": 2}, Valve: "V1", Expected: "open", NewState: "closed"},
	}
	// 保持每个控制台内部顺序的不同交错
	orders := [][]string{
		{"a1", "a2", "b1", "b2"},
		{"b1", "b2", "a1", "a2"},
		{"a1", "b1", "a2", "b2"},
		{"b1", "a1", "b2", "a2"},
		{"a1", "b1", "b2", "a2"},
	}
	byID := map[string]Event{}
	for _, e := range events {
		byID[e.EventID] = e
	}
	var want RoundView
	for i, order := range orders {
		ng := NewEngine()
		if _, err := ng.CreateRound("r", consoles, valves); err != nil {
			t.Fatal(err)
		}
		for _, id := range order {
			if _, err := ng.Submit("r", byID[id]); err != nil {
				t.Fatalf("order %d submit %s: %v", i, id, err)
			}
		}
		got, _ := ng.View("r")
		if i == 0 {
			want = got
			continue
		}
		if !equalFrontier(got.Frontier, want.Frontier) {
			t.Fatalf("order %v: frontier %v != %v", order, got.Frontier, want.Frontier)
		}
		for v, st := range want.Valves {
			if got.Valves[v] != st {
				t.Fatalf("order %v: valve %s = %s, want %s", order, v, got.Valves[v], st)
			}
		}
		gotVerdicts := map[string]Status{}
		for _, rec := range got.Log {
			gotVerdicts[rec.Event.EventID] = rec.Status
		}
		for _, rec := range want.Log {
			if gotVerdicts[rec.Event.EventID] != rec.Status {
				t.Fatalf("order %v: verdict of %s = %s, want %s",
					order, rec.Event.EventID, gotVerdicts[rec.Event.EventID], rec.Status)
			}
		}
	}
	if want.Valves["V1"] != "closed" || want.Valves["V2"] != "open" || want.Valves["V3"] != "open" {
		t.Fatalf("unexpected final valves: %+v", want.Valves)
	}
}

func equalFrontier(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
