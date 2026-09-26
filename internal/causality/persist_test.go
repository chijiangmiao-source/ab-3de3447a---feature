package causality

import (
	"errors"
	"testing"
)

// fakeStore records every append and can be told to fail.
type fakeStore struct {
	created   []RoundCreated
	accepted  []Event
	committed [][]CommittedEntry
	fail      bool
}

func (s *fakeStore) AppendCreated(c RoundCreated) error {
	if s.fail {
		return errors.New("disk on fire")
	}
	s.created = append(s.created, c)
	return nil
}

func (s *fakeStore) AppendAccepted(_ string, e Event) error {
	if s.fail {
		return errors.New("disk on fire")
	}
	s.accepted = append(s.accepted, e)
	return nil
}

func (s *fakeStore) AppendCommitted(_ string, entries []CommittedEntry) error {
	if s.fail {
		return errors.New("disk on fire")
	}
	s.committed = append(s.committed, entries)
	return nil
}

// 成功响应只在完整因果状态持久化后返回：每个 API 调用对应确定的 WAL 追加。
func TestPersistentEngineWritesThrough(t *testing.T) {
	st := &fakeStore{}
	ng := NewPersistentEngine(st)
	if _, err := ng.CreateRound("r1", []string{"alpha", "beta"},
		map[string]string{"GV1": "closed", "GV2": "closed"}); err != nil {
		t.Fatal(err)
	}
	if len(st.created) != 1 || st.created[0].ID != "r1" {
		t.Fatalf("round creation must be persisted first: %+v", st.created)
	}

	// 等待事件 → accepted 帧
	b1 := Event{EventID: "b1", Console: "beta", Seq: 1, Deps: map[string]int{"alpha": 1},
		Valve: "GV1", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit("r1", b1); err != nil {
		t.Fatal(err)
	}
	if len(st.accepted) != 1 || st.accepted[0].EventID != "b1" {
		t.Fatalf("waiting event must be persisted as accepted: %+v", st.accepted)
	}
	if len(st.committed) != 0 {
		t.Fatalf("waiting event must not produce a commit batch: %+v", st.committed)
	}

	// 补齐前驱 → 一个原子批次包含触发事件与级联放行
	a1 := Event{EventID: "a1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "GV2", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit("r1", a1); err != nil {
		t.Fatal(err)
	}
	if len(st.committed) != 1 {
		t.Fatalf("consuming submit must persist exactly one batch: %+v", st.committed)
	}
	batch := st.committed[0]
	if len(batch) != 2 || batch[0].Event.EventID != "a1" || batch[1].Event.EventID != "b1" {
		t.Fatalf("batch must contain trigger + cascade in causal order: %+v", batch)
	}
	if batch[0].Status != StatusApplied || batch[1].Status != StatusApplied {
		t.Fatalf("batch statuses wrong: %+v", batch)
	}
	if batch[1].ValveAfter != "open" {
		t.Fatalf("batch must record valve_after: %+v", batch[1])
	}

	// 幂等重投不再产生任何持久化写入
	if _, err := ng.Submit("r1", b1); err != nil {
		t.Fatal(err)
	}
	if len(st.committed) != 1 || len(st.accepted) != 1 {
		t.Fatal("idempotent replay must not append to the log")
	}
}

// 持久化失败：成功响应绝不返回，引擎进入 fatal 并拒绝后续写入，
// 健康检查转为不可用，等待容器监督重启恢复。
func TestPersistentEngineFatalOnStoreFailure(t *testing.T) {
	st := &fakeStore{}
	ng := NewPersistentEngine(st)
	if _, err := ng.CreateRound("r1", []string{"alpha", "beta"}, map[string]string{"V": "closed"}); err != nil {
		t.Fatal(err)
	}
	st.fail = true
	e := Event{EventID: "e1", Console: "alpha", Seq: 1, Deps: map[string]int{},
		Valve: "V", Expected: "closed", NewState: "open"}
	if _, err := ng.Submit("r1", e); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("persist failure must surface as ErrUnavailable, got %v", err)
	}
	if ng.Healthy() {
		t.Fatal("engine must report unhealthy after a persistence failure")
	}
	// 后续写请求统一拒绝
	if _, err := ng.Submit("r1", e); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("post-fatal submit must be refused, got %v", err)
	}
	if _, err := ng.CreateRound("r2", []string{"a", "b"}, map[string]string{"V": "closed"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("post-fatal create must be refused, got %v", err)
	}
}

// 创建轮次的持久化失败同样进入 fatal，且轮次不可见。
func TestCreateRoundPersistFailure(t *testing.T) {
	st := &fakeStore{fail: true}
	ng := NewPersistentEngine(st)
	if _, err := ng.CreateRound("r1", []string{"a", "b"}, map[string]string{"V": "closed"}); err == nil {
		t.Fatal("persist failure must not report success")
	}
	if ids := ng.ListRounds(); len(ids) != 0 {
		t.Fatalf("failed creation must not publish the round: %v", ids)
	}
	if ng.Healthy() {
		t.Fatal("engine must be fatal after creation persist failure")
	}
}

// 录制→重放一致性：把活跃引擎产生的全部 WAL 帧重放进新引擎，
// 恢复出的阀门、前沿、等待项与每条裁决必须与原引擎一致。
func TestReplayMatchesLiveEngine(t *testing.T) {
	st := &fakeStore{}
	live := NewPersistentEngine(st)
	live.CreateRound("r1", []string{"alpha", "beta", "gamma"},
		map[string]string{"GV1": "closed", "GV2": "closed"})
	submits := []Event{
		{EventID: "b1", Console: "beta", Seq: 1, Deps: map[string]int{"alpha": 1},
			Valve: "GV1", Expected: "closed", NewState: "open"},
		{EventID: "c1", Console: "gamma", Seq: 1, Deps: map[string]int{"alpha": 1},
			Valve: "GV1", Expected: "closed", NewState: "open"},
		{EventID: "a1", Console: "alpha", Seq: 1, Deps: map[string]int{},
			Valve: "GV2", Expected: "closed", NewState: "open"},
		{EventID: "w1", Console: "beta", Seq: 2, Deps: map[string]int{"gamma": 2},
			Valve: "GV2", Expected: "open", NewState: "closed"},
	}
	for _, e := range submits {
		if _, err := live.Submit("r1", e); err != nil {
			t.Fatalf("submit %s: %v", e.EventID, err)
		}
	}

	replayed := NewEngine()
	if len(st.created) != 1 {
		t.Fatalf("created frames: %+v", st.created)
	}
	if err := replayed.LoadCreated(st.created[0]); err != nil {
		t.Fatal(err)
	}
	// 帧顺序：accepted 与 committed 交错，重放必须按原顺序进行：
	// b1,c1 accepted → a1 的 committed 批次（含级联）→ w1 accepted。
	type frame struct {
		accepted *Event
		batch    []CommittedEntry
	}
	frames := []frame{
		{accepted: &submits[0]},
		{accepted: &submits[1]},
		{batch: st.committed[0]},
		{accepted: &submits[3]},
	}
	for _, fr := range frames {
		var err error
		if fr.accepted != nil {
			err = replayed.ReplayWaiting("r1", *fr.accepted)
		} else {
			err = replayed.ReplayBatch("r1", fr.batch)
		}
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
	}

	want, _ := live.View("r1")
	got, _ := replayed.View("r1")
	if !equalFrontier(got.Frontier, want.Frontier) {
		t.Fatalf("frontier %v != %v", got.Frontier, want.Frontier)
	}
	for v, s := range want.Valves {
		if got.Valves[v] != s {
			t.Fatalf("valve %s = %s, want %s", v, got.Valves[v], s)
		}
	}
	if len(got.Pending) != len(want.Pending) {
		t.Fatalf("pending %d != %d", len(got.Pending), len(want.Pending))
	}
	if len(got.Log) != len(want.Log) {
		t.Fatalf("log %d != %d", len(got.Log), len(want.Log))
	}
	for i := range want.Log {
		if got.Log[i].Event.EventID != want.Log[i].Event.EventID ||
			got.Log[i].Status != want.Log[i].Status ||
			got.Log[i].Reason != want.Log[i].Reason {
			t.Fatalf("log[%d] = %+v, want %+v", i, got.Log[i], want.Log[i])
		}
	}
	// 争用裁决：b1 < c1，b1 放行、c1 预条件拒绝
	verdicts := map[string]Status{}
	for _, r := range got.Log {
		verdicts[r.Event.EventID] = r.Status
	}
	if verdicts["b1"] != StatusApplied || verdicts["c1"] != StatusRejectedPrecondition {
		t.Fatalf("contention arbitration not preserved by replay: %+v", verdicts)
	}
}
