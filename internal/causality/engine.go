// Package causality implements the causal-consistency engine for the
// synchrotron-radiation vacuum-valve interlock rounds.
//
// Consoles may submit interlock operations while offline and replay them
// after reconnecting: the arrival order at the server must not change the
// resulting causal state. An event is atomically consumed only when its
// console-local sequence is exactly the next one on the causal frontier
// and every entry of its dependency vector is satisfied. A consumed event
// either applies (expected old valve state matches) or is rejected on its
// precondition — in both cases the causal position advances so successors
// are never permanently blocked. When several events become releasable in
// the same batch, the smallest event id wins, which makes concurrent
// contention for the same old valve state deterministic.
package causality

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Status is the verdict recorded for an event.
type Status string

const (
	// StatusApplied means the event was consumed and the valve was updated.
	StatusApplied Status = "applied"
	// StatusRejectedPrecondition means the event was consumed (the causal
	// position advanced) but the expected old valve state did not match.
	StatusRejectedPrecondition Status = "rejected_precondition"
	// StatusWaiting means the event is recorded but its dependency vector
	// is not satisfied yet.
	StatusWaiting Status = "waiting"
	// StatusRejectedStale means another event took this causal position
	// first (same console and sequence, different event id).
	StatusRejectedStale Status = "rejected_stale"
)

// Event is one interlock operation submitted by a console.
type Event struct {
	EventID  string         `json:"event_id"`
	Console  string         `json:"console"`
	Seq      int            `json:"seq"`
	Deps     map[string]int `json:"deps"`
	Valve    string         `json:"valve"`
	Expected string         `json:"expected_old"`
	NewState string         `json:"new_state"`
}

// fingerprint is a stable hash of the full payload, used to detect event
// ids that are reused with a changed payload.
func (e Event) fingerprint() string {
	b, _ := json.Marshal(e) // encoding/json sorts map keys
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Record is the stored outcome of an accepted event.
type Record struct {
	Event      Event  `json:"event"`
	Status     Status `json:"status"`
	Reason     string `json:"reason,omitempty"`
	Consumed   bool   `json:"consumed"`
	ValveAfter string `json:"valve_after,omitempty"`

	fingerprint string
}

// ValidationError rejects a submission without recording or consuming it
// and without touching any valve state (sequence gap, stale sequence,
// unknown console, future dependency, ...).
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// ConflictError reports an event id reused with a different payload.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return e.Reason }

// ErrRoundNotFound is returned when the round id is unknown.
var ErrRoundNotFound = errors.New("round not found")

// ErrUnavailable is returned after a persistence failure: in-memory and
// on-disk state can no longer be proven identical, so the process must be
// restarted (e.g. by the container supervisor) and recovered from the WAL.
var ErrUnavailable = errors.New("persistence failed; service must recover from its data directory")

// RoundCreated is the persisted birth record of a round.
type RoundCreated struct {
	ID       string            `json:"id"`
	Consoles []string          `json:"consoles"`
	Valves   map[string]string `json:"valves"`
}

// CommittedEntry is one finalized verdict inside an atomic commit batch:
// a consumed event (applied / rejected_precondition) or a waiting event
// that lost its causal position (rejected_stale).
type CommittedEntry struct {
	Event      Event  `json:"event"`
	Status     Status `json:"status"`
	Reason     string `json:"reason,omitempty"`
	ValveAfter string `json:"valve_after,omitempty"`
}

// Store is the durability sink. Every append must be on stable storage
// (fsync) before it returns, so a successful HTTP response always means
// the full causal state is durable.
type Store interface {
	AppendCreated(created RoundCreated) error
	AppendAccepted(roundID string, e Event) error
	AppendCommitted(roundID string, entries []CommittedEntry) error
}

// Round is the causal state of one duty round.
type Round struct {
	id         string
	consoles   []string
	consoleSet map[string]bool
	valves     map[string]string
	frontier   map[string]int // console -> last consumed local sequence
	records    map[string]*Record
	pending    map[string]*Record
	log        []*Record // consumed/finalized records in causal order
}

// PendingView describes one waiting event and the dependencies it lacks.
type PendingView struct {
	Event   Event          `json:"event"`
	Missing map[string]int `json:"missing"`
	Reason  string         `json:"reason"`
}

// RoundView is a consistent snapshot of a round for the API and the page.
type RoundView struct {
	ID       string            `json:"id"`
	Consoles []string          `json:"consoles"`
	Valves   map[string]string `json:"valves"`
	Frontier map[string]int    `json:"frontier"`
	Pending  []PendingView     `json:"pending"`
	Log      []Record          `json:"log"`
}

// Engine owns all rounds; every mutation is serialized by one mutex so an
// event consumption (frontier advance + valve update + cascade) is atomic.
// Mutations are also appended to the Store before success is reported, so a
// crash can never lose a committed verdict nor expose a response that was
// not durable.
type Engine struct {
	mu     sync.Mutex
	rounds map[string]*Round
	seq    int
	store  Store
	fatal  bool
}

// NewEngine creates an empty, non-persistent engine (mainly for tests).
func NewEngine() *Engine {
	return &Engine{rounds: map[string]*Round{}}
}

// NewPersistentEngine creates an engine whose mutations are written through
// to store before success is returned.
func NewPersistentEngine(store Store) *Engine {
	return &Engine{rounds: map[string]*Round{}, store: store}
}

// AttachStore installs the durability sink after startup recovery: the
// store replays its logs into a fresh engine first, then the engine starts
// appending new commits to it.
func (ng *Engine) AttachStore(store Store) {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	ng.store = store
}

// Healthy reports whether the engine may serve mutating requests. After a
// persistence failure the engine refuses further writes (and recovery is a
// process restart) rather than risk diverging in-memory from durable state.
func (ng *Engine) Healthy() bool {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	return !ng.fatal
}

// fail locks the engine into the fatal state after a persistence error.
func (ng *Engine) failLocked() {
	ng.fatal = true
}

// CreateRound registers a round of 2..5 consoles and one or more valves.
// An empty valve state defaults to "closed". An empty id is generated.
// Success is reported only after the round's birth record is durable.
func (ng *Engine) CreateRound(id string, consoles []string, valves map[string]string) (RoundView, error) {
	ng.mu.Lock()
	defer ng.mu.Unlock()

	if ng.fatal {
		return RoundView{}, ErrUnavailable
	}

	auto := id == ""
	if auto {
		ng.seq++
		id = fmt.Sprintf("round-%d", ng.seq)
	}
	if _, ok := ng.rounds[id]; ok {
		return RoundView{}, &ConflictError{Reason: fmt.Sprintf("round id %q already exists", id)}
	}
	created := RoundCreated{ID: id, Consoles: consoles, Valves: valves}
	r, err := newRoundFromCreated(created)
	if err != nil {
		if auto {
			ng.seq-- // an auto-generated id that failed validation was never used
		}
		return RoundView{}, err
	}
	// Durability first: only publish the round once its creation is committed.
	if ng.store != nil {
		if err := ng.store.AppendCreated(created); err != nil {
			ng.failLocked()
			if auto {
				ng.seq--
			}
			return RoundView{}, fmt.Errorf("%w: persist round creation: %v", ErrUnavailable, err)
		}
	}
	ng.rounds[id] = r
	ng.bumpSeqLocked(id)
	return r.view(), nil
}

// newRoundFromCreated validates a birth record (a submitted one or a
// replayed one) and builds the empty round it describes.
func newRoundFromCreated(c RoundCreated) (*Round, error) {
	if c.ID == "" {
		return nil, &ValidationError{Reason: "round id is required"}
	}
	if len(c.Consoles) < 2 || len(c.Consoles) > 5 {
		return nil, &ValidationError{Reason: fmt.Sprintf("a round needs 2 to 5 consoles, got %d", len(c.Consoles))}
	}
	seen := map[string]bool{}
	for _, name := range c.Consoles {
		if name == "" {
			return nil, &ValidationError{Reason: "console names must not be empty"}
		}
		if seen[name] {
			return nil, &ValidationError{Reason: fmt.Sprintf("duplicate console %q", name)}
		}
		seen[name] = true
	}
	if len(c.Valves) == 0 {
		return nil, &ValidationError{Reason: "a round needs at least one valve"}
	}
	vs := map[string]string{}
	for name, state := range c.Valves {
		if name == "" {
			return nil, &ValidationError{Reason: "valve names must not be empty"}
		}
		if state == "" {
			state = "closed"
		}
		vs[name] = state
	}
	r := &Round{
		id:         c.ID,
		consoles:   append([]string(nil), c.Consoles...),
		consoleSet: seen,
		valves:     vs,
		frontier:   map[string]int{},
		records:    map[string]*Record{},
		pending:    map[string]*Record{},
	}
	for _, name := range c.Consoles {
		r.frontier[name] = 0
	}
	return r, nil
}

// bumpSeqLocked keeps the auto-id counter ahead of recovered round ids.
func (ng *Engine) bumpSeqLocked(id string) {
	var n int
	if _, err := fmt.Sscanf(id, "round-%d", &n); err == nil && n > ng.seq {
		ng.seq = n
	}
}

// LoadCreated installs an empty round from a persisted birth record during
// recovery. The caller has already verified the frame checksum; the engine
// still re-validates every semantic invariant and refuses anything odd.
func (ng *Engine) LoadCreated(c RoundCreated) error {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	if _, ok := ng.rounds[c.ID]; ok {
		return fmt.Errorf("duplicate round creation for %q", c.ID)
	}
	r, err := newRoundFromCreated(c)
	if err != nil {
		return err
	}
	ng.rounds[c.ID] = r
	ng.bumpSeqLocked(c.ID)
	return nil
}

// ReplayWaiting re-applies one verified "accepted" (waiting) frame. On the
// durable frame order the event is not yet consumed, so it must pass the
// exact same acceptance rules as a fresh submit at this frontier.
func (ng *Engine) ReplayWaiting(roundID string, e Event) error {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	r := ng.rounds[roundID]
	if r == nil {
		return fmt.Errorf("frame for unknown round %q", roundID)
	}
	if _, ok := r.records[e.EventID]; ok {
		return fmt.Errorf("duplicate accepted frame for event %q", e.EventID)
	}
	if err := r.validateEvent(e); err != nil {
		return err
	}
	if missing := r.missingDeps(e); len(missing) == 0 {
		return fmt.Errorf("accepted frame for %q has no missing dependencies at replay", e.EventID)
	}
	rec := &Record{
		Event:       e,
		fingerprint: e.fingerprint(),
		Status:      StatusWaiting,
		Reason:      fmt.Sprintf("waiting for dependencies: %s", formatMissing(r.missingDeps(e))),
	}
	r.records[e.EventID] = rec
	r.pending[e.EventID] = rec
	return nil
}

// ReplayBatch re-applies one verified atomic commit batch. The whole batch
// is first validated on a throwaway clone of the round: every stored
// verdict must match what the deterministic engine would adjudicate at
// that point. Only a fully consistent batch is applied to the real round,
// so a corrupt or forged frame can never leave a half-applied mutation
// behind — the caller rolls the log back to before this frame.
func (ng *Engine) ReplayBatch(roundID string, entries []CommittedEntry) error {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	r := ng.rounds[roundID]
	if r == nil {
		return fmt.Errorf("frame for unknown round %q", roundID)
	}
	if len(entries) == 0 {
		return errors.New("empty committed batch")
	}
	sim := r.clone()
	// The triggering event (first entry) is always a fresh, immediately
	// consumed submission in a real commit, so it must pass the full
	// submit-time validation and carry a consumed verdict.
	first := entries[0]
	if first.Status != StatusApplied && first.Status != StatusRejectedPrecondition {
		return fmt.Errorf("committed batch triggered by %q has non-consumed status %q",
			first.Event.EventID, first.Status)
	}
	if _, known := sim.records[first.Event.EventID]; !known {
		if err := sim.validateEvent(first.Event); err != nil {
			return err
		}
	}
	for i, en := range entries {
		if err := sim.applyCommitted(en, i == 0); err != nil {
			return err
		}
	}
	for i, en := range entries {
		if err := r.applyCommitted(en, i == 0); err != nil {
			return fmt.Errorf("re-apply of validated batch failed (engine bug): %w", err)
		}
	}
	return nil
}

// clone returns a deep-enough copy of the round for transactional replay
// validation: causal state, records and pending set are independent, while
// the immutable console set is shared.
func (r *Round) clone() *Round {
	c := &Round{
		id:         r.id,
		consoles:   r.consoles,
		consoleSet: r.consoleSet,
		valves:     map[string]string{},
		frontier:   map[string]int{},
		records:    map[string]*Record{},
		pending:    map[string]*Record{},
	}
	for k, v := range r.valves {
		c.valves[k] = v
	}
	for k, v := range r.frontier {
		c.frontier[k] = v
	}
	for id, rec := range r.records {
		rc := *rec
		c.records[id] = &rc
	}
	for id := range r.pending {
		c.pending[id] = c.records[id]
	}
	c.log = append([]*Record(nil), r.log...)
	return c
}

// applyCommitted verifies one batch entry against the round and, only if
// every check passes, applies it. All checks run before any mutation.
func (r *Round) applyCommitted(en CommittedEntry, first bool) error {
	e := en.Event
	if e.EventID == "" {
		return errors.New("committed entry without event id")
	}
	rec, known := r.records[e.EventID]
	switch {
	case !known:
		// The trigger is always a brand-new record; every later verdict in
		// the cascade belongs to a previously accepted waiting event.
		if !first {
			return fmt.Errorf("committed entry %q has no prior accepted frame", e.EventID)
		}
	case first:
		// A live trigger is consumed immediately and can never have been
		// recorded (waiting or finalized) before its commit.
		return fmt.Errorf("committed trigger %q was already recorded as %q", e.EventID, rec.Status)
	case rec.Status != StatusWaiting:
		return fmt.Errorf("committed entry %q is not a waiting record", e.EventID)
	default:
		// The verdict must adjudicate exactly the payload that was accepted
		// as waiting — an id reused with changed payload is a live 409.
		if rec.fingerprint != e.fingerprint() {
			return fmt.Errorf("committed entry %q payload differs from its accepted frame", e.EventID)
		}
	}

	next := r.frontier[e.Console] + 1
	switch en.Status {
	case StatusApplied, StatusRejectedPrecondition:
		if e.Seq != next {
			return fmt.Errorf("committed %q: seq %d is not next (%d) for console %q",
				e.EventID, e.Seq, next, e.Console)
		}
		if missing := r.missingDeps(e); len(missing) > 0 {
			return fmt.Errorf("committed %q released while dependencies missing: %s",
				e.EventID, formatMissing(missing))
		}
		cur, ok := r.valves[e.Valve]
		if !ok {
			return fmt.Errorf("committed %q targets unknown valve %q", e.EventID, e.Valve)
		}
		switch en.Status {
		case StatusApplied:
			if cur != e.Expected {
				return fmt.Errorf("committed %q marked applied but valve %q is %q, expected old %q",
					e.EventID, e.Valve, cur, e.Expected)
			}
			if en.ValveAfter != e.NewState {
				return fmt.Errorf("committed %q valve_after %q != new_state %q",
					e.EventID, en.ValveAfter, e.NewState)
			}
		case StatusRejectedPrecondition:
			if cur == e.Expected {
				return fmt.Errorf("committed %q marked precondition-rejected but valve %q is %q",
					e.EventID, e.Valve, cur)
			}
			if en.ValveAfter != "" && en.ValveAfter != cur {
				return fmt.Errorf("committed %q valve_after %q != actual %q",
					e.EventID, en.ValveAfter, cur)
			}
		}
	case StatusRejectedStale:
		if e.Seq >= next {
			return fmt.Errorf("committed %q marked stale but its causal position %s#%d was not taken (next %d)",
				e.EventID, e.Console, e.Seq, next)
		}
	default:
		return fmt.Errorf("committed %q has unknown status %q", e.EventID, en.Status)
	}

	// All checks passed: mutate.
	if !known {
		rec = &Record{Event: e, fingerprint: e.fingerprint()}
		r.records[e.EventID] = rec
	} else {
		delete(r.pending, e.EventID)
	}
	switch en.Status {
	case StatusApplied:
		r.frontier[e.Console] = e.Seq
		r.valves[e.Valve] = e.NewState
		rec.Consumed = true
		rec.ValveAfter = e.NewState
	case StatusRejectedPrecondition:
		r.frontier[e.Console] = e.Seq
		rec.Consumed = true
		rec.ValveAfter = r.valves[e.Valve]
	case StatusRejectedStale:
		rec.Consumed = false
	}
	rec.Status = en.Status
	rec.Reason = en.Reason
	r.log = append(r.log, rec)
	return nil
}

// validateEvent applies the acceptance rules shared by live submits and
// WAL replay: a rejected event is never recorded and changes nothing.
func (r *Round) validateEvent(e Event) error {
	if e.EventID == "" {
		return &ValidationError{Reason: "event_id is required"}
	}
	if !r.consoleSet[e.Console] {
		return &ValidationError{Reason: fmt.Sprintf("unknown console %q in round %q", e.Console, r.id)}
	}
	if _, ok := r.valves[e.Valve]; !ok {
		return &ValidationError{Reason: fmt.Sprintf("unknown valve %q in round %q", e.Valve, r.id)}
	}
	if e.Seq < 1 {
		return &ValidationError{Reason: fmt.Sprintf("seq must be >= 1, got %d", e.Seq)}
	}
	if e.Expected == "" {
		return &ValidationError{Reason: "expected_old is required"}
	}
	if e.NewState == "" {
		return &ValidationError{Reason: "new_state is required"}
	}
	for c, s := range e.Deps {
		if !r.consoleSet[c] {
			return &ValidationError{Reason: fmt.Sprintf("dependency references unknown console %q", c)}
		}
		if s < 0 {
			return &ValidationError{Reason: fmt.Sprintf("dependency on console %q has negative seq %d", c, s)}
		}
		if c == e.Console && s >= e.Seq {
			return &ValidationError{Reason: fmt.Sprintf("future dependency: event %q (console %q seq %d) cannot depend on its own console at seq %d", e.EventID, e.Console, e.Seq, s)}
		}
	}
	next := r.frontier[e.Console] + 1
	if e.Seq < next {
		return &ValidationError{Reason: fmt.Sprintf("stale seq %d: console %q frontier is already at %d", e.Seq, e.Console, r.frontier[e.Console])}
	}
	if e.Seq > next {
		return &ValidationError{Reason: fmt.Sprintf("sequence gap: console %q next expected seq is %d, got %d", e.Console, next, e.Seq)}
	}
	return nil
}

// Submit validates and records an event, consuming it (and cascading any
// releasable waiting events) when its position and dependencies allow.
// Identical resubmissions return the existing conclusion; an event id
// reused with a changed payload is a conflict. Validation failures change
// nothing. A waiting verdict is persisted as an "accepted" frame; the
// consuming event plus every verdict its release triggers (the whole
// cascade) is persisted as one atomic "committed" frame — the success
// response is sent only after that frame is durable, so a crash can never
// adjudicate an event twice nor split a cascade across the restart.
func (ng *Engine) Submit(roundID string, e Event) (*Record, error) {
	ng.mu.Lock()
	defer ng.mu.Unlock()

	if ng.fatal {
		return nil, ErrUnavailable
	}

	r := ng.rounds[roundID]
	if r == nil {
		return nil, ErrRoundNotFound
	}
	if e.EventID == "" {
		return nil, &ValidationError{Reason: "event_id is required"}
	}
	fp := e.fingerprint()
	if prev, ok := r.records[e.EventID]; ok {
		if prev.fingerprint == fp {
			return prev, nil // idempotent replay: return the existing conclusion
		}
		return nil, &ConflictError{Reason: fmt.Sprintf("event id %q was already used with a different payload", e.EventID)}
	}
	if err := r.validateEvent(e); err != nil {
		return nil, err
	}

	rec := &Record{Event: e, fingerprint: fp}
	r.records[e.EventID] = rec
	if missing := r.missingDeps(e); len(missing) > 0 {
		rec.Status = StatusWaiting
		rec.Reason = fmt.Sprintf("waiting for dependencies: %s", formatMissing(missing))
		r.pending[e.EventID] = rec
		if ng.store != nil {
			if err := ng.store.AppendAccepted(roundID, e); err != nil {
				ng.failLocked()
				return nil, fmt.Errorf("%w: persist waiting event %q: %v", ErrUnavailable, e.EventID, err)
			}
		}
		return rec, nil
	}

	r.consume(rec)
	finalized := []*Record{rec}
	finalized = append(finalized, r.drain()...)
	if ng.store != nil {
		entries := make([]CommittedEntry, len(finalized))
		for i, fr := range finalized {
			entries[i] = CommittedEntry{
				Event:      fr.Event,
				Status:     fr.Status,
				Reason:     fr.Reason,
				ValveAfter: fr.ValveAfter,
			}
		}
		if err := ng.store.AppendCommitted(roundID, entries); err != nil {
			ng.failLocked()
			return nil, fmt.Errorf("%w: persist verdict batch for %q: %v", ErrUnavailable, e.EventID, err)
		}
	}
	return rec, nil
}

// View returns a consistent snapshot of a round.
func (ng *Engine) View(roundID string) (RoundView, error) {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	r := ng.rounds[roundID]
	if r == nil {
		return RoundView{}, ErrRoundNotFound
	}
	return r.view(), nil
}

// ListRounds returns the ids of all rounds, sorted.
func (ng *Engine) ListRounds() []string {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	ids := make([]string, 0, len(ng.rounds))
	for id := range ng.rounds {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// consume atomically advances the frontier past the event and either
// applies the valve transition or rejects it on its precondition. The
// causal position advances in both cases so successors are never blocked.
func (r *Round) consume(rec *Record) {
	e := rec.Event
	r.frontier[e.Console] = e.Seq
	rec.Consumed = true
	cur := r.valves[e.Valve]
	if cur == e.Expected {
		r.valves[e.Valve] = e.NewState
		rec.Status = StatusApplied
		rec.Reason = fmt.Sprintf("released: valve %q %s -> %s", e.Valve, cur, e.NewState)
		rec.ValveAfter = e.NewState
	} else {
		rec.Status = StatusRejectedPrecondition
		rec.Reason = fmt.Sprintf("precondition rejected: valve %q expected old state %q but actual is %q; causal position advanced", e.Valve, e.Expected, cur)
		rec.ValveAfter = cur
	}
	r.log = append(r.log, rec)
}

// drain repeatedly releases the waiting events that have become
// consumable. Each batch is arbitrated by the smallest event id, so the
// outcome is stable and independent of arrival order. Waiting events
// whose causal position was taken by another event are finalized as
// rejected_stale. It returns every record finalized during the cascade,
// in causal order, so the caller can persist them as one atomic batch.
func (r *Round) drain() []*Record {
	var finalized []*Record
	for {
		var stale []*Record
		var best *Record
		for _, rec := range r.pending {
			e := rec.Event
			next := r.frontier[e.Console] + 1
			switch {
			case e.Seq < next:
				stale = append(stale, rec)
			case e.Seq == next && len(r.missingDeps(e)) == 0:
				if best == nil || e.EventID < best.Event.EventID {
					best = rec
				}
			}
		}
		for _, rec := range stale {
			delete(r.pending, rec.Event.EventID)
			rec.Status = StatusRejectedStale
			rec.Reason = fmt.Sprintf("rejected: causal position %s#%d was already taken by another event", rec.Event.Console, rec.Event.Seq)
			r.log = append(r.log, rec)
			finalized = append(finalized, rec)
		}
		if best == nil {
			return finalized
		}
		delete(r.pending, best.Event.EventID)
		r.consume(best)
		finalized = append(finalized, best)
	}
}

// missingDeps returns the dependency entries not yet covered by the
// frontier: console -> required seq.
func (r *Round) missingDeps(e Event) map[string]int {
	var missing map[string]int
	for c, s := range e.Deps {
		if r.frontier[c] < s {
			if missing == nil {
				missing = map[string]int{}
			}
			missing[c] = s
		}
	}
	return missing
}

func (r *Round) view() RoundView {
	v := RoundView{
		ID:       r.id,
		Consoles: append([]string(nil), r.consoles...),
		Valves:   map[string]string{},
		Frontier: map[string]int{},
		Pending:  []PendingView{},
		Log:      []Record{},
	}
	for name, st := range r.valves {
		v.Valves[name] = st
	}
	for c, s := range r.frontier {
		v.Frontier[c] = s
	}
	for _, rec := range r.pending {
		v.Pending = append(v.Pending, PendingView{
			Event:   rec.Event,
			Missing: r.missingDeps(rec.Event),
			Reason:  rec.Reason,
		})
	}
	sort.Slice(v.Pending, func(i, j int) bool {
		return v.Pending[i].Event.EventID < v.Pending[j].Event.EventID
	})
	for _, rec := range r.log {
		v.Log = append(v.Log, *rec)
	}
	return v
}

func formatMissing(missing map[string]int) string {
	parts := make([]string, 0, len(missing))
	for c, s := range missing {
		parts = append(parts, fmt.Sprintf("%s>=%d", c, s))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}
