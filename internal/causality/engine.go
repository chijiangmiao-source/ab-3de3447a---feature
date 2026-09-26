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
type Engine struct {
	mu     sync.Mutex
	rounds map[string]*Round
	seq    int
}

// NewEngine creates an empty engine.
func NewEngine() *Engine {
	return &Engine{rounds: map[string]*Round{}}
}

// CreateRound registers a round of 2..5 consoles and one or more valves.
// An empty valve state defaults to "closed". An empty id is generated.
func (ng *Engine) CreateRound(id string, consoles []string, valves map[string]string) (RoundView, error) {
	ng.mu.Lock()
	defer ng.mu.Unlock()

	if id == "" {
		ng.seq++
		id = fmt.Sprintf("round-%d", ng.seq)
	}
	if _, ok := ng.rounds[id]; ok {
		return RoundView{}, &ConflictError{Reason: fmt.Sprintf("round id %q already exists", id)}
	}
	if len(consoles) < 2 || len(consoles) > 5 {
		return RoundView{}, &ValidationError{Reason: fmt.Sprintf("a round needs 2 to 5 consoles, got %d", len(consoles))}
	}
	seen := map[string]bool{}
	for _, c := range consoles {
		if c == "" {
			return RoundView{}, &ValidationError{Reason: "console names must not be empty"}
		}
		if seen[c] {
			return RoundView{}, &ValidationError{Reason: fmt.Sprintf("duplicate console %q", c)}
		}
		seen[c] = true
	}
	if len(valves) == 0 {
		return RoundView{}, &ValidationError{Reason: "a round needs at least one valve"}
	}
	vs := map[string]string{}
	for name, state := range valves {
		if name == "" {
			return RoundView{}, &ValidationError{Reason: "valve names must not be empty"}
		}
		if state == "" {
			state = "closed"
		}
		vs[name] = state
	}

	r := &Round{
		id:         id,
		consoles:   append([]string(nil), consoles...),
		consoleSet: seen,
		valves:     vs,
		frontier:   map[string]int{},
		records:    map[string]*Record{},
		pending:    map[string]*Record{},
	}
	for _, c := range consoles {
		r.frontier[c] = 0
	}
	ng.rounds[id] = r
	return r.view(), nil
}

// Submit validates and records an event, consuming it (and cascading any
// releasable waiting events) when its position and dependencies allow.
// Identical resubmissions return the existing conclusion; an event id
// reused with a changed payload is a conflict. Validation failures change
// nothing.
func (ng *Engine) Submit(roundID string, e Event) (*Record, error) {
	ng.mu.Lock()
	defer ng.mu.Unlock()

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
	if !r.consoleSet[e.Console] {
		return nil, &ValidationError{Reason: fmt.Sprintf("unknown console %q in round %q", e.Console, roundID)}
	}
	if _, ok := r.valves[e.Valve]; !ok {
		return nil, &ValidationError{Reason: fmt.Sprintf("unknown valve %q in round %q", e.Valve, roundID)}
	}
	if e.Seq < 1 {
		return nil, &ValidationError{Reason: fmt.Sprintf("seq must be >= 1, got %d", e.Seq)}
	}
	if e.Expected == "" {
		return nil, &ValidationError{Reason: "expected_old is required"}
	}
	if e.NewState == "" {
		return nil, &ValidationError{Reason: "new_state is required"}
	}
	for c, s := range e.Deps {
		if !r.consoleSet[c] {
			return nil, &ValidationError{Reason: fmt.Sprintf("dependency references unknown console %q", c)}
		}
		if s < 0 {
			return nil, &ValidationError{Reason: fmt.Sprintf("dependency on console %q has negative seq %d", c, s)}
		}
		if c == e.Console && s >= e.Seq {
			return nil, &ValidationError{Reason: fmt.Sprintf("future dependency: event %q (console %q seq %d) cannot depend on its own console at seq %d", e.EventID, e.Console, e.Seq, s)}
		}
	}
	next := r.frontier[e.Console] + 1
	if e.Seq < next {
		return nil, &ValidationError{Reason: fmt.Sprintf("stale seq %d: console %q frontier is already at %d", e.Seq, e.Console, r.frontier[e.Console])}
	}
	if e.Seq > next {
		return nil, &ValidationError{Reason: fmt.Sprintf("sequence gap: console %q next expected seq is %d, got %d", e.Console, next, e.Seq)}
	}

	rec := &Record{Event: e, fingerprint: fp}
	r.records[e.EventID] = rec
	if missing := r.missingDeps(e); len(missing) > 0 {
		rec.Status = StatusWaiting
		rec.Reason = fmt.Sprintf("waiting for dependencies: %s", formatMissing(missing))
		r.pending[e.EventID] = rec
		return rec, nil
	}
	r.consume(rec)
	r.drain()
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
// rejected_stale.
func (r *Round) drain() {
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
		}
		if best == nil {
			return
		}
		delete(r.pending, best.Event.EventID)
		r.consume(best)
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
