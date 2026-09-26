// Persistence support for the causal engine: every accepted mutation is
// committed to an append-only log as a complete snapshot of the round's
// causal state, and the success response is only allowed to surface after
// the commit is durable. Recovery validates every commit and restores the
// recorded conclusions directly — consumed events are never re-judged,
// waiting items, the frontier and the existing verdicts are never lost.
package causality

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"vacuum-interlock/internal/store"
)

// commitLog is the durability backend of the engine (the WAL store in
// production, a fake in tests).
type commitLog interface {
	Append(payload []byte) error
	Close() error
}

// OpenEngine opens an engine whose state is durable in dir: every accepted
// round creation and event consumption is appended to the commit log
// before its success response is returned. On open, the log is validated
// and only provably complete commits are restored; a truncated or corrupt
// tail is rolled back to the last consistent state and the recovered
// rounds stay fully operable.
func OpenEngine(dir string) (*Engine, error) {
	clog, commits, err := store.Open(dir, validateSnapshot)
	if err != nil {
		return nil, err
	}
	ng := &Engine{rounds: map[string]*Round{}, clog: clog}
	for _, payload := range commits {
		var snap roundSnapshot
		if err := json.Unmarshal(payload, &snap); err != nil {
			_ = clog.Close()
			return nil, fmt.Errorf("decode commit accepted during recovery: %w", err)
		}
		r := roundFromSnapshot(snap)
		ng.rounds[r.id] = r
		if n, ok := generatedRoundSeq(r.id); ok && n > ng.seq {
			ng.seq = n
		}
	}
	return ng, nil
}

// Close closes the commit log, if any. All commits were already fsynced
// when appended.
func (ng *Engine) Close() error {
	ng.mu.Lock()
	defer ng.mu.Unlock()
	if ng.clog == nil {
		return nil
	}
	return ng.clog.Close()
}

// persistLocked appends the complete causal state of the round to the
// commit log. Callers must hold ng.mu and must only surface success after
// this returns nil.
func (ng *Engine) persistLocked(r *Round) error {
	if ng.clog == nil {
		return nil
	}
	payload, err := json.Marshal(snapshotOf(r))
	if err != nil {
		return fmt.Errorf("encode causal state of round %q: %w", r.id, err)
	}
	if err := ng.clog.Append(payload); err != nil {
		return fmt.Errorf("persist causal state of round %q: %w", r.id, err)
	}
	return nil
}

// roundSnapshot is the persisted complete causal state of one round.
type roundSnapshot struct {
	ID       string            `json:"id"`
	Consoles []string          `json:"consoles"`
	Valves   map[string]string `json:"valves"`
	Frontier map[string]int    `json:"frontier"`
	Records  []Record          `json:"records"`
	Pending  []string          `json:"pending"` // event ids still waiting
	Log      []string          `json:"log"`     // event ids in consumption order
}

// snapshotOf captures the complete causal state of a round in a stable
// (deterministically ordered) form.
func snapshotOf(r *Round) roundSnapshot {
	snap := roundSnapshot{
		ID:       r.id,
		Consoles: append([]string(nil), r.consoles...),
		Valves:   maps.Clone(r.valves),
		Frontier: maps.Clone(r.frontier),
		Records:  []Record{},
		Pending:  []string{},
		Log:      []string{},
	}
	ids := make([]string, 0, len(r.records))
	for id := range r.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		snap.Records = append(snap.Records, *r.records[id])
	}
	for id := range r.pending {
		snap.Pending = append(snap.Pending, id)
	}
	sort.Strings(snap.Pending)
	for _, rec := range r.log {
		snap.Log = append(snap.Log, rec.Event.EventID)
	}
	return snap
}

// validateSnapshot checks the structural integrity of one recovered
// commit; anything that is not provably complete is rejected so recovery
// rolls back to the last consistent commit.
func validateSnapshot(payload []byte) error {
	var snap roundSnapshot
	if err := json.Unmarshal(payload, &snap); err != nil {
		return fmt.Errorf("snapshot does not decode: %w", err)
	}
	if snap.ID == "" {
		return errors.New("snapshot is missing the round id")
	}
	if len(snap.Consoles) < 2 || len(snap.Consoles) > 5 {
		return fmt.Errorf("snapshot of round %q has %d consoles", snap.ID, len(snap.Consoles))
	}
	for _, c := range snap.Consoles {
		if _, ok := snap.Frontier[c]; !ok {
			return fmt.Errorf("snapshot of round %q misses the frontier of console %q", snap.ID, c)
		}
	}
	ids := make(map[string]bool, len(snap.Records))
	for _, rec := range snap.Records {
		if rec.Event.EventID == "" {
			return fmt.Errorf("snapshot of round %q holds a record without event id", snap.ID)
		}
		ids[rec.Event.EventID] = true
	}
	for _, id := range snap.Pending {
		if !ids[id] {
			return fmt.Errorf("snapshot of round %q: pending references unknown event %q", snap.ID, id)
		}
	}
	for _, id := range snap.Log {
		if !ids[id] {
			return fmt.Errorf("snapshot of round %q: log references unknown event %q", snap.ID, id)
		}
	}
	return nil
}

// roundFromSnapshot rebuilds the in-memory round from its persisted state.
// Recorded conclusions are restored as-is — consumed events are never
// re-judged — and event fingerprints are recomputed so an event id reused
// with a changed payload still conflicts after recovery.
func roundFromSnapshot(snap roundSnapshot) *Round {
	r := &Round{
		id:         snap.ID,
		consoles:   append([]string(nil), snap.Consoles...),
		consoleSet: make(map[string]bool, len(snap.Consoles)),
		valves:     maps.Clone(snap.Valves),
		frontier:   maps.Clone(snap.Frontier),
		records:    make(map[string]*Record, len(snap.Records)),
		pending:    make(map[string]*Record, len(snap.Pending)),
	}
	for _, c := range r.consoles {
		r.consoleSet[c] = true
	}
	for i := range snap.Records {
		rec := snap.Records[i]
		rec.fingerprint = rec.Event.fingerprint()
		r.records[rec.Event.EventID] = &rec
	}
	for _, id := range snap.Pending {
		if rec, ok := r.records[id]; ok {
			r.pending[id] = rec
		}
	}
	for _, id := range snap.Log {
		if rec, ok := r.records[id]; ok {
			r.log = append(r.log, rec)
		}
	}
	return r
}

// clone deep-copies the round so a mutation can be prepared, persisted and
// only then committed to the engine; when persistence fails the engine
// simply keeps the previous copy.
func (r *Round) clone() *Round {
	c := &Round{
		id:         r.id,
		consoles:   append([]string(nil), r.consoles...),
		consoleSet: maps.Clone(r.consoleSet),
		valves:     maps.Clone(r.valves),
		frontier:   maps.Clone(r.frontier),
		records:    make(map[string]*Record, len(r.records)),
		pending:    make(map[string]*Record, len(r.pending)),
		log:        make([]*Record, len(r.log)),
	}
	for id, rec := range r.records {
		cp := *rec
		c.records[id] = &cp
	}
	for id := range r.pending {
		c.pending[id] = c.records[id]
	}
	for i, rec := range r.log {
		c.log[i] = c.records[rec.Event.EventID]
	}
	return c
}

// generatedRoundSeq extracts the sequence of an auto-generated round id
// ("round-N") so id generation resumes above every recovered round.
func generatedRoundSeq(id string) (int, bool) {
	rest, ok := strings.CutPrefix(id, "round-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}
