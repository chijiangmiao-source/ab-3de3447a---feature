// Package store implements the write-ahead log behind the causal engine.
//
// Every accepted event and every atomic verdict batch is appended to a
// per-round log file and fsynced before the engine reports success, so a
// successful HTTP response always means the full causal state is durable.
// On startup the log is scanned frame by frame; each frame carries a
// SHA-256 checksum over its header and payload. A truncated or corrupted
// tail (a crash mid-write, a torn sector) is rolled back to the last
// consistent frame and the file is physically truncated there, so the
// round stays operable and only provably complete commits are admitted.
//
// Frame layout (all integers big-endian):
//
//	magic     4 bytes   "VILW"
//	length    uint32    length of kind+payload in bytes
//	kind      1 byte    1=created, 2=accepted(waiting), 3=committed(batch)
//	payload   JSON
//	checksum  32 bytes  sha256(magic | length | kind | payload)
package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"vacuum-interlock/internal/causality"
)

const (
	magic       = "VILW"
	headerLen   = 8 // magic(4) + length(4)
	checksumLen = 32
	maxPayload  = 1 << 20 // 1 MiB per frame is far beyond any batch

	kindCreated   byte = 1
	kindAccepted  byte = 2
	kindCommitted byte = 3
)

var (
	errTruncated = errors.New("truncated frame")
	errBadMagic  = errors.New("bad frame magic")
	errBadLength = errors.New("bad frame length")
	errChecksum  = errors.New("frame checksum mismatch")
)

// frame is one verified WAL frame with its file offsets.
type frame struct {
	kind    byte
	payload []byte
	offset  int64 // where the frame starts
	end     int64 // one past the last byte
}

// Store is a causality.Store backed by per-round WAL files in a directory.
type Store struct {
	dir   string
	mu    sync.Mutex
	files map[string]*os.File // round id -> append handle
}

// fileNameFor maps a round id to a safe, unique file name: a sanitized
// prefix for humans plus a hash suffix for collision freedom.
func fileNameFor(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name := b.String()
	if name == "" || name == "." || name == ".." {
		name = "round"
	}
	if len(name) > 40 {
		name = name[:40]
	}
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("%s-%x.wal", name, sum[:4])
}

// Open prepares the data directory, recovers every round log into eng
// (rolling back torn or corrupted tails), and returns a Store ready for
// appends. Recovery never fails hard on a corrupt log: the file is rolled
// back to its last consistent frame and the round keeps operating.
func Open(dir string, eng *causality.Engine) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory %q: %w", dir, err)
	}
	s := &Store{dir: dir, files: map[string]*os.File{}}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("scan data directory %q: %w", dir, err)
	}
	names := []string{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // deterministic recovery order
	for _, name := range names {
		if err := s.recoverFile(filepath.Join(dir, name), eng); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// recoverFile verifies one log file, truncates it at the first frame that
// is not provably complete, replays the surviving prefix into the engine,
// and registers the append handle for the round.
func (s *Store) recoverFile(path string, eng *causality.Engine) error {
	frames, validEnd, perr := readFrames(path)
	if perr != nil {
		log.Printf("recovery: %s: %v at offset %d; rolling back to last consistent frame", path, perr, validEnd)
		if err := truncateTo(path, validEnd); err != nil {
			return fmt.Errorf("roll back corrupted tail of %q: %w", path, err)
		}
	}
	if len(frames) == 0 {
		// Nothing provably committed (e.g. crash between file creation and
		// the first fsync): the round was never acknowledged, so the empty
		// file is simply ignored and will be reused if the id reappears.
		return nil
	}
	if frames[0].kind != kindCreated {
		log.Printf("recovery: %s: first frame is not a round creation; discarding unverifiable log", path)
		return truncateTo(path, 0)
	}
	var created causality.RoundCreated
	if err := json.Unmarshal(frames[0].payload, &created); err != nil || created.ID == "" {
		log.Printf("recovery: %s: undecodable creation frame; discarding unverifiable log", path)
		return truncateTo(path, 0)
	}
	if fileNameFor(created.ID) != filepath.Base(path) {
		log.Printf("recovery: %s: creation frame names round %q; discarding mismatched log", path, created.ID)
		return truncateTo(path, 0)
	}
	if err := eng.LoadCreated(created); err != nil {
		// A duplicate creation means another file already carries this
		// round; this file cannot be a faithful continuation.
		log.Printf("recovery: %s: %v; discarding log", path, err)
		return truncateTo(path, 0)
	}
	kept := frames[:1]
	for _, fr := range frames[1:] {
		if err := replayFrame(eng, created.ID, fr); err != nil {
			log.Printf("recovery: %s: frame at offset %d rejected (%v); rolling back to last consistent frame", path, fr.offset, err)
			if err := truncateTo(path, fr.offset); err != nil {
				return fmt.Errorf("roll back unfaithful log %q: %w", path, err)
			}
			break
		}
		kept = append(kept, fr)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open recovered log %q: %w", path, err)
	}
	s.files[created.ID] = f
	log.Printf("recovery: round %q restored from %s (%d frame(s))", created.ID, path, len(kept))
	return nil
}

// replayFrame decodes one frame and applies it to the engine.
func replayFrame(eng *causality.Engine, roundID string, fr frame) error {
	switch fr.kind {
	case kindAccepted:
		var e causality.Event
		if err := json.Unmarshal(fr.payload, &e); err != nil {
			return fmt.Errorf("decode accepted frame: %w", err)
		}
		return eng.ReplayWaiting(roundID, e)
	case kindCommitted:
		var batch struct {
			Entries []causality.CommittedEntry `json:"entries"`
		}
		if err := json.Unmarshal(fr.payload, &batch); err != nil {
			return fmt.Errorf("decode committed frame: %w", err)
		}
		return eng.ReplayBatch(roundID, batch.Entries)
	default:
		return fmt.Errorf("unknown frame kind %d", fr.kind)
	}
}

// readFrames scans a log file and returns every verified frame plus the
// offset of the first byte that is not part of a complete, intact frame.
// A physical read error is reported together with that offset so the
// caller can roll the file back to it.
func readFrames(path string) ([]frame, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var frames []frame
	off := int64(0)
	for {
		header := make([]byte, headerLen)
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				return frames, off, nil // clean end exactly on a frame boundary
			}
			if err == io.ErrUnexpectedEOF {
				return frames, off, errTruncated
			}
			return frames, off, err
		}
		if string(header[:4]) != magic {
			return frames, off, errBadMagic
		}
		length := binary.BigEndian.Uint32(header[4:])
		if length < 1 || length > maxPayload+1 {
			return frames, off, errBadLength
		}
		body := make([]byte, int(length)+checksumLen)
		if _, err := io.ReadFull(f, body); err != nil {
			return frames, off, errTruncated
		}
		sum := sha256.New()
		sum.Write(header)
		sum.Write(body[:length])
		if !bytes.Equal(sum.Sum(nil), body[length:]) {
			return frames, off, errChecksum
		}
		end := off + headerLen + int64(length) + checksumLen
		frames = append(frames, frame{
			kind:    body[0],
			payload: body[1:length],
			offset:  off,
			end:     end,
		})
		off = end
	}
}

// encodeFrame builds one on-disk frame.
func encodeFrame(kind byte, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	body := make([]byte, 0, 1+len(raw))
	body = append(body, kind)
	body = append(body, raw...)
	if len(body) > maxPayload+1 {
		return nil, fmt.Errorf("frame payload of %d bytes exceeds limit", len(body))
	}
	out := make([]byte, headerLen, headerLen+len(body)+checksumLen)
	copy(out[:4], magic)
	binary.BigEndian.PutUint32(out[4:], uint32(len(body)))
	sum := sha256.New()
	sum.Write(out)
	sum.Write(body)
	out = append(out, body...)
	out = append(out, sum.Sum(nil)...)
	return out, nil
}

// appendLocked writes one frame to an open log and fsyncs it before
// returning, so the caller's success response is durable.
func appendLocked(f *os.File, kind byte, payload any) error {
	data, err := encodeFrame(kind, payload)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return f.Sync()
}

// truncateTo rolls a log file back to a consistent offset and fsyncs both
// the file and its directory so the rollback itself is durable.
func truncateTo(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// AppendCreated persists a round's birth record (first frame of its log).
func (s *Store) AppendCreated(created causality.RoundCreated) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[created.ID]; ok {
		return fmt.Errorf("log for round %q already exists", created.ID)
	}
	path := filepath.Join(s.dir, fileNameFor(created.ID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := appendLocked(f, kindCreated, created); err != nil {
		f.Close()
		return err
	}
	if err := syncDir(s.dir); err != nil { // the new directory entry must be durable too
		f.Close()
		return err
	}
	s.files[created.ID] = f
	return nil
}

// AppendAccepted persists an event recorded as waiting.
func (s *Store) AppendAccepted(roundID string, e causality.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.files[roundID]
	if !ok {
		return fmt.Errorf("no log for round %q", roundID)
	}
	return appendLocked(f, kindAccepted, e)
}

// AppendCommitted persists one atomic batch of verdicts.
func (s *Store) AppendCommitted(roundID string, entries []causality.CommittedEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.files[roundID]
	if !ok {
		return fmt.Errorf("no log for round %q", roundID)
	}
	return appendLocked(f, kindCommitted, struct {
		Entries []causality.CommittedEntry `json:"entries"`
	}{Entries: entries})
}

// Close flushes and closes all log handles.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for id, f := range s.files {
		if err := f.Close(); err != nil && first == nil {
			first = fmt.Errorf("close log of round %q: %w", id, err)
		}
	}
	s.files = map[string]*os.File{}
	return first
}
