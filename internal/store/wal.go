// Package store implements the append-only commit log that makes the
// interlock causal state durable across process exits and container
// rebuilds.
//
// Every commit is one frame:
//
//	magic "VIW1" (4B) | seq (uint64 LE) | payload length (uint32 LE) | CRC32-IEEE of payload (4B) | payload
//
// Frames are chained by a strictly increasing sequence number and every
// frame is fsynced before the writer is allowed to surface success. On
// open, the log is scanned and only provably complete commits are
// accepted: the first frame that is torn, fails its checksum, breaks the
// sequence chain or is rejected by the caller's validator marks a corrupt
// tail. The tail is rolled back (truncated) so the log ends at the last
// consistent commit and stays appendable.
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileName is the commit log file inside the data directory.
const FileName = "interlock.wal"

// maxPayload bounds one frame so a corrupt length field cannot trigger a
// huge allocation during recovery.
const maxPayload = 32 << 20

// headerSize is magic + seq + length + crc.
const headerSize = 4 + 8 + 4 + 4

var frameMagic = []byte("VIW1")

// Store appends commits to the log and recovers them on open.
type Store struct {
	mu     sync.Mutex
	f      *os.File
	seq    uint64 // sequence of the last commit in the log
	offset int64  // end of the last consistent commit
}

// Open opens (creating when necessary) the commit log in dir, scans it and
// returns the payloads of all provably complete commits in order. A
// truncated or corrupt tail is rolled back: the file is truncated to the
// last consistent commit so later appends build on a clean state. The
// optional validator is called for every payload; a payload it rejects is
// treated as the start of the corrupt tail.
func Open(dir string, validate func(payload []byte) error) (*Store, [][]byte, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create data dir %s: %w", dir, err)
	}
	path := filepath.Join(dir, FileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open commit log %s: %w", path, err)
	}
	st := &Store{f: f}
	commits, off, err := st.scan(validate)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("stat commit log: %w", err)
	}
	if info.Size() != off {
		// 截断或损坏尾部：回退到最后一致提交，保持日志可继续追加。
		if err := f.Truncate(off); err != nil {
			_ = f.Close()
			return nil, nil, fmt.Errorf("roll back commit log to last consistent commit: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, nil, fmt.Errorf("sync commit log rollback: %w", err)
		}
	}
	st.seq = uint64(len(commits))
	st.offset = off
	syncDir(dir)
	return st, commits, nil
}

// Append writes one commit and fsyncs it before returning: a nil error
// means the commit is durable. On failure the partial frame is rolled back
// best-effort and the log is left at the last consistent commit.
func (st *Store) Append(payload []byte) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(payload) > maxPayload {
		return fmt.Errorf("commit payload of %d bytes exceeds the %d byte limit", len(payload), maxPayload)
	}
	frame := encodeFrame(st.seq+1, payload)
	if _, err := st.f.Write(frame); err != nil {
		_ = st.f.Truncate(st.offset)
		return fmt.Errorf("append commit %d: %w", st.seq+1, err)
	}
	if err := st.f.Sync(); err != nil {
		_ = st.f.Truncate(st.offset)
		return fmt.Errorf("sync commit %d: %w", st.seq+1, err)
	}
	st.seq++
	st.offset += int64(len(frame))
	return nil
}

// Close closes the log. All commits were already fsynced when appended.
func (st *Store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.f.Close()
}

// scan reads frames from the start of the file and returns the payloads of
// all complete commits plus the offset just past the last one — the point
// where a corrupt tail, if any, begins.
func (st *Store) scan(validate func([]byte) error) ([][]byte, int64, error) {
	var commits [][]byte
	var off int64
	expect := uint64(1)
	hdr := make([]byte, headerSize)
	for {
		if _, err := st.f.ReadAt(hdr, off); err != nil {
			if errors.Is(err, io.EOF) {
				break // 干净结尾或撕裂的帧头
			}
			return nil, 0, fmt.Errorf("read commit log: %w", err)
		}
		seq := binary.LittleEndian.Uint64(hdr[4:12])
		length := binary.LittleEndian.Uint32(hdr[12:16])
		crc := binary.LittleEndian.Uint32(hdr[16:20])
		if !bytes.Equal(hdr[0:4], frameMagic) || seq != expect || length > maxPayload {
			break // 损坏帧：回退点
		}
		payload := make([]byte, length)
		if _, err := st.f.ReadAt(payload, off+headerSize); err != nil {
			if errors.Is(err, io.EOF) {
				break // 撕裂的载荷
			}
			return nil, 0, fmt.Errorf("read commit log: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break // 校验和不符
		}
		if validate != nil {
			if err := validate(payload); err != nil {
				break // 记录结构不完整
			}
		}
		commits = append(commits, payload)
		off += headerSize + int64(length)
		expect++
	}
	return commits, off, nil
}

func encodeFrame(seq uint64, payload []byte) []byte {
	frame := make([]byte, headerSize+len(payload))
	copy(frame[0:4], frameMagic)
	binary.LittleEndian.PutUint64(frame[4:12], seq)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[16:20], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)
	return frame
}

// syncDir best-effort fsyncs the directory so the log file's directory
// entry itself is durable.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
