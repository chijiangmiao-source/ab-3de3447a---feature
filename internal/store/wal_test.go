package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func appendRaw(t *testing.T, dir string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, FileName), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open for raw append: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("raw append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close raw append: %v", err)
	}
}

func walFileSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	return info.Size()
}

func TestAppendRecoverAndContinue(t *testing.T) {
	dir := t.TempDir()
	st, commits, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(commits) != 0 {
		t.Fatalf("fresh log must have no commits, got %d", len(commits))
	}
	if err := st.Append([]byte(`{"round":"r1"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := st.Append([]byte(`{"round":"r2"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st, commits, err = Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(commits) != 2 || string(commits[0]) != `{"round":"r1"}` || string(commits[1]) != `{"round":"r2"}` {
		t.Fatalf("recovered commits wrong: %q", commits)
	}
	// 恢复后继续追加，序号链保持连续
	if err := st.Append([]byte(`{"round":"r3"}`)); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st, commits, err = Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen again: %v", err)
	}
	defer st.Close()
	if len(commits) != 3 || string(commits[2]) != `{"round":"r3"}` {
		t.Fatalf("commit chain must continue across recovery: %q", commits)
	}
}

func TestTornHeaderTailRolledBack(t *testing.T) {
	dir := t.TempDir()
	st, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	st.Append([]byte("one"))
	st.Append([]byte("two"))
	st.Close()
	size := walFileSize(t, dir)

	// 崩溃撕裂写：只写出了半个帧头
	appendRaw(t, dir, []byte("VIW1\x00\x01"))

	st, commits, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(commits) != 2 {
		t.Fatalf("torn tail must roll back to the consistent commits, got %d", len(commits))
	}
	if got := walFileSize(t, dir); got != size {
		t.Fatalf("corrupt tail must be truncated: size %d, want %d", got, size)
	}
	// 回退后日志可继续追加
	if err := st.Append([]byte("three")); err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
}

func TestTornPayloadTailRolledBack(t *testing.T) {
	dir := t.TempDir()
	st, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	st.Append([]byte("one"))
	st.Close()
	size := walFileSize(t, dir)

	frame := encodeFrame(2, []byte("a payload that never fully arrived"))
	appendRaw(t, dir, frame[:headerSize+5]) // 帧头完整，载荷撕裂

	st, commits, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(commits) != 1 {
		t.Fatalf("torn payload must roll back, got %d commits", len(commits))
	}
	if got := walFileSize(t, dir); got != size {
		t.Fatalf("corrupt tail must be truncated: size %d, want %d", got, size)
	}
}

func TestCorruptFramesRolledBack(t *testing.T) {
	corrupt := map[string]func(frame []byte){
		"bad magic": func(f []byte) { f[0] ^= 0xFF },
		"bad crc":   func(f []byte) { f[headerSize-1] ^= 0xFF },
	}
	for name, spoil := range corrupt {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			st, _, err := Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			st.Append([]byte("one"))
			st.Close()
			size := walFileSize(t, dir)

			frame := encodeFrame(2, []byte("two"))
			spoil(frame)
			appendRaw(t, dir, frame)

			st, commits, err := Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if len(commits) != 1 {
				t.Fatalf("corrupt frame must be rejected, got %d commits", len(commits))
			}
			if got := walFileSize(t, dir); got != size {
				t.Fatalf("corrupt tail must be truncated: size %d, want %d", got, size)
			}
		})
	}
}

func TestSequenceGapRolledBack(t *testing.T) {
	dir := t.TempDir()
	st, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	st.Append([]byte("one"))
	st.Close()
	size := walFileSize(t, dir)

	appendRaw(t, dir, encodeFrame(3, []byte("three"))) // 跳过序号 2

	st, commits, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(commits) != 1 {
		t.Fatalf("a gap in the commit chain must roll back, got %d commits", len(commits))
	}
	if got := walFileSize(t, dir); got != size {
		t.Fatalf("gap tail must be truncated: size %d, want %d", got, size)
	}
}

func TestGarbageOnlyLogRecoversEmpty(t *testing.T) {
	dir := t.TempDir()
	appendRaw(t, dir, []byte("total garbage from a crashed writer"))
	st, commits, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(commits) != 0 {
		t.Fatalf("garbage log must recover zero commits, got %d", len(commits))
	}
	if got := walFileSize(t, dir); got != 0 {
		t.Fatalf("garbage must be truncated away, size=%d", got)
	}
	if err := st.Append([]byte("one")); err != nil {
		t.Fatalf("log must stay appendable: %v", err)
	}
}

func TestValidatorRejectsIncompleteCommits(t *testing.T) {
	dir := t.TempDir()
	st, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	st.Append([]byte(`{"ok":true}`))
	st.Append([]byte(`{"broken":`))
	st.Close()

	// 两帧本身都完整，但第二个载荷不是合法 JSON：验证器必须拒绝并回退
	st, commits, err := Open(dir, func(payload []byte) error {
		if !json.Valid(payload) {
			return errors.New("invalid json")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(commits) != 1 || string(commits[0]) != `{"ok":true}` {
		t.Fatalf("validator must reject incomplete commits: %q", commits)
	}
}
