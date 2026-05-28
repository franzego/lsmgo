package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestAppendReplayAddSSTable(t *testing.T) {
	dir := t.TempDir()
	m, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	path := filepath.Join(dir, "000007.sst")
	if err := m.Append(Edit{AddSSTables: []SSTable{{FileNum: 7, Path: path}}}); err != nil {
		t.Fatalf("append add: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close manifest: %v", err)
	}

	raw, err := os.ReadFile(Path(dir))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if got, want := string(raw), "add 7 "+path+"\n"; got != want {
		t.Fatalf("manifest contents = %q, want %q", got, want)
	}

	state, err := ReplayFile(Path(dir))
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if len(state.SSTables) != 1 || state.SSTables[0].FileNum != 7 || state.SSTables[0].Path != path {
		t.Fatalf("unexpected sstable state: %+v", state.SSTables)
	}
	if state.NextFileNum != 8 {
		t.Fatalf("expected next file num inferred as 8, got %d", state.NextFileNum)
	}
}

func TestManifestAppendReplaySetNextFileNum(t *testing.T) {
	dir := t.TempDir()
	m, _, err := Open(dir)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	next := uint64(42)
	if err := m.Append(Edit{NextFileNum: &next}); err != nil {
		t.Fatalf("append set next: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close manifest: %v", err)
	}

	state, err := ReplayFile(Path(dir))
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if state.NextFileNum != 42 {
		t.Fatalf("expected next file num 42, got %d", state.NextFileNum)
	}
}

func TestManifestReplayAppliesMultipleLinesInOrder(t *testing.T) {
	dir := t.TempDir()
	contents := strings.Join([]string{
		"add 1 " + filepath.Join(dir, "000001.sst"),
		"next 2",
		"add 2 " + filepath.Join(dir, "000002.sst"),
		"next 3",
		"",
	}, "\n")
	if err := os.WriteFile(Path(dir), []byte(contents), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	state, err := ReplayFile(Path(dir))
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if len(state.SSTables) != 2 {
		t.Fatalf("expected two sstables, got %d", len(state.SSTables))
	}
	if state.NextFileNum != 3 {
		t.Fatalf("expected next file num 3, got %d", state.NextFileNum)
	}
}

func TestManifestReplayFailsOnMalformedLine(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(Path(dir), []byte("add not-a-number x.sst\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	_, err := ReplayFile(Path(dir))
	if !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("expected ErrCorruptManifest, got %v", err)
	}
}
