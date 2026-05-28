package manifest

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// fileName is intentionally boring for now. Real systems often use numbered
// manifests (MANIFEST-000001, MANIFEST-000002, ...), but one text file is enough
// for this simple prototype.
const fileName = "MANIFEST"

var ErrCorruptManifest = errors.New("manifest: corrupt manifest")

type Manifest struct {
	path    string
	file    *os.File
	dirFile *os.File
}

// SSTable is one table file recorded in the manifest.
type SSTable struct {
	FileNum uint64
	Path    string
}

// State is the catalog reconstructed by replaying the manifest.
type State struct {
	SSTables    []SSTable
	NextFileNum uint64
}

// Edit is the unit of change appended to the manifest.
//
// A flush currently produces one edit:
//
//	add <file number> <sstable path>
//	next <next file number>
//
// This keeps the durable catalog in sync with the files the DB has created.
// The pointer lets callers omit the "next" line when they do not want to change
// the allocator state.
type Edit struct {
	AddSSTables []SSTable
	NextFileNum *uint64
}

// Open loads the current manifest state and returns an append handle for future
// edits. Replaying happens before opening for append so the DB starts with the
// same SSTable catalog it had before shutdown.
func Open(dir string) (*Manifest, State, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, State{}, err
	}

	path := Path(dir)
	state, err := ReplayFile(path)
	if err != nil {
		return nil, State{}, err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, State{}, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = dirFile.Close()
		return nil, State{}, err
	}

	return &Manifest{
		path:    path,
		file:    file,
		dirFile: dirFile,
	}, state, nil
}

// ReplayFile rebuilds State by scanning the manifest from top to bottom.
//
// The format is line-oriented and human-readable:
//
//	add 1 /tmp/db/sst/000001.sst
//	next 2
//
// It is not trying to be crash-perfect yet. The goal here is to make the
// durable catalog concept easy to understand before adding checksums,
// fragmentation, manifest rotation, or compaction edits.
func ReplayFile(path string) (State, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return State{NextFileNum: 1}, nil
		}
		return State{}, err
	}
	defer file.Close()

	state := State{NextFileNum: 1}
	var maxFileNum uint64

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() { // scanning line by line. So nice to do in go.
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := applyLine(&state, &maxFileNum, line); err != nil {
			return state, fmt.Errorf("%w: line %d: %v", ErrCorruptManifest, lineNum, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return state, err
	}

	// Defensive fallback: if the manifest contains "add 7 ..." but no later
	// "next 8", do not allocate file number 7 again.
	if state.NextFileNum <= maxFileNum {
		state.NextFileNum = maxFileNum + 1
	}
	return state, nil
}

// Append writes one manifest edit and syncs it. DB flush relies on this ordering:
// the SSTable file is written first, this manifest edit is made durable second,
// and only then is the table published in the in-memory catalog.
func (m *Manifest) Append(edit Edit) error {
	lines, err := encodeEdit(edit)
	if err != nil {
		return err
	}
	if _, err := m.file.WriteString(lines); err != nil {
		return err
	}
	if err := m.file.Sync(); err != nil {
		return err
	}
	return m.dirFile.Sync()
}

func (m *Manifest) Close() error {
	if err := m.file.Close(); err != nil {
		return err
	}
	return m.dirFile.Close()
}

func Path(dir string) string {
	return filepath.Join(dir, fileName)
}

// encodeEdit turns an in-memory catalog change into manifest lines.
//
// Example:
//
//	Edit{
//	    AddSSTables: []SSTable{{FileNum: 1, Path: "/tmp/db/sst/000001.sst"}},
//	    NextFileNum: &next,
//	}
//
// becomes:
//
//	add 1 /tmp/db/sst/000001.sst
//	next 2
func encodeEdit(edit Edit) (string, error) {
	var b strings.Builder
	for _, table := range edit.AddSSTables {
		if table.FileNum == 0 || table.Path == "" {
			return "", ErrCorruptManifest
		}
		fmt.Fprintf(&b, "add %d %s\n", table.FileNum, table.Path)
	}
	if edit.NextFileNum != nil {
		if *edit.NextFileNum == 0 {
			return "", ErrCorruptManifest
		}
		fmt.Fprintf(&b, "next %d\n", *edit.NextFileNum)
	}
	if b.Len() == 0 {
		return "", ErrCorruptManifest
	}
	return b.String(), nil
}

// applyLine is the replay interpreter for one manifest line. It mutates the
// recovered State in place because replay is just "start empty, apply each line".
func applyLine(state *State, maxFileNum *uint64, line string) error {
	cmd, rest, ok := strings.Cut(line, " ")
	if !ok {
		return fmt.Errorf("missing command payload")
	}

	switch cmd {
	case "add":
		numText, path, ok := strings.Cut(strings.TrimSpace(rest), " ")
		if !ok {
			return fmt.Errorf("add requires file number and path")
		}
		fileNum, err := strconv.ParseUint(numText, 10, 64)
		if err != nil || fileNum == 0 {
			return fmt.Errorf("invalid sstable file number")
		}
		path = strings.TrimSpace(path)
		if path == "" {
			return fmt.Errorf("empty sstable path")
		}
		state.SSTables = append(state.SSTables, SSTable{
			FileNum: fileNum,
			Path:    path,
		})
		if fileNum > *maxFileNum {
			*maxFileNum = fileNum
		}
	case "next":
		next, err := strconv.ParseUint(strings.TrimSpace(rest), 10, 64)
		if err != nil || next == 0 {
			return fmt.Errorf("invalid next file number")
		}
		state.NextFileNum = next
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}
