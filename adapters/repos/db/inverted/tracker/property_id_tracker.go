//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tracker

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type JsonPropertyIdTracker struct {
	path        string
	LastId      uint64
	PropertyIds map[string]uint64
	sync.Mutex
}

func NewJsonPropertyIdTracker(path string) (*JsonPropertyIdTracker, error) {
	t := &JsonPropertyIdTracker{
		path:        path,
		PropertyIds: make(map[string]uint64),
		LastId:      1,
	}

	// read the file into memory
	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			t.Flush(false)
			return t, nil
		}
		return nil, err
	}

	// Unmarshal the data

	if err := json.Unmarshal(bytes, &t); err != nil {
		return nil, err
	}
	t.path = path

	// Leave zero undefined to catch undefined key lookups
	if t.LastId == 0 {
		t.LastId = 1
	}

	return t, nil
}

// Writes the current state of the tracker to disk.  (flushBackup = true) will only write the backup file
func (t *JsonPropertyIdTracker) Flush(flushBackup bool) error {
	if !flushBackup { // Write the backup file first
		t.Flush(true)
	}

	t.Lock()
	defer t.Unlock()

	bytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	filename := t.path
	if flushBackup {
		filename = t.path + ".bak"
	}

	// Do a write+rename to avoid corrupting the file if we crash while writing
	tempfile := filename + ".tmp"

	err = os.WriteFile(tempfile, bytes, 0o666)
	if err != nil {
		return err
	}

	err = os.Rename(tempfile, filename)
	if err != nil {
		return err
	}

	return nil
}

// Drop removes the tracker from disk
func (t *JsonPropertyIdTracker) Drop() error {
	t.Lock()

	defer t.Unlock()

	if err := os.Remove(t.path); err != nil {
		return fmt.Errorf("remove prop length tracker state from disk:%v, %w", t.path, err)
	}
	if err := os.Remove(t.path + ".bak"); err != nil {
		return fmt.Errorf("remove prop length tracker state from disk:%v, %w", t.path+".bak", err)
	}

	return nil
}

func (t *JsonPropertyIdTracker) GetIdForProperty(property string) (uint64, error) {
	if t == nil {
		return 0, fmt.Errorf("property id tracker not initialised")
	}

	t.Lock()
	defer t.Unlock()

	if id, ok := t.PropertyIds[property]; ok {
		return id, nil
	}

	return t.doCreateProperty(property)
}

func (t *JsonPropertyIdTracker) CreateProperty(property string) (uint64, error) {
	t.Lock()
	defer t.Unlock()

	return t.doCreateProperty(property)
}

func (t *JsonPropertyIdTracker) doCreateProperty(property string) (uint64, error) {
	if id, ok := t.PropertyIds[property]; ok {
		return id, fmt.Errorf("property %v already exists\n", property)
	}

	t.LastId++
	t.PropertyIds[property] = t.LastId

	return t.LastId, nil
}