// Copyright (c) 2024 Cisco and/or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux
// +build linux

package fs_test

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/networkservicemesh/sdk/pkg/tools/fs"
)

// WatchFile must survive fsnotify.NewWatcher() failing. Under file-descriptor or inotify-instance
// exhaustion NewWatcher returns (nil, EMFILE); closing that nil watcher panics and kills the
// process. This reproduces the exhaustion by dropping RLIMIT_NOFILE so no new fd can be allocated.
func TestWatchFileSurvivesWatcherCreationFailure(t *testing.T) {
	var orig syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
		t.Skipf("cannot read RLIMIT_NOFILE: %v", err)
	}
	// Soft limit of 0 makes every new fd allocation fail with EMFILE. The hard limit is left
	// untouched so an unprivileged process can restore it.
	restricted := syscall.Rlimit{Cur: 0, Max: orig.Max}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &restricted); err != nil {
		t.Skipf("cannot lower RLIMIT_NOFILE: %v", err)
	}
	defer func() {
		require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &orig))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Before the fix this panicked with a nil pointer dereference instead of returning.
	ch := fs.WatchFile(ctx, filepath.Join(t.TempDir(), "excluded_prefixes.yaml"))
	require.NotNil(t, ch)

	// The channel must be closed rather than left hanging, so callers ranging over it or
	// waiting on a first value make progress instead of blocking forever.
	select {
	case _, ok := <-ch:
		require.False(t, ok, "expected a closed channel when the watcher could not be created")
	case <-ctx.Done():
		t.Fatal("WatchFile returned a channel that was neither closed nor written to")
	}
}
