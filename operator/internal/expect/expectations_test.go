// Copyright 2025 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expect

import (
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
)

const controlleeKey = "test-ns/test-resource"

func TestExpectCreations(t *testing.T) {
	testCases := []struct {
		description                   string
		existingUIDs                  []types.UID
		newUIDs                       []types.UID
		expectedCreateExpectationUIDs []types.UID
	}{
		{
			description:                   "should create new expectation when none exists",
			existingUIDs:                  nil,
			newUIDs:                       []types.UID{"1", "2"},
			expectedCreateExpectationUIDs: []types.UID{"1", "2"},
		},
		{
			description:                   "should add unique new expectations when there are existing expectations",
			existingUIDs:                  []types.UID{"1", "2"},
			newUIDs:                       []types.UID{"1", "3", "4"},
			expectedCreateExpectationUIDs: []types.UID{"1", "2", "3", "4"},
		},
	}

	expStore := NewExpectationsStore()
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			if tc.existingUIDs != nil {
				assert.NoError(t, initializeControlleeExpectations(expStore, controlleeKey, tc.existingUIDs, nil))
			}
			// test the method
			wg := sync.WaitGroup{}
			wg.Add(len(tc.newUIDs))
			for _, uid := range tc.newUIDs {
				go func() {
					defer wg.Done()
					err := expStore.ExpectCreations(logr.Discard(), controlleeKey, uid)
					assert.NoError(t, err)
				}()
			}
			wg.Wait()
			// compare the expected with actual
			assert.ElementsMatch(t, tc.expectedCreateExpectationUIDs, expStore.GetCreateExpectations(controlleeKey))
		})
	}
}

func TestObserveDeletions(t *testing.T) {
	testCases := []struct {
		description                   string
		existingUIDs                  []types.UID
		observedDeleteExpectationUIDs []types.UID
		expectedDeleteExpectationUIDs []types.UID
	}{
		{
			description:                   "should be no-op if UIDs to delete have already been removed",
			existingUIDs:                  []types.UID{"1", "3", "5"},
			observedDeleteExpectationUIDs: []types.UID{"8"},
			expectedDeleteExpectationUIDs: []types.UID{"1", "3", "5"},
		},
		{
			description:                   "should remove the observed deletions",
			existingUIDs:                  []types.UID{"1", "2", "3", "5"},
			observedDeleteExpectationUIDs: []types.UID{"2", "5"},
			expectedDeleteExpectationUIDs: []types.UID{"1", "3"},
		},
	}

	expStore := NewExpectationsStore()
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			if tc.existingUIDs != nil {
				assert.NoError(t, initializeControlleeExpectations(expStore, controlleeKey, nil, tc.existingUIDs))
			}
			expStore.ObserveDeletions(logr.Discard(), controlleeKey, tc.observedDeleteExpectationUIDs...)
			leftOverActualDeleteExpectations := expStore.GetDeleteExpectations(controlleeKey)
			assert.ElementsMatch(t, tc.expectedDeleteExpectationUIDs, leftOverActualDeleteExpectations)
		})
	}
}

func TestExpectDeletions(t *testing.T) {
	testCases := []struct {
		description                   string
		existingUIDs                  []types.UID
		newUIDs                       []types.UID
		expectedDeleteExpectationUIDs []types.UID
	}{
		{
			description:                   "should create new expectation when none exists",
			existingUIDs:                  nil,
			newUIDs:                       []types.UID{"1", "2"},
			expectedDeleteExpectationUIDs: []types.UID{"1", "2"},
		},
		{
			description:                   "should add unique new expectations when there are existing expectations",
			existingUIDs:                  []types.UID{"1", "2"},
			newUIDs:                       []types.UID{"1", "3", "4"},
			expectedDeleteExpectationUIDs: []types.UID{"1", "2", "3", "4"},
		},
	}

	expStore := NewExpectationsStore()
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			if tc.existingUIDs != nil {
				assert.NoError(t, initializeControlleeExpectations(expStore, controlleeKey, nil, tc.existingUIDs))
			}
			// test the method
			wg := sync.WaitGroup{}
			wg.Add(len(tc.newUIDs))
			for _, uid := range tc.newUIDs {
				go func() {
					defer wg.Done()
					err := expStore.ExpectDeletions(logr.Discard(), controlleeKey, uid)
					assert.NoError(t, err)
				}()
			}
			wg.Wait()
			// compare the expected with actual
			assert.ElementsMatch(t, tc.expectedDeleteExpectationUIDs, expStore.GetDeleteExpectations(controlleeKey))
		})
	}
}

func TestDeleteExpectations(t *testing.T) {
	testCases := []struct {
		description       string
		expectationsExist bool
	}{
		{
			description:       "should be a no-op when expectations do not exist",
			expectationsExist: false,
		},
		{
			description:       "should delete the existing expectations",
			expectationsExist: true,
		},
	}

	expStore := NewExpectationsStore()
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			if tc.expectationsExist {
				assert.NoError(t, initializeControlleeExpectations(expStore,
					controlleeKey,
					[]types.UID{"3"},
					[]types.UID{"1", "2"}))
			}
			err := expStore.DeleteExpectations(logr.Discard(), controlleeKey)
			assert.NoError(t, err)
			_, exists, err := expStore.GetExpectations(controlleeKey)
			assert.NoError(t, err)
			assert.False(t, exists)
		})
	}
}

func TestSyncExpectations(t *testing.T) {
	testCases := []struct {
		description                   string
		controlleeKey                 string
		createExpectationUIDs         []types.UID
		deleteExpectationsUIDs        []types.UID
		existingNonTerminatingUIDs    []types.UID
		existingTerminatingUIDs       []types.UID
		createExpectationUIDsPostSync []types.UID
		deleteExpectationUIDsPostSync []types.UID
	}{
		{
			description:                   "should sync both create and delete expectations",
			controlleeKey:                 controlleeKey,
			createExpectationUIDs:         []types.UID{"1", "2"},
			deleteExpectationsUIDs:        []types.UID{"3", "6"},
			existingNonTerminatingUIDs:    []types.UID{"1", "3", "4", "7"},
			createExpectationUIDsPostSync: []types.UID{"2"},
			deleteExpectationUIDsPostSync: []types.UID{"3"},
		},
		{
			description:                   "should re-add terminating pods",
			controlleeKey:                 controlleeKey,
			createExpectationUIDs:         []types.UID{"1"},
			deleteExpectationsUIDs:        []types.UID{},
			existingNonTerminatingUIDs:    []types.UID{"2", "3"},
			existingTerminatingUIDs:       []types.UID{"4", "5"},
			createExpectationUIDsPostSync: []types.UID{"1"},
			deleteExpectationUIDsPostSync: []types.UID{"4", "5"},
		},
		{
			description:                   "should be a no-op when expectations do not exist",
			controlleeKey:                 "does-not-exist",
			existingNonTerminatingUIDs:    []types.UID{"1", "2"},
			createExpectationUIDsPostSync: []types.UID{},
			deleteExpectationUIDsPostSync: []types.UID{},
		},
	}

	expStore := NewExpectationsStore()
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.NoError(t, initializeControlleeExpectations(expStore, tc.controlleeKey, tc.createExpectationUIDs, tc.deleteExpectationsUIDs))
			expStore.SyncExpectations(tc.controlleeKey, tc.existingNonTerminatingUIDs, tc.existingTerminatingUIDs)
			assert.ElementsMatch(t, tc.createExpectationUIDsPostSync, expStore.GetCreateExpectations(tc.controlleeKey))
			assert.ElementsMatch(t, tc.deleteExpectationUIDsPostSync, expStore.GetDeleteExpectations(tc.controlleeKey))
		})
	}
}

func TestControlleeExpectationsKey(t *testing.T) {
	exp := &ControlleeExpectations{key: controlleeKey}
	assert.Equal(t, controlleeKey, exp.Key())
}

// TestDeleteExpectationsByIndex verifies that a consumer-registered indexer groups expectations by a
// value derived from their key, and that DeleteExpectationsByIndex clears only the targeted group.
func TestDeleteExpectationsByIndex(t *testing.T) {
	const (
		indexName = "byOwner"
		ownerA    = "test-ns/owner-a"
		ownerB    = "test-ns/owner-b"
	)
	// The consumer groups by the key prefix that precedes the last path segment.
	indexers := cache.Indexers{
		indexName: func(obj any) ([]string, error) {
			exp := obj.(*ControlleeExpectations)
			key := exp.Key()
			return []string{key[:strings.LastIndex(key, "/")]}, nil
		},
	}
	expStore := NewExpectationsStore()
	require.NoError(t, expStore.AddIndexers(indexers))

	// Two expectations belong to ownerA and one to ownerB.
	require.NoError(t, initializeControlleeExpectations(expStore, ownerA+"/scope-0", []types.UID{"1"}, nil))
	require.NoError(t, initializeControlleeExpectations(expStore, ownerA+"/scope-1", []types.UID{"2"}, nil))
	require.NoError(t, initializeControlleeExpectations(expStore, ownerB+"/scope-0", []types.UID{"3"}, nil))

	grouped, err := expStore.ByIndex(indexName, ownerA)
	require.NoError(t, err)
	assert.Len(t, grouped, 2, "both of ownerA's expectations must be in the group")

	require.NoError(t, expStore.DeleteExpectationsByIndex(logr.Discard(), indexName, ownerA))

	_, existsA0, err := expStore.GetExpectations(ownerA + "/scope-0")
	require.NoError(t, err)
	assert.False(t, existsA0, "ownerA's expectations must be deleted")
	_, existsA1, err := expStore.GetExpectations(ownerA + "/scope-1")
	require.NoError(t, err)
	assert.False(t, existsA1, "ownerA's expectations must be deleted")
	_, existsB0, err := expStore.GetExpectations(ownerB + "/scope-0")
	require.NoError(t, err)
	assert.True(t, existsB0, "ownerB's expectations must be untouched")
}

// TestConcurrentAccessIsRaceFree exercises the store from multiple goroutines so the race detector
// observes concurrent access to the same expectation set. Writers raise and lower delete expectations
// (mutating the set under the lock) while readers query it. Run with -race.
func TestConcurrentAccessIsRaceFree(t *testing.T) {
	const (
		key        = "test-ns/test-resource"
		goroutines = 8
		iterations = 500
	)
	store := NewExpectationsStore()
	logger := logr.Discard()
	uids := []types.UID{"a", "b", "c", "d"}
	require.NoError(t, store.ExpectDeletions(logger, key, uids...))

	var wg sync.WaitGroup
	// Writers raise and lower delete expectations, mutating the underlying set.
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				_ = store.ExpectDeletions(logger, key, uids...)
				store.ObserveDeletions(logger, key, uids...)
			}
		})
	}
	// Readers query the same expectation set.
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				_ = store.HasDeleteExpectation(key, "a")
				_ = store.GetDeleteExpectations(key)
			}
		})
	}
	wg.Wait()
}

func initializeControlleeExpectations(expStore *ExpectationsStore, controlleeKey string, uidsToAdd, uidsToDelete []types.UID) error {
	return expStore.Add(&ControlleeExpectations{
		key:          controlleeKey,
		uidsToAdd:    sets.New(uidsToAdd...),
		uidsToDelete: sets.New(uidsToDelete...),
	})
}

// ==================== Index Tracking Tests ====================

func TestExpectCreationsWithIndex(t *testing.T) {
	expStore := NewExpectationsStore()
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 2))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-2"), 3))

	assert.ElementsMatch(t, []types.UID{"uid-1", "uid-2"}, expStore.GetCreateExpectations(controlleeKey))
	indices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Len(t, indices, 2)
	assert.ElementsMatch(t, []int{2, 3}, indices)
}

func TestGetCreateExpectationIndices_AfterSync(t *testing.T) {
	expStore := NewExpectationsStore()
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 2))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-2"), 3))

	// After sync (observe uid-1), only uid-2's index remains reserved
	expStore.SyncExpectations(controlleeKey, []types.UID{"uid-1"}, nil)
	assert.ElementsMatch(t, []int{3}, expStore.GetCreateExpectationIndices(controlleeKey))
}

func TestGetCreateExpectationIndices_NilIndices(t *testing.T) {
	// When expectations exist but no indices were recorded (e.g. via initializeControlleeExpectations),
	// GetCreateExpectationIndices should return nil.
	expStore := NewExpectationsStore()
	assert.NoError(t, initializeControlleeExpectations(expStore, controlleeKey, []types.UID{"uid-1", "uid-2"}, nil))

	indices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Nil(t, indices)
}

func TestGetCreateExpectationIndices_NoExpectations(t *testing.T) {
	expStore := NewExpectationsStore()
	indices := expStore.GetCreateExpectationIndices("nonexistent-key")
	assert.Nil(t, indices)
}

func TestLowerExpectations_CleansCreationIndices(t *testing.T) {
	expStore := NewExpectationsStore()
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 10))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-2"), 20))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-3"), 30))

	// Verify all 3 indices are present
	assert.ElementsMatch(t, []int{10, 20, 30}, expStore.GetCreateExpectationIndices(controlleeKey))

	// Sync with uid-1 observed (it appears in non-terminating UIDs)
	expStore.SyncExpectations(controlleeKey, []types.UID{"uid-1"}, nil)

	// uid-1's index (10) should be gone
	assert.ElementsMatch(t, []int{20, 30}, expStore.GetCreateExpectationIndices(controlleeKey))
	assert.ElementsMatch(t, []types.UID{"uid-2", "uid-3"}, expStore.GetCreateExpectations(controlleeKey))
}

func TestExpectCreationsWithIndex_OverwriteIndex(t *testing.T) {
	// If the same UID is registered again with a different index, the new index should overwrite.
	expStore := NewExpectationsStore()
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 5))
	assert.ElementsMatch(t, []int{5}, expStore.GetCreateExpectationIndices(controlleeKey))

	// Re-register same UID with different index
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 99))
	indices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Len(t, indices, 1)
	assert.Equal(t, 99, indices[0])
}

func TestExpectCreationsAndDeletions_Interleaved(t *testing.T) {
	// Test the scenario where a UID is added and then deleted in the same expectations set.
	// The delete should remove the UID from uidsToAdd and clean up its creationIndex.
	expStore := NewExpectationsStore()

	// Create expectations with indices
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 10))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-2"), 20))

	// Now expect deletion of uid-1 (this removes uid-1 from uidsToAdd)
	assert.NoError(t, expStore.ExpectDeletions(logr.Discard(), controlleeKey, types.UID("uid-1")))

	// uid-1 should be removed from create expectations and its index cleaned up
	assert.ElementsMatch(t, []types.UID{"uid-2"}, expStore.GetCreateExpectations(controlleeKey))
	assert.ElementsMatch(t, []int{20}, expStore.GetCreateExpectationIndices(controlleeKey))
}

func TestFullLifecycle_CreateExpectation_GetIndices_Observe_Cleanup(t *testing.T) {
	expStore := NewExpectationsStore()

	// Step 1: Reconciler creates 3 pods with indices 0, 1, 2
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("pod-uid-a"), 0))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("pod-uid-b"), 1))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("pod-uid-c"), 2))

	// Step 2: Next reconcile starts, informer cache has pod-uid-a but not b and c yet
	// GetCreateExpectationIndices should return all 3 indices as reserved
	allIndices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Len(t, allIndices, 3)
	assert.ElementsMatch(t, []int{0, 1, 2}, allIndices)

	// Step 3: SyncExpectations sees pod-uid-a in cache
	expStore.SyncExpectations(controlleeKey, []types.UID{"pod-uid-a"}, nil)

	// Step 4: Now only indices 1, 2 should be reserved
	reservedIndices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Len(t, reservedIndices, 2)
	assert.ElementsMatch(t, []int{1, 2}, reservedIndices)

	// Step 5: pod-uid-b and pod-uid-c appear in cache
	expStore.SyncExpectations(controlleeKey, []types.UID{"pod-uid-b", "pod-uid-c"}, nil)

	// Step 6: No more reserved indices
	finalIndices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Nil(t, finalIndices)
	assert.Empty(t, expStore.GetCreateExpectations(controlleeKey))
}

func TestConcurrentExpectCreationsWithIndex(t *testing.T) {
	expStore := NewExpectationsStore()
	const numGoroutines = 50

	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			uid := types.UID("uid-" + string(rune('0'+idx)))
			_ = expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, uid, idx)
		}(i)
	}
	wg.Wait()

	// All 50 UIDs should be present
	createExps := expStore.GetCreateExpectations(controlleeKey)
	assert.Len(t, createExps, numGoroutines)

	// All 50 indices should be present
	indices := expStore.GetCreateExpectationIndices(controlleeKey)
	assert.Len(t, indices, numGoroutines)
}

func TestSyncExpectations_WithCreationIndices(t *testing.T) {
	expStore := NewExpectationsStore()
	key := "test-ns/indices-sync"
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), key, types.UID("uid-1"), 10))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), key, types.UID("uid-2"), 20))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), key, types.UID("uid-3"), 30))

	// Sync: uid-1 is now visible as non-terminating
	expStore.SyncExpectations(key, []types.UID{"uid-1"}, nil)

	// uid-1's index must be gone; uid-2, uid-3 still reserved
	assert.ElementsMatch(t, []int{20, 30}, expStore.GetCreateExpectationIndices(key))
	assert.ElementsMatch(t, []types.UID{"uid-2", "uid-3"}, expStore.GetCreateExpectations(key))
}

func TestDeleteExpectations_CleansIndices(t *testing.T) {
	expStore := NewExpectationsStore()
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-1"), 5))
	assert.NoError(t, expStore.ExpectCreationsWithIndex(logr.Discard(), controlleeKey, types.UID("uid-2"), 10))

	// Delete all expectations
	assert.NoError(t, expStore.DeleteExpectations(logr.Discard(), controlleeKey))

	// Everything should be gone
	assert.Nil(t, expStore.GetCreateExpectationIndices(controlleeKey))
	assert.Nil(t, expStore.GetCreateExpectations(controlleeKey))
}

func TestDeepCopy_WithCreationIndices(t *testing.T) {
	original := &ControlleeExpectations{
		key:          "test-key",
		uidsToAdd:    sets.New[types.UID]("uid-1", "uid-2"),
		uidsToDelete: sets.New[types.UID]("uid-3"),
		creationIndices: map[types.UID]int{
			"uid-1": 10,
			"uid-2": 20,
		},
	}

	copied := original.deepCopy()

	// Verify the copy has the same values
	assert.Equal(t, original.key, copied.key)
	assert.ElementsMatch(t, original.uidsToAdd.UnsortedList(), copied.uidsToAdd.UnsortedList())
	assert.ElementsMatch(t, original.uidsToDelete.UnsortedList(), copied.uidsToDelete.UnsortedList())
	assert.Equal(t, len(original.creationIndices), len(copied.creationIndices))
	for uid, idx := range original.creationIndices {
		assert.Equal(t, idx, copied.creationIndices[uid])
	}

	// Verify mutations to the copy don't affect the original
	copied.uidsToAdd.Insert("uid-99")
	copied.creationIndices["uid-99"] = 99

	assert.False(t, original.uidsToAdd.Has("uid-99"))
	assert.NotContains(t, original.creationIndices, types.UID("uid-99"))
}
