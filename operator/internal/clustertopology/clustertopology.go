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

package clustertopology

import (
	"context"
	"fmt"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/apierrors"
	"github.com/ai-dynamo/grove/operator/internal/retry"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DefaultSyncRetryBackoff is the retry backoff used by SynchronizeTopologyWithRetry.
// Six steps with doubling delay gives roughly 60 seconds of retry budget (1+2+4+8+16+32).
var DefaultSyncRetryBackoff = wait.Backoff{
	Duration: time.Second,
	Factor:   2.0,
	Jitter:   0.1,
	Steps:    6,
}

// SynchronizeTopologyWithRetry wraps SynchronizeTopology with a generic, context-aware
// retry mechanism using bounded exponential backoff. Transient API-server errors
// (5xx, timeouts, service-unavailable, transport errors) at operator startup are retried.
// Permanent errors (Forbidden, Unauthorized) are returned immediately without retrying.
// If ctx is canceled during a backoff sleep, the function returns ctx.Err() immediately.
func SynchronizeTopologyWithRetry(ctx context.Context, cl client.Client, logger logr.Logger, backends map[string]scheduler.TopologyAwareBackend, backoff wait.Backoff) error {
	retrier := retry.Retrier{
		Backoff:     backoff,
		IsRetryable: apierrors.IsTransientAPIError,
		OnRetry: func(attempt int, err error) {
			logger.Info("Retrying topology synchronization after transient error", "attempt", attempt, "error", err.Error())
		},
	}
	return retrier.Do(ctx, func() error {
		return SynchronizeTopology(ctx, cl, logger, backends)
	})
}

// SynchronizeTopology synchronizes scheduler-specific topology resources at operator startup.
// Lists all existing ClusterTopologyBinding resources and ensures backend topologies exist for each.
// Called before controllers start to avoid races with PCS reconciliation.
func SynchronizeTopology(ctx context.Context, cl client.Client, logger logr.Logger, backends map[string]scheduler.TopologyAwareBackend) error {
	ctList := &grovecorev1alpha1.ClusterTopologyBindingList{}
	if err := cl.List(ctx, ctList); err != nil {
		return fmt.Errorf("failed to list ClusterTopologyBinding resources: %w", err)
	}
	for i := range ctList.Items {
		ct := &ctList.Items[i]
		schedulerRefMap := BuildSchedulerReferenceMap(ct.Spec.SchedulerTopologyBindings)

		for backendName, tasBackend := range backends {
			// Only sync grove-managed scheduler topology resources (not listed in schedulerTopologyReferences).
			// Externally-managed scheduler topology resources are handled by the ClusterTopologyBinding controller via CheckTopologyDrift.
			if _, isExternallyManaged := schedulerRefMap[backendName]; isExternallyManaged {
				continue
			}
			if err := tasBackend.SyncTopology(ctx, cl, ct); err != nil {
				return fmt.Errorf("failed to sync topology %s for backend %s: %w", ct.Name, backendName, err)
			}
		}
		logger.Info("Synchronized backend topologies for ClusterTopologyBinding", "name", ct.Name)
	}
	return nil
}

// GetClusterTopologyLevels retrieves the TopologyLevels from the specified ClusterTopologyBinding resource.
func GetClusterTopologyLevels(ctx context.Context, cl client.Client, name string) ([]grovecorev1alpha1.TopologyLevel, error) {
	clusterTopology := &grovecorev1alpha1.ClusterTopologyBinding{}
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, clusterTopology); err != nil {
		return nil, err
	}
	return clusterTopology.Spec.Levels, nil
}

// BuildSchedulerReferenceMap builds a map from scheduler name to SchedulerTopologyReference pointer.
func BuildSchedulerReferenceMap(refs []grovecorev1alpha1.SchedulerTopologyBinding) map[string]*grovecorev1alpha1.SchedulerTopologyBinding {
	m := make(map[string]*grovecorev1alpha1.SchedulerTopologyBinding, len(refs))
	for i := range refs {
		m[refs[i].SchedulerName] = &refs[i]
	}
	return m
}
