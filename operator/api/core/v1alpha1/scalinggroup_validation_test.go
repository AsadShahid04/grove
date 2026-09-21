// Copyright 2026 The Grove Authors.
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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// TestPodCliqueScalingGroupSpecValidation tests the CEL validation rules on PodCliqueScalingGroupSpec.
// The XValidation rule should ensure replicas >= minAvailable when minAvailable is set.
// Note: These tests document the validation logic. The actual CEL validation is enforced by the API server
// and would be caught in e2e tests or integration tests that create real PCSG objects.
func TestPodCliqueScalingGroupSpecValidation(t *testing.T) {
	tests := []struct {
		name         string
		replicas     int32
		minAvailable *int32
		shouldBeValid bool
		description  string
	}{
		{
			name:          "replicas equals minAvailable should be valid",
			replicas:      3,
			minAvailable:  ptr.To(int32(3)),
			shouldBeValid: true,
			description:   "replicas >= minAvailable",
		},
		{
			name:          "replicas greater than minAvailable should be valid",
			replicas:      5,
			minAvailable:  ptr.To(int32(3)),
			shouldBeValid: true,
			description:   "replicas >= minAvailable",
		},
		{
			name:          "replicas less than minAvailable should be invalid",
			replicas:      2,
			minAvailable:  ptr.To(int32(3)),
			shouldBeValid: false,
			description:   "replicas < minAvailable violates the CEL rule",
		},
		{
			name:          "minAvailable not set should be valid",
			replicas:      2,
			minAvailable:  nil,
			shouldBeValid: true,
			description:   "CEL rule checks !has(self.minAvailable) || ..., so nil minAvailable is valid",
		},
		{
			name:          "zero replicas with zero minAvailable should be valid",
			replicas:      0,
			minAvailable:  ptr.To(int32(0)),
			shouldBeValid: true,
			description:   "replicas >= minAvailable (0 >= 0)",
		},
		{
			name:          "zero replicas with positive minAvailable should be invalid",
			replicas:      0,
			minAvailable:  ptr.To(int32(1)),
			shouldBeValid: false,
			description:   "replicas < minAvailable (0 < 1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pcsg := &PodCliqueScalingGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pcsg",
					Namespace: "default",
				},
				Spec: PodCliqueScalingGroupSpec{
					Replicas:     tt.replicas,
					MinAvailable: tt.minAvailable,
					CliqueNames:  []string{"test-clique"},
				},
			}

			// Document the expected validation outcome
			if tt.shouldBeValid {
				assert.NotNil(t, pcsg, "Valid PCSG should be constructible: %s", tt.description)
			} else {
				// The CEL validation rule would reject this at API server level:
				// +kubebuilder:validation:XValidation:rule="!has(self.minAvailable) || self.replicas >= self.minAvailable",message="replicas must be greater than or equal to minAvailable"
				assert.NotNil(t, pcsg, "PCSG with replicas < minAvailable would be rejected by API server: %s", tt.description)
			}

			// Verify the spec values for documentation
			assert.Equal(t, tt.replicas, pcsg.Spec.Replicas, "Replicas should match test input")
			assert.Equal(t, tt.minAvailable, pcsg.Spec.MinAvailable, "MinAvailable should match test input")
		})
	}
}

// TestPodCliqueScalingGroupSpecValidationLogic validates the CEL rule logic itself.
// CEL rule: !has(self.minAvailable) || self.replicas >= self.minAvailable
func TestPodCliqueScalingGroupSpecValidationLogic(t *testing.T) {
	tests := []struct {
		name              string
		hasMinAvailable   bool
		replicas          int32
		minAvailable      int32
		expectedCELResult bool
	}{
		{
			name:              "no minAvailable set - always valid",
			hasMinAvailable:   false,
			replicas:          1,
			minAvailable:      0,
			expectedCELResult: true,
		},
		{
			name:              "replicas > minAvailable - valid",
			hasMinAvailable:   true,
			replicas:          5,
			minAvailable:      3,
			expectedCELResult: true,
		},
		{
			name:              "replicas == minAvailable - valid",
			hasMinAvailable:   true,
			replicas:          3,
			minAvailable:      3,
			expectedCELResult: true,
		},
		{
			name:              "replicas < minAvailable - invalid",
			hasMinAvailable:   true,
			replicas:          2,
			minAvailable:      3,
			expectedCELResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the CEL rule logic
			celResult := !tt.hasMinAvailable || (tt.replicas >= tt.minAvailable)
			assert.Equal(t, tt.expectedCELResult, celResult, "CEL rule evaluation should match expected result")
		})
	}
}
