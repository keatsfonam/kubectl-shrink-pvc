package pvcmanifest

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var bindingAnnotations = map[string]struct{}{
	"pv.kubernetes.io/bind-completed":                  {},
	"pv.kubernetes.io/bound-by-controller":             {},
	"volume.kubernetes.io/storage-provisioner":         {},
	"volume.beta.kubernetes.io/storage-provisioner":    {},
	"volume.kubernetes.io/selected-node":               {},
	"volume.kubernetes.io/storage-resizer":             {},
	"volume.beta.kubernetes.io/storage-resizer":        {},
	"kubectl.kubernetes.io/last-applied-configuration": {},
}

func Build(source *corev1.PersistentVolumeClaim, name string, target resource.Quantity) (*corev1.PersistentVolumeClaim, error) {
	if source == nil {
		return nil, fmt.Errorf("source PVC is nil")
	}
	if name == "" {
		return nil, fmt.Errorf("target PVC name is required")
	}

	pvc := source.DeepCopy()
	pvc.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"}
	pvc.ObjectMeta = metav1.ObjectMeta{
		Name:        name,
		Namespace:   source.Namespace,
		Labels:      copyStringMap(source.Labels),
		Annotations: sanitizedAnnotations(source.Annotations),
	}
	pvc.OwnerReferences = nil
	pvc.Finalizers = nil
	pvc.ManagedFields = nil
	pvc.Status = corev1.PersistentVolumeClaimStatus{}

	pvc.Spec.VolumeName = ""
	pvc.Spec.Selector = nil
	pvc.Spec.DataSource = nil
	pvc.Spec.DataSourceRef = nil
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = target
	pvc.Spec.Resources.Limits = nil

	return pvc, nil
}

func TempName(sourceName, suffix string) string {
	if suffix == "" {
		suffix = "shrink-tmp"
	}
	base := sourceName + "-" + suffix
	if len(base) <= 63 {
		return base
	}
	trim := 63 - len(suffix) - 1
	if trim < 1 {
		trim = 1
	}
	return strings.TrimRight(sourceName[:trim], "-") + "-" + suffix
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sanitizedAnnotations(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := map[string]string{}
	for k, v := range in {
		if _, drop := bindingAnnotations[k]; drop {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func CurrentSize(pvc *corev1.PersistentVolumeClaim) resource.Quantity {
	if pvc == nil {
		return resource.Quantity{}
	}
	requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	capacity := pvc.Status.Capacity[corev1.ResourceStorage]
	if capacity.IsZero() || requested.Cmp(capacity) >= 0 {
		return requested
	}
	return capacity
}
