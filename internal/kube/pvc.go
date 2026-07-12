package kube

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// DeletePVC deletes exactly the PVC identified by name and UID. The UID
// precondition prevents a replacement object with the same name from being
// deleted if another controller or operator wins a race.
func DeletePVC(ctx context.Context, client kubernetes.Interface, namespace, name string, uid types.UID) error {
	if uid == "" {
		return fmt.Errorf("delete PVC %s/%s: UID is required", namespace, name)
	}
	err := client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete PVC %s/%s: %w", namespace, name, err)
	}
	return nil
}
