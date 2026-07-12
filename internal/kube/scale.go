package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

func ScaleDeployments(ctx context.Context, client kubernetes.Interface, deps []DeploymentRef, replicas int32) error {
	scaled := make([]DeploymentRef, 0, len(deps))
	for _, dep := range deps {
		if err := scaleDeployment(ctx, client, dep, replicas); err != nil {
			rollbackErr := restoreDeploymentSet(ctx, client, scaled)
			if rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("roll back partially scaled Deployments: %w", rollbackErr))
			}
			return err
		}
		scaled = append(scaled, dep)
	}
	return nil
}

func scaleDeployment(ctx context.Context, client kubernetes.Interface, dep DeploymentRef, replicas int32) error {
	scale, err := client.AppsV1().Deployments(dep.Namespace).GetScale(ctx, dep.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get scale for Deployment %s/%s: %w", dep.Namespace, dep.Name, err)
	}
	scale.Spec.Replicas = replicas
	if _, err := client.AppsV1().Deployments(dep.Namespace).UpdateScale(ctx, dep.Name, scale, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("scale Deployment %s/%s to %d: %w", dep.Namespace, dep.Name, replicas, err)
	}
	return nil
}

func restoreDeploymentSet(ctx context.Context, client kubernetes.Interface, deps []DeploymentRef) error {
	var errs []error
	for _, dep := range deps {
		if err := scaleDeployment(ctx, client, dep, dep.Replicas); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func RestoreDeployments(ctx context.Context, client kubernetes.Interface, deps []DeploymentRef) error {
	return restoreDeploymentSet(ctx, client, deps)
}

func WaitForPVCUnmounted(ctx context.Context, client kubernetes.Interface, namespace, pvcName string, timeout, poll time.Duration) error {
	var holdouts []string
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := ActivePodsUsingPVC(ctx, client, namespace, pvcName)
		if err != nil {
			return false, err
		}
		holdouts = pods
		return len(pods) == 0, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for PVC %s/%s to be unmounted; still used by pods: %v", namespace, pvcName, holdouts)
	}
	return err
}

func WaitForPVCDeleted(ctx context.Context, client kubernetes.Interface, namespace, pvcName string, expectedUID types.UID, timeout, poll time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("get PVC while waiting for deletion: %w", err)
		}
		if expectedUID != "" && pvc.UID != expectedUID {
			return false, fmt.Errorf("PVC %s/%s was replaced while waiting for deletion: expected UID %s, found %s", namespace, pvcName, expectedUID, pvc.UID)
		}
		return false, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for PVC %s/%s deletion", namespace, pvcName)
	}
	return err
}
