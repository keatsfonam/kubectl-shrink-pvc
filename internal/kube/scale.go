package kube

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

func ScaleDeployments(ctx context.Context, client kubernetes.Interface, deps []DeploymentRef, replicas int32) error {
	for _, dep := range deps {
		scale, err := client.AppsV1().Deployments(dep.Namespace).GetScale(ctx, dep.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get scale for Deployment %s/%s: %w", dep.Namespace, dep.Name, err)
		}
		scale.Spec.Replicas = replicas
		if _, err := client.AppsV1().Deployments(dep.Namespace).UpdateScale(ctx, dep.Name, scale, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("scale Deployment %s/%s to %d: %w", dep.Namespace, dep.Name, replicas, err)
		}
	}
	return nil
}

func RestoreDeployments(ctx context.Context, client kubernetes.Interface, deps []DeploymentRef) error {
	for _, dep := range deps {
		if err := ScaleDeployments(ctx, client, []DeploymentRef{dep}, dep.Replicas); err != nil {
			return err
		}
	}
	return nil
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

func WaitForPVCDeleted(ctx context.Context, client kubernetes.Interface, namespace, pvcName string, timeout, poll time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		return err != nil, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for PVC %s/%s deletion", namespace, pvcName)
	}
	return err
}
