package kube

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type DeploymentRef struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
	Replicas  int32     `json:"replicas"`
}

type UnsupportedConsumer struct {
	Kind string
	Name string
	Pod  string
}

type ConsumerPlan struct {
	Pods        []string
	Deployments []DeploymentRef
	Unsupported []UnsupportedConsumer
}

type HPARef struct {
	Namespace      string
	Name           string
	DeploymentName string
}

func DiscoverConsumers(ctx context.Context, client kubernetes.Interface, namespace, pvcName string) (*ConsumerPlan, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	plan := &ConsumerPlan{}
	deployments := map[string]DeploymentRef{}
	unsupported := map[string]UnsupportedConsumer{}
	addUnsupported := func(kind, name, pod string) {
		key := kind + "/" + name
		if _, exists := unsupported[key]; !exists {
			unsupported[key] = UnsupportedConsumer{Kind: kind, Name: name, Pod: pod}
		}
	}

	for i := range pods.Items {
		pod := pods.Items[i]
		if !podUsesPVC(&pod, pvcName) || isTerminalPod(&pod) {
			continue
		}
		plan.Pods = append(plan.Pods, pod.Name)
		dep, unsup, err := resolvePodOwner(ctx, client, namespace, &pod)
		if err != nil {
			return nil, err
		}
		if dep != nil {
			deployments[dep.Namespace+"/"+dep.Name] = *dep
		} else if unsup != nil {
			addUnsupported(unsup.Kind, unsup.Name, unsup.Pod)
		}
	}

	deps, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Deployments: %w", err)
	}
	for i := range deps.Items {
		dep := &deps.Items[i]
		if templateUsesPVC(&dep.Spec.Template.Spec, pvcName) {
			replicas := int32(1)
			if dep.Spec.Replicas != nil {
				replicas = *dep.Spec.Replicas
			}
			deployments[namespace+"/"+dep.Name] = DeploymentRef{Namespace: namespace, Name: dep.Name, UID: dep.UID, Replicas: replicas}
		}
	}

	statefulSets, err := client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list StatefulSets: %w", err)
	}
	for i := range statefulSets.Items {
		if templateUsesPVC(&statefulSets.Items[i].Spec.Template.Spec, pvcName) {
			addUnsupported("StatefulSet", statefulSets.Items[i].Name, "")
		}
	}
	daemonSets, err := client.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list DaemonSets: %w", err)
	}
	for i := range daemonSets.Items {
		if templateUsesPVC(&daemonSets.Items[i].Spec.Template.Spec, pvcName) {
			addUnsupported("DaemonSet", daemonSets.Items[i].Name, "")
		}
	}
	replicaSets, err := client.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list ReplicaSets: %w", err)
	}
	for i := range replicaSets.Items {
		rs := &replicaSets.Items[i]
		if owner := controllerOwner(rs.OwnerReferences); owner != nil && owner.Kind == "Deployment" {
			continue
		}
		if templateUsesPVC(&rs.Spec.Template.Spec, pvcName) {
			addUnsupported("ReplicaSet", rs.Name, "")
		}
	}
	jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Jobs: %w", err)
	}
	for i := range jobs.Items {
		if templateUsesPVC(&jobs.Items[i].Spec.Template.Spec, pvcName) {
			addUnsupported("Job", jobs.Items[i].Name, "")
		}
	}
	cronJobs, err := client.BatchV1().CronJobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list CronJobs: %w", err)
	}
	for i := range cronJobs.Items {
		if templateUsesPVC(&cronJobs.Items[i].Spec.JobTemplate.Spec.Template.Spec, pvcName) {
			addUnsupported("CronJob", cronJobs.Items[i].Name, "")
		}
	}

	for _, dep := range deployments {
		plan.Deployments = append(plan.Deployments, dep)
	}
	for _, item := range unsupported {
		plan.Unsupported = append(plan.Unsupported, item)
	}
	sort.Strings(plan.Pods)
	sort.Slice(plan.Deployments, func(i, j int) bool { return plan.Deployments[i].Name < plan.Deployments[j].Name })
	sort.Slice(plan.Unsupported, func(i, j int) bool {
		if plan.Unsupported[i].Kind == plan.Unsupported[j].Kind {
			return plan.Unsupported[i].Name < plan.Unsupported[j].Name
		}
		return plan.Unsupported[i].Kind < plan.Unsupported[j].Kind
	})
	return plan, nil
}

func DiscoverDeploymentHPAs(ctx context.Context, client kubernetes.Interface, deps []DeploymentRef) ([]HPARef, error) {
	targetsByNamespace := map[string]map[string]struct{}{}
	for _, dep := range deps {
		if targetsByNamespace[dep.Namespace] == nil {
			targetsByNamespace[dep.Namespace] = map[string]struct{}{}
		}
		targetsByNamespace[dep.Namespace][dep.Name] = struct{}{}
	}
	var refs []HPARef
	for namespace, targets := range targetsByNamespace {
		hpas, err := client.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list HorizontalPodAutoscalers in %s: %w", namespace, err)
		}
		for i := range hpas.Items {
			hpa := &hpas.Items[i]
			ref := hpa.Spec.ScaleTargetRef
			if ref.Kind != "Deployment" || (ref.APIVersion != "" && ref.APIVersion != "apps/v1") {
				continue
			}
			if _, ok := targets[ref.Name]; ok {
				refs = append(refs, HPARef{Namespace: namespace, Name: hpa.Name, DeploymentName: ref.Name})
			}
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Namespace == refs[j].Namespace {
			return refs[i].Name < refs[j].Name
		}
		return refs[i].Namespace < refs[j].Namespace
	})
	return refs, nil
}

func ActivePodsUsingPVC(ctx context.Context, client kubernetes.Interface, namespace, pvcName string) ([]string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	var names []string
	for i := range pods.Items {
		pod := pods.Items[i]
		if podUsesPVC(&pod, pvcName) && !isTerminalPod(&pod) {
			names = append(names, pod.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func podUsesPVC(pod *corev1.Pod, pvcName string) bool { return templateUsesPVC(&pod.Spec, pvcName) }
func templateUsesPVC(spec *corev1.PodSpec, pvcName string) bool {
	for _, vol := range spec.Volumes {
		if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvcName {
			return true
		}
	}
	return false
}
func isTerminalPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func resolvePodOwner(ctx context.Context, client kubernetes.Interface, namespace string, pod *corev1.Pod) (*DeploymentRef, *UnsupportedConsumer, error) {
	if len(pod.OwnerReferences) == 0 {
		return nil, &UnsupportedConsumer{Kind: "Pod", Name: pod.Name, Pod: pod.Name}, nil
	}
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil {
		owner = &pod.OwnerReferences[0]
	}
	switch owner.Kind {
	case "ReplicaSet":
		rs, err := client.AppsV1().ReplicaSets(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("get ReplicaSet %s/%s for pod %s: %w", namespace, owner.Name, pod.Name, err)
		}
		rsOwner := controllerOwner(rs.OwnerReferences)
		if rsOwner != nil && rsOwner.Kind == "Deployment" {
			dep, err := client.AppsV1().Deployments(namespace).Get(ctx, rsOwner.Name, metav1.GetOptions{})
			if err != nil {
				return nil, nil, fmt.Errorf("get Deployment %s/%s for pod %s: %w", namespace, rsOwner.Name, pod.Name, err)
			}
			replicas := int32(1)
			if dep.Spec.Replicas != nil {
				replicas = *dep.Spec.Replicas
			}
			return &DeploymentRef{Namespace: namespace, Name: dep.Name, UID: dep.UID, Replicas: replicas}, nil, nil
		}
		return nil, &UnsupportedConsumer{Kind: "ReplicaSet", Name: owner.Name, Pod: pod.Name}, nil
	case "Deployment":
		dep, err := client.AppsV1().Deployments(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("get Deployment %s/%s for pod %s: %w", namespace, owner.Name, pod.Name, err)
		}
		replicas := int32(1)
		if dep.Spec.Replicas != nil {
			replicas = *dep.Spec.Replicas
		}
		return &DeploymentRef{Namespace: namespace, Name: dep.Name, UID: dep.UID, Replicas: replicas}, nil, nil
	default:
		return nil, &UnsupportedConsumer{Kind: owner.Kind, Name: owner.Name, Pod: pod.Name}, nil
	}
}

func controllerOwner(owners []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range owners {
		if owners[i].Controller != nil && *owners[i].Controller {
			return &owners[i]
		}
	}
	return nil
}

func DeploymentObject(ref DeploymentRef) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, UID: ref.UID}}
}
