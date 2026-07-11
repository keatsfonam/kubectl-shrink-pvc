package kube

import (
	"fmt"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
)

func Clientset(configFlags *genericclioptions.ConfigFlags) (kubernetes.Interface, string, error) {
	if configFlags == nil {
		return nil, "", fmt.Errorf("config flags are required")
	}

	namespace, _, err := configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return nil, "", fmt.Errorf("resolve namespace: %w", err)
	}
	if namespace == "" {
		namespace = "default"
	}

	restConfig, err := configFlags.ToRESTConfig()
	if err != nil {
		return nil, "", fmt.Errorf("build rest config: %w", err)
	}

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, "", fmt.Errorf("build kubernetes client: %w", err)
	}

	return client, namespace, nil
}
