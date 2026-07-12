package podsec

import (
	corev1 "k8s.io/api/core/v1"
)

// Pod returns the pod-level security context. runAsUser and fsGroup below
// zero mean "run as the image default", which is root for the stock inspect
// and copy images.
func Pod(runAsUser, fsGroup int64) *corev1.PodSecurityContext {
	sc := &corev1.PodSecurityContext{
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if runAsUser >= 0 {
		nonRoot := true
		sc.RunAsNonRoot = &nonRoot
		sc.RunAsUser = &runAsUser
	}
	if fsGroup >= 0 {
		sc.FSGroup = &fsGroup
	}
	return sc
}

// Container returns the container-level security context. In root mode the
// listed capabilities are added back after dropping everything; in non-root
// mode nothing is added so the pod satisfies the restricted PodSecurity
// profile.
func Container(nonRoot bool, caps ...corev1.Capability) *corev1.SecurityContext {
	noEscalation := false
	sc := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &noEscalation,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
	if !nonRoot {
		sc.Capabilities.Add = caps
	}
	return sc
}
