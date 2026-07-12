package operation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
)

const (
	AnnotationOperationID = "shrink-pvc.keats.dev/operation-id"
	AnnotationRole        = "shrink-pvc.keats.dev/role"
	RoleRecreatedSource   = "recreated-source"
	stateKey              = "state.json"
)

type Phase string

const (
	PhaseCopiedToTemp    Phase = "CopiedToTemp"
	PhaseSourceDeleted   Phase = "SourceDeleted"
	PhaseSourceRecreated Phase = "SourceRecreated"
	PhaseCopiedBack      Phase = "CopiedBack"
)

type State struct {
	Version            int                  `json:"version"`
	OperationID        string               `json:"operationID"`
	Namespace          string               `json:"namespace"`
	SourceName         string               `json:"sourceName"`
	OriginalSourceUID  types.UID            `json:"originalSourceUID"`
	TempName           string               `json:"tempName"`
	TempUID            types.UID            `json:"tempUID"`
	TargetSize         string               `json:"targetSize"`
	Image              string               `json:"image"`
	RsyncExtraArgs     string               `json:"rsyncExtraArgs,omitempty"`
	RunAsUser          int64                `json:"runAsUser"`
	FSGroup            int64                `json:"fsGroup"`
	KeepTemp           bool                 `json:"keepTemp"`
	NoScale            bool                 `json:"noScale"`
	Deployments        []kube.DeploymentRef `json:"deployments,omitempty"`
	FinalPVCJSON       []byte               `json:"finalPVCJSON"`
	RecreatedSourceUID types.UID            `json:"recreatedSourceUID,omitempty"`
	Phase              Phase                `json:"phase"`
}

func NewID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate operation ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func NameForPVC(sourceName string) string {
	return naming.SafeDNSLabel(sourceName + "-shrink-state")
}

func StampRecreatedPVC(pvc *corev1.PersistentVolumeClaim, operationID string) {
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[AnnotationOperationID] = operationID
	pvc.Annotations[AnnotationRole] = RoleRecreatedSource
}

func ValidateRecreatedPVC(pvc *corev1.PersistentVolumeClaim, operationID string) error {
	if pvc.Annotations[AnnotationOperationID] != operationID || pvc.Annotations[AnnotationRole] != RoleRecreatedSource {
		return fmt.Errorf("PVC %s/%s is not owned by operation %s", pvc.Namespace, pvc.Name, operationID)
	}
	return nil
}

type Store struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
}

func (s Store) Create(ctx context.Context, state *State) (*corev1.ConfigMap, error) {
	data, err := encode(state)
	if err != nil {
		return nil, err
	}
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, Labels: map[string]string{"app.kubernetes.io/name": "kubectl-shrink-pvc"}},
		Data:       map[string]string{stateKey: string(data)},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("operation state %s/%s already exists; rerun with --resume", s.Namespace, s.Name)
	}
	if err != nil {
		return nil, fmt.Errorf("create operation state: %w", err)
	}
	return cm, nil
}

func (s Store) Load(ctx context.Context) (*State, *corev1.ConfigMap, error) {
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, s.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("load operation state %s/%s: %w", s.Namespace, s.Name, err)
	}
	var state State
	if err := json.Unmarshal([]byte(cm.Data[stateKey]), &state); err != nil {
		return nil, nil, fmt.Errorf("decode operation state: %w", err)
	}
	if state.Version != 1 || state.Namespace != s.Namespace || state.SourceName == "" || state.OperationID == "" {
		return nil, nil, fmt.Errorf("operation state %s/%s is invalid or unsupported", s.Namespace, s.Name)
	}
	return &state, cm, nil
}

func (s Store) Update(ctx context.Context, state *State, resourceVersion string) (string, error) {
	data, err := encode(state)
	if err != nil {
		return "", err
	}
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Update(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, ResourceVersion: resourceVersion},
		Data:       map[string]string{stateKey: string(data)},
	}, metav1.UpdateOptions{})
	if err != nil {
		return "", fmt.Errorf("update operation state to %s: %w", state.Phase, err)
	}
	return cm.ResourceVersion, nil
}

func (s Store) Delete(ctx context.Context, uid types.UID) error {
	if uid == "" {
		return fmt.Errorf("delete operation state: UID is required")
	}
	err := s.Client.CoreV1().ConfigMaps(s.Namespace).Delete(ctx, s.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete operation state: %w", err)
	}
	return nil
}

func encode(state *State) ([]byte, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode operation state: %w", err)
	}
	if len(data) > 900*1024 {
		return nil, fmt.Errorf("operation state is too large: %d bytes", len(data))
	}
	return data, nil
}
