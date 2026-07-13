package operation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
)

const (
	leaseDuration = 30 * time.Second
	leaseRenewal  = 10 * time.Second
)

func LockNameForPVC(sourceName string) string {
	return naming.SafeDNSLabel(sourceName + "-shrink-lock")
}

// LeaseLock serializes operations for a PVC. It renews a Kubernetes Lease and
// cancels Context if ownership can no longer be proven.
type LeaseLock struct {
	client    kubernetes.Interface
	namespace string
	name      string
	holder    string
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	mu        sync.Mutex
	err       error
}

func AcquireLease(ctx context.Context, client kubernetes.Interface, namespace, pvcName, holder string) (*LeaseLock, error) {
	if holder == "" {
		return nil, fmt.Errorf("lease holder identity is required")
	}
	name := LockNameForPVC(pvcName)
	now := metav1.NewMicroTime(time.Now().UTC())
	duration := int32(leaseDuration / time.Second)
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: coordinationv1.LeaseSpec{
		HolderIdentity:       &holder,
		LeaseDurationSeconds: &duration,
		AcquireTime:          &now,
		RenewTime:            &now,
	}}
	_, err := client.CoordinationV1().Leases(namespace).Create(ctx, lease, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := client.CoordinationV1().Leases(namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return nil, fmt.Errorf("inspect operation lock %s/%s: %w", namespace, name, getErr)
		}
		if !leaseExpired(existing, time.Now()) {
			owner := "unknown"
			if existing.Spec.HolderIdentity != nil {
				owner = *existing.Spec.HolderIdentity
			}
			return nil, fmt.Errorf("operation lock %s/%s is held by %s; wait for it to finish or for its Lease to expire", namespace, name, owner)
		}
		existing.Spec = lease.Spec
		_, err = client.CoordinationV1().Leases(namespace).Update(ctx, existing, metav1.UpdateOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("acquire operation lock %s/%s: %w", namespace, name, err)
	}
	lockCtx, cancel := context.WithCancel(ctx)
	lock := &LeaseLock{client: client, namespace: namespace, name: name, holder: holder, ctx: lockCtx, cancel: cancel, done: make(chan struct{})}
	go lock.renew()
	return lock, nil
}

func leaseExpired(lease *coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.LeaseDurationSeconds == nil {
		return true
	}
	base := lease.CreationTimestamp.Time
	if lease.Spec.AcquireTime != nil {
		base = lease.Spec.AcquireTime.Time
	}
	if lease.Spec.RenewTime != nil {
		base = lease.Spec.RenewTime.Time
	}
	return !base.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second).After(now)
}

func (l *LeaseLock) Context() context.Context { return l.ctx }

func (l *LeaseLock) renew() {
	defer close(l.done)
	ticker := time.NewTicker(leaseRenewal)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			// Bound each renewal attempt so this process cancels well before its
			// last successful Lease can expire and be taken over.
			renewCtx, cancel := context.WithTimeout(l.ctx, leaseRenewal)
			current, err := l.client.CoordinationV1().Leases(l.namespace).Get(renewCtx, l.name, metav1.GetOptions{})
			if err == nil && (current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != l.holder) {
				err = fmt.Errorf("operation lock ownership changed")
			}
			if err == nil {
				now := metav1.NewMicroTime(time.Now().UTC())
				current.Spec.RenewTime = &now
				_, err = l.client.CoordinationV1().Leases(l.namespace).Update(renewCtx, current, metav1.UpdateOptions{})
			}
			cancel()
			if err != nil && l.ctx.Err() != nil {
				return
			}
			if err != nil {
				l.mu.Lock()
				l.err = fmt.Errorf("renew operation lock %s/%s: %w", l.namespace, l.name, err)
				l.mu.Unlock()
				l.cancel()
				return
			}
		}
	}
}

func (l *LeaseLock) Release(ctx context.Context) error {
	l.cancel()
	<-l.done
	current, err := l.client.CoordinationV1().Leases(l.namespace).Get(ctx, l.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return l.Err()
	}
	if err != nil {
		return errors.Join(l.Err(), fmt.Errorf("inspect operation lock before release: %w", err))
	}
	if current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != l.holder {
		return errors.Join(l.Err(), fmt.Errorf("operation lock ownership changed before release"))
	}
	preconditions := &metav1.Preconditions{}
	if current.UID != "" {
		preconditions.UID = &current.UID
	}
	if current.ResourceVersion != "" {
		preconditions.ResourceVersion = &current.ResourceVersion
	}
	if err := l.client.CoordinationV1().Leases(l.namespace).Delete(ctx, l.name, metav1.DeleteOptions{Preconditions: preconditions}); err != nil && !apierrors.IsNotFound(err) {
		return errors.Join(l.Err(), fmt.Errorf("release operation lock: %w", err))
	}
	return l.Err()
}

func (l *LeaseLock) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}
