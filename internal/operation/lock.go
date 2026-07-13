package operation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	renewal   time.Duration
}

func AcquireLease(ctx context.Context, client kubernetes.Interface, namespace, pvcName, holder string) (*LeaseLock, error) {
	return acquireLease(ctx, client, namespace, pvcName, holder, leaseDuration, leaseRenewal)
}

// acquireLease uses the same safety rule as client-go leader election: expiry
// is measured from how long this client has observed an unchanged Lease record,
// never by comparing a remote client's timestamps with the local wall clock.
func acquireLease(ctx context.Context, client kubernetes.Interface, namespace, pvcName, holder string, duration, renewal time.Duration) (*LeaseLock, error) {
	if holder == "" {
		return nil, fmt.Errorf("lease holder identity is required")
	}
	name := LockNameForPVC(pvcName)
	if duration <= 0 || renewal <= 0 {
		return nil, fmt.Errorf("lease duration and renewal must be positive")
	}
	poll := duration / 10
	if poll > time.Second {
		poll = time.Second
	}
	if poll < time.Millisecond {
		poll = time.Millisecond
	}
	type observedRecord struct {
		spec            coordinationv1.LeaseSpec
		resourceVersion string
	}
	var observed *observedRecord
	var observedAt time.Time
	for {
		now := metav1.NewMicroTime(time.Now().UTC())
		seconds := int32(duration / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		desired := coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &seconds, AcquireTime: &now, RenewTime: &now}
		_, err := client.CoordinationV1().Leases(namespace).Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: desired,
		}, metav1.CreateOptions{})
		if err == nil {
			return startLeaseLock(ctx, client, namespace, name, holder, renewal), nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("acquire operation lock %s/%s: %w", namespace, name, err)
		}

		existing, err := client.CoordinationV1().Leases(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("inspect operation lock %s/%s: %w", namespace, name, err)
		}
		if observed == nil || observed.resourceVersion != existing.ResourceVersion || !reflect.DeepEqual(observed.spec, existing.Spec) {
			observed = &observedRecord{spec: *existing.Spec.DeepCopy(), resourceVersion: existing.ResourceVersion}
			observedAt = time.Now()
		}
		if time.Since(observedAt) >= duration {
			existing.Spec = desired
			if _, err = client.CoordinationV1().Leases(namespace).Update(ctx, existing, metav1.UpdateOptions{}); err == nil {
				return startLeaseLock(ctx, client, namespace, name, holder, renewal), nil
			}
			if !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("take over operation lock %s/%s: %w", namespace, name, err)
			}
			observed = nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			owner := "unknown"
			if existing.Spec.HolderIdentity != nil {
				owner = *existing.Spec.HolderIdentity
			}
			return nil, fmt.Errorf("operation lock %s/%s is held by %s: %w", namespace, name, owner, ctx.Err())
		case <-timer.C:
		}
	}
}

func startLeaseLock(ctx context.Context, client kubernetes.Interface, namespace, name, holder string, renewal time.Duration) *LeaseLock {
	lockCtx, cancel := context.WithCancel(ctx)
	lock := &LeaseLock{client: client, namespace: namespace, name: name, holder: holder, ctx: lockCtx, cancel: cancel, done: make(chan struct{}), renewal: renewal}
	go lock.renew()
	return lock
}

func (l *LeaseLock) Context() context.Context { return l.ctx }

func (l *LeaseLock) renew() {
	defer close(l.done)
	ticker := time.NewTicker(l.renewal)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(l.ctx, l.renewal)
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
