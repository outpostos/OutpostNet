package reconciler

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/outpostos/OutpostNet/pkg/annotations"
)

// newEndpointSliceScheme creates a runtime.Scheme with core and discovery types registered.
func newEndpointSliceScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = discoveryv1.AddToScheme(s)
	return s
}

func TestEndpointSliceReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newEndpointSliceScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &EndpointSliceReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonexistent-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}
}

func TestEndpointSliceReconciler_Reconcile_NotInjected(t *testing.T) {
	scheme := newEndpointSliceScheme()

	// Pod exists but does not have the injected label.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plain-pod",
			Namespace: "default",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &EndpointSliceReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "plain-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify no Service was created.
	svc := &corev1.Service{}
	svcErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "web",
		Namespace: "default",
	}, svc)
	if svcErr == nil {
		t.Error("expected service to not exist, but it was found")
	}
}

func TestEndpointSliceReconciler_Reconcile_NoPodIP(t *testing.T) {
	scheme := newEndpointSliceScheme()

	// Injected pod with exports but no PodIP — should requeue.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
			UID: "uid-123",
		},
		Status: corev1.PodStatus{
			PodIP: "", // No IP assigned yet.
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &EndpointSliceReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "web-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !result.Requeue {
		t.Error("expected requeue when PodIP is empty")
	}

	// Verify no Service was created.
	svc := &corev1.Service{}
	svcErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "web",
		Namespace: "default",
	}, svc)
	if svcErr == nil {
		t.Error("expected service to not exist, but it was found")
	}
}

func TestEndpointSliceReconciler_Reconcile_NoExports(t *testing.T) {
	scheme := newEndpointSliceScheme()

	// Injected pod with imports only (no exports) — should be a no-op.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "consumer-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ImportsAnnotation: "db:3306:tcp",
			},
			UID: "uid-456",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.5",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &EndpointSliceReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "consumer-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue for pod with no exports")
	}
}

func TestEndpointSliceReconciler_Reconcile_CreatesServiceAndSlice(t *testing.T) {
	scheme := newEndpointSliceScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
			UID: "uid-789",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.10",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &EndpointSliceReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "web-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify headless Service was created.
	svc := &corev1.Service{}
	svcErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "web",
		Namespace: "default",
	}, svc)
	if svcErr != nil {
		t.Fatalf("expected service to exist, got error: %v", svcErr)
	}
	if svc.Spec.ClusterIP != "None" {
		t.Errorf("expected headless service (ClusterIP=None), got %q", svc.Spec.ClusterIP)
	}
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("expected 1 port, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("expected port 8080, got %d", svc.Spec.Ports[0].Port)
	}
	if svc.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Errorf("expected TCP protocol, got %v", svc.Spec.Ports[0].Protocol)
	}
	if svc.Labels[managedByLabel] != managedByValue {
		t.Errorf("expected managed-by label on service, got %q", svc.Labels[managedByLabel])
	}

	// Verify EndpointSlice was created.
	slice := &discoveryv1.EndpointSlice{}
	sliceErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "web-web-pod",
		Namespace: "default",
	}, slice)
	if sliceErr != nil {
		t.Fatalf("expected endpointslice to exist, got error: %v", sliceErr)
	}
	if slice.AddressType != discoveryv1.AddressTypeIPv4 {
		t.Errorf("expected AddressTypeIPv4, got %v", slice.AddressType)
	}
	if len(slice.Endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(slice.Endpoints))
	}
	if len(slice.Endpoints[0].Addresses) != 1 || slice.Endpoints[0].Addresses[0] != "10.0.0.10" {
		t.Errorf("expected endpoint address 10.0.0.10, got %v", slice.Endpoints[0].Addresses)
	}
	if slice.Endpoints[0].Conditions.Ready == nil || !*slice.Endpoints[0].Conditions.Ready {
		t.Error("expected endpoint condition Ready=true")
	}

	// Verify ports on EndpointSlice.
	if len(slice.Ports) != 1 {
		t.Fatalf("expected 1 port on endpointslice, got %d", len(slice.Ports))
	}
	if slice.Ports[0].Port == nil || *slice.Ports[0].Port != 8080 {
		t.Errorf("expected port 8080 on endpointslice")
	}
	if slice.Ports[0].Protocol == nil || *slice.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Error("expected TCP protocol on endpointslice port")
	}

	// Verify labels on EndpointSlice.
	if slice.Labels[discoveryv1.LabelServiceName] != "web" {
		t.Errorf("expected service name label 'web', got %q", slice.Labels[discoveryv1.LabelServiceName])
	}
	if slice.Labels["outpostnet.io/pod"] != "web-pod" {
		t.Errorf("expected pod label 'web-pod', got %q", slice.Labels["outpostnet.io/pod"])
	}

	// Verify owner reference on EndpointSlice.
	if len(slice.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(slice.OwnerReferences))
	}
	if slice.OwnerReferences[0].Name != "web-pod" {
		t.Errorf("expected owner ref name 'web-pod', got %q", slice.OwnerReferences[0].Name)
	}
}

func TestEndpointSliceReconciler_Reconcile_MultipleExports(t *testing.T) {
	scheme := newEndpointSliceScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp,api:9090:tcp",
			},
			UID: "uid-multi",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.20",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &EndpointSliceReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "multi-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify both Services were created.
	for _, svcName := range []string{"web", "api"} {
		svc := &corev1.Service{}
		svcErr := fakeClient.Get(context.Background(), types.NamespacedName{
			Name:      svcName,
			Namespace: "default",
		}, svc)
		if svcErr != nil {
			t.Errorf("expected service %q to exist, got error: %v", svcName, svcErr)
		}
	}

	// Verify both EndpointSlices were created.
	for _, sliceName := range []string{"web-multi-pod", "api-multi-pod"} {
		slice := &discoveryv1.EndpointSlice{}
		sliceErr := fakeClient.Get(context.Background(), types.NamespacedName{
			Name:      sliceName,
			Namespace: "default",
		}, slice)
		if sliceErr != nil {
			t.Errorf("expected endpointslice %q to exist, got error: %v", sliceName, sliceErr)
		}
	}
}

func TestEndpointSliceReconciler_Reconcile_UDPExport(t *testing.T) {
	scheme := newEndpointSliceScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "udp-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "dns:53:udp",
			},
			UID: "uid-udp",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.30",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &EndpointSliceReconciler{Client: fakeClient}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "udp-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify Service has UDP protocol.
	svc := &corev1.Service{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: "dns", Namespace: "default",
	}, svc); err != nil {
		t.Fatalf("expected service to exist: %v", err)
	}
	if svc.Spec.Ports[0].Protocol != corev1.ProtocolUDP {
		t.Errorf("expected UDP protocol on service, got %v", svc.Spec.Ports[0].Protocol)
	}

	// Verify EndpointSlice has UDP protocol.
	slice := &discoveryv1.EndpointSlice{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: "dns-udp-pod", Namespace: "default",
	}, slice); err != nil {
		t.Fatalf("expected endpointslice to exist: %v", err)
	}
	if slice.Ports[0].Protocol == nil || *slice.Ports[0].Protocol != corev1.ProtocolUDP {
		t.Error("expected UDP protocol on endpointslice port")
	}
}
