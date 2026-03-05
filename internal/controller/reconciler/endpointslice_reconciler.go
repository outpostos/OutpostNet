package reconciler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/outpostos/OutpostNet/pkg/annotations"
)

const managedByLabel = "app.kubernetes.io/managed-by"
const managedByValue = "outpostnet-controller"

// EndpointSliceReconciler watches OutpostNet-injected Pods and manages
// headless Services + EndpointSlices for exported services.
type EndpointSliceReconciler struct {
	client.Client
}

// Reconcile handles Pod events to maintain Services and EndpointSlices.
func (r *EndpointSliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.NamespacedName)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if errors.IsNotFound(err) {
			// Pod deleted — Services/EndpointSlices with OwnerRef are GC'd.
			// Cross-pod aggregated Services need explicit cleanup, handled below.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Only handle injected pods.
	if pod.Labels == nil || pod.Labels[annotations.InjectedLabel] != "true" {
		return ctrl.Result{}, nil
	}

	// Parse exports.
	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		logger.Error(err, "failed to parse annotations")
		return ctrl.Result{}, nil
	}

	if len(cfg.Exports) == 0 {
		return ctrl.Result{}, nil
	}

	// Pod must have an IP to register endpoints.
	if pod.Status.PodIP == "" {
		return ctrl.Result{Requeue: true}, nil
	}

	// For each exported service, ensure headless Service + EndpointSlice.
	for _, export := range cfg.Exports {
		if err := r.ensureService(ctx, pod, export); err != nil {
			logger.Error(err, "failed to ensure service", "service", export.Name)
			return ctrl.Result{}, err
		}
		if err := r.ensureEndpointSlice(ctx, pod, export); err != nil {
			logger.Error(err, "failed to ensure endpointslice", "service", export.Name)
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *EndpointSliceReconciler) ensureService(ctx context.Context, pod *corev1.Pod, export annotations.ServiceEntry) error {
	svcName := export.Name
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: svcName, Namespace: pod.Namespace}, svc)

	if errors.IsNotFound(err) {
		proto := corev1.ProtocolTCP
		if export.Protocol == "udp" {
			proto = corev1.ProtocolUDP
		}

		svc = &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: pod.Namespace,
				Labels: map[string]string{
					managedByLabel:    managedByValue,
					"outpostnet.io/service": export.Name,
				},
			},
			Spec: corev1.ServiceSpec{
				Type:      corev1.ServiceTypeClusterIP,
				ClusterIP: "None", // Headless service.
				Ports: []corev1.ServicePort{
					{
						Name:       export.Protocol,
						Port:       int32(export.Port),
						TargetPort: intstr.FromInt32(int32(export.Port)),
						Protocol:   proto,
					},
				},
			},
		}

		if err := r.Create(ctx, svc); err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("create service %s: %w", svcName, err)
		}
		log.FromContext(ctx).Info("created headless service", "service", svcName)
		return nil
	}

	return err
}

func (r *EndpointSliceReconciler) ensureEndpointSlice(ctx context.Context, pod *corev1.Pod, export annotations.ServiceEntry) error {
	svcName := export.Name
	sliceName := fmt.Sprintf("%s-%s", svcName, pod.Name)

	proto := corev1.ProtocolTCP
	if export.Protocol == "udp" {
		proto = corev1.ProtocolUDP
	}
	addrType := discoveryv1.AddressTypeIPv4
	port := int32(export.Port)
	ready := true

	slice := &discoveryv1.EndpointSlice{}
	err := r.Get(ctx, types.NamespacedName{Name: sliceName, Namespace: pod.Namespace}, slice)

	if errors.IsNotFound(err) {
		slice = &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sliceName,
				Namespace: pod.Namespace,
				Labels: map[string]string{
					managedByLabel:                           managedByValue,
					discoveryv1.LabelServiceName:             svcName,
					"outpostnet.io/pod":                      pod.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(pod, corev1.SchemeGroupVersion.WithKind("Pod")),
				},
			},
			AddressType: addrType,
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses: []string{pod.Status.PodIP},
					Conditions: discoveryv1.EndpointConditions{
						Ready: &ready,
					},
					TargetRef: &corev1.ObjectReference{
						Kind:      "Pod",
						Name:      pod.Name,
						Namespace: pod.Namespace,
						UID:       pod.UID,
					},
				},
			},
			Ports: []discoveryv1.EndpointPort{
				{
					Name:     &export.Protocol,
					Port:     &port,
					Protocol: &proto,
				},
			},
		}

		if err := r.Create(ctx, slice); err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("create endpointslice %s: %w", sliceName, err)
		}
		log.FromContext(ctx).Info("created endpointslice", "endpointslice", sliceName, "podIP", pod.Status.PodIP)
		return nil
	}

	if err != nil {
		return err
	}

	// Update endpoint if IP changed.
	needsUpdate := false
	if len(slice.Endpoints) == 0 || slice.Endpoints[0].Addresses[0] != pod.Status.PodIP {
		slice.Endpoints = []discoveryv1.Endpoint{
			{
				Addresses: []string{pod.Status.PodIP},
				Conditions: discoveryv1.EndpointConditions{
					Ready: &ready,
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Name:      pod.Name,
					Namespace: pod.Namespace,
					UID:       pod.UID,
				},
			},
		}
		needsUpdate = true
	}

	if needsUpdate {
		if err := r.Update(ctx, slice); err != nil {
			return fmt.Errorf("update endpointslice %s: %w", sliceName, err)
		}
		log.FromContext(ctx).Info("updated endpointslice", "endpointslice", sliceName)
	}

	return nil
}

// SetupWithManager sets up the reconciler with the controller manager.
func (r *EndpointSliceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Owns(&discoveryv1.EndpointSlice{}).
		Complete(r)
}
