package csidrivercontroller

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/resource/resourcemerge"
	"github.com/openshift/library-go/pkg/operator/resource/resourceread"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

const (
	daemonSet          = "node.yaml"
	deployment         = "controller.yaml"
	credentialsRequest = "credentials.yaml"
	specHashAnnotation = "operator.openshift.io/spec-hash"
)

func (c *Controller) syncCredentialsRequest(status *operatorv1.OperatorStatus) (*unstructured.Unstructured, error) {
	cr := readCredentialRequestsOrDie(c.config.CredentialsManifest)

	// Set spec.secretRef.namespace
	err := unstructured.SetNestedField(cr.Object, c.config.OperandNamespace, "spec", "secretRef", "namespace")
	if err != nil {
		return nil, err
	}

	forceRollout := false
	if c.versionChanged("operator", c.operatorVersion) {
		// Operator version changed. The new one _may_ have updated Deployment -> we should deploy it.
		forceRollout = true
	}

	var expectedGeneration int64 = -1
	generation := resourcemerge.GenerationFor(
		status.Generations,
		schema.GroupResource{Group: credentialsRequestGroup, Resource: credentialsRequestResource},
		cr.GetNamespace(),
		cr.GetName())
	if generation != nil {
		expectedGeneration = generation.LastGeneration
	}

	cr, _, err = applyCredentialsRequest(c.dynamicClient, c.eventRecorder, cr, expectedGeneration, forceRollout)
	return cr, err
}

func (c *Controller) syncDeployment(spec *operatorv1.OperatorSpec, status *operatorv1.OperatorStatus) (*appsv1.Deployment, error) {
	deploy := c.getExpectedDeployment(spec)

	deploy, _, err := resourceapply.ApplyDeployment(
		c.kubeClient.AppsV1(),
		c.eventRecorder,
		deploy,
		resourcemerge.ExpectedDeploymentGeneration(deploy, status.Generations))
	if err != nil {
		return nil, err
	}

	return deploy, nil
}

func (c *Controller) syncDaemonSet(spec *operatorv1.OperatorSpec, status *operatorv1.OperatorStatus) (*appsv1.DaemonSet, error) {
	daemonSet := c.getExpectedDaemonSet(spec)

	daemonSet, _, err := resourceapply.ApplyDaemonSet(
		c.kubeClient.AppsV1(),
		c.eventRecorder,
		daemonSet,
		resourcemerge.ExpectedDaemonSetGeneration(daemonSet, status.Generations))
	if err != nil {
		return nil, err
	}

	return daemonSet, nil
}

func (c *Controller) getExpectedDeployment(spec *operatorv1.OperatorSpec) *appsv1.Deployment {
	deployment := resourceread.ReadDeploymentV1OrDie(c.config.ControllerManifest)

	if c.images.csiDriver != "" {
		deployment.Spec.Template.Spec.Containers[0].Image = c.images.csiDriver
	}
	if c.images.provisioner != "" {
		deployment.Spec.Template.Spec.Containers[provisionerContainerIndex].Image = c.images.provisioner
	}
	if c.images.attacher != "" {
		deployment.Spec.Template.Spec.Containers[attacherContainerIndex].Image = c.images.attacher
	}
	if c.images.resizer != "" {
		deployment.Spec.Template.Spec.Containers[resizerContainerIndex].Image = c.images.resizer
	}
	if c.images.snapshotter != "" {
		deployment.Spec.Template.Spec.Containers[snapshottterContainerIndex].Image = c.images.snapshotter
	}

	// TODO: add LivenessProbe when

	logLevel := getLogLevel(spec.LogLevel)
	for i, container := range deployment.Spec.Template.Spec.Containers {
		for j, arg := range container.Args {
			if strings.HasPrefix(arg, "--v=") {
				deployment.Spec.Template.Spec.Containers[i].Args[j] = fmt.Sprintf("--v=%d", logLevel)
			}
		}
	}

	return deployment
}

func (c *Controller) getExpectedDaemonSet(spec *operatorv1.OperatorSpec) *appsv1.DaemonSet {
	daemonSet := resourceread.ReadDaemonSetV1OrDie(c.config.NodeManifest)

	if c.images.csiDriver != "" {
		daemonSet.Spec.Template.Spec.Containers[csiDriverContainerIndex].Image = c.images.csiDriver
	}
	if c.images.nodeDriverRegistrar != "" {
		daemonSet.Spec.Template.Spec.Containers[nodeDriverRegistrarContainerIndex].Image = c.images.nodeDriverRegistrar
	}
	if c.images.livenessProbe != "" {
		daemonSet.Spec.Template.Spec.Containers[livenessProbeContainerIndex].Image = c.images.livenessProbe
	}

	logLevel := getLogLevel(spec.LogLevel)
	for i, container := range daemonSet.Spec.Template.Spec.Containers {
		for j, arg := range container.Args {
			if strings.HasPrefix(arg, "--v=") {
				daemonSet.Spec.Template.Spec.Containers[i].Args[j] = fmt.Sprintf("--v=%d", logLevel)
			}
		}
	}

	return daemonSet
}

func (c *Controller) syncStatus(meta *metav1.ObjectMeta, status *operatorv1.OperatorStatus, deployment *appsv1.Deployment,
	daemonSet *appsv1.DaemonSet, credentialsRequest *unstructured.Unstructured) error {
	c.syncConditions(status, deployment, daemonSet)

	resourcemerge.SetDeploymentGeneration(&status.Generations, deployment)
	resourcemerge.SetDaemonSetGeneration(&status.Generations, daemonSet)
	if credentialsRequest != nil {
		resourcemerge.SetGeneration(&status.Generations, operatorv1.GenerationStatus{
			Group:          credentialsRequestGroup,
			Resource:       credentialsRequestResource,
			Namespace:      credentialsRequest.GetNamespace(),
			Name:           credentialsRequest.GetName(),
			LastGeneration: credentialsRequest.GetGeneration(),
		})
	}

	status.ObservedGeneration = meta.Generation

	// TODO: what should be the number of replicas? Right now the formula is:
	if deployment != nil && daemonSet != nil {
		if deployment.Status.UnavailableReplicas == 0 && daemonSet.Status.NumberUnavailable == 0 {
			status.ReadyReplicas = deployment.Status.UpdatedReplicas + daemonSet.Status.UpdatedNumberScheduled
		}
	}

	c.setVersion("operator", c.operatorVersion)
	c.setVersion(c.config.OperandName, c.operandVersion)

	return nil
}

func (c *Controller) syncConditions(status *operatorv1.OperatorStatus, deployment *appsv1.Deployment, daemonSet *appsv1.DaemonSet) {
	// The operator does not have any prerequisites (at least now)
	v1helpers.SetOperatorCondition(&status.Conditions,
		operatorv1.OperatorCondition{
			Type:   operatorv1.OperatorStatusTypePrereqsSatisfied,
			Status: operatorv1.ConditionTrue,
		})
	// The operator is always upgradeable (at least now)
	v1helpers.SetOperatorCondition(&status.Conditions,
		operatorv1.OperatorCondition{
			Type:   operatorv1.OperatorStatusTypeUpgradeable,
			Status: operatorv1.ConditionTrue,
		})
	c.syncProgressingCondition(status, deployment, daemonSet)
	c.syncAvailableCondition(status, deployment, daemonSet)
}

func (c *Controller) syncAvailableCondition(status *operatorv1.OperatorStatus, deployment *appsv1.Deployment, daemonSet *appsv1.DaemonSet) {
	// TODO: is it enough to check if these values are >0? Or should be more strict and check against the exact desired value?
	isDeploymentAvailable := deployment != nil && deployment.Status.AvailableReplicas > 0
	isDaemonSetAvailable := daemonSet != nil && daemonSet.Status.NumberAvailable > 0
	if isDeploymentAvailable && isDaemonSetAvailable {
		v1helpers.SetOperatorCondition(&status.Conditions,
			operatorv1.OperatorCondition{
				Type:   operatorv1.OperatorStatusTypeAvailable,
				Status: operatorv1.ConditionTrue,
			})
	} else {
		v1helpers.SetOperatorCondition(&status.Conditions,
			operatorv1.OperatorCondition{
				Type:    operatorv1.OperatorStatusTypeAvailable,
				Status:  operatorv1.ConditionFalse,
				Message: "Waiting for Deployment and DaemonSet to deploy aws-ebs-csi-driver pods",
				Reason:  "AsExpected",
			})
	}
}

func (c *Controller) syncProgressingCondition(status *operatorv1.OperatorStatus, deployment *appsv1.Deployment, daemonSet *appsv1.DaemonSet) {
	// Progressing: true when Deployment or DaemonSet have some work to do
	// (false: when all replicas are updated to the latest release and available)/
	var progressing operatorv1.ConditionStatus
	var progressingMessage string
	var deploymentExpectedReplicas int32
	if deployment != nil && deployment.Spec.Replicas != nil {
		deploymentExpectedReplicas = *deployment.Spec.Replicas
	}
	switch {
	// Controller
	case deployment == nil:
		// Not reachable in theory, but better to be on the safe side...
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for Deployment to be created"

	case deployment.Generation != deployment.Status.ObservedGeneration:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for Deployment to act on changes"

	case deployment.Status.UnavailableReplicas > 0:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for Deployment to deploy controller pods"

	case deployment.Status.UpdatedReplicas < deploymentExpectedReplicas:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for Deployment to update pods"

	case deployment.Status.AvailableReplicas < deploymentExpectedReplicas:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for Deployment to deploy pods"
	// Node
	case daemonSet == nil:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for DaemonSet to be created"

	case daemonSet.Generation != daemonSet.Status.ObservedGeneration:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for DaemonSet to act on changes"

	case daemonSet.Status.NumberUnavailable > 0:
		progressing = operatorv1.ConditionTrue
		progressingMessage = "Waiting for DaemonSet to deploy node pods"

	// TODO: the following seems redundant. Remove if that's not the case.

	// case daemonSet.Status.UpdatedNumberScheduled < daemonSet.Status.DesiredNumberScheduled:
	// 	progressing = operatorv1.ConditionTrue
	// 	progressingMessage = "Waiting for DaemonSet to update pods"

	// case daemonSet.Status.NumberAvailable < 1:
	// 	progressing = operatorv1.ConditionTrue
	// 	progressingMessage = "Waiting for DaemonSet to deploy pods"

	default:
		progressing = operatorv1.ConditionFalse
	}
	v1helpers.SetOperatorCondition(&status.Conditions,
		operatorv1.OperatorCondition{
			Type:    operatorv1.OperatorStatusTypeProgressing,
			Status:  progressing,
			Message: progressingMessage,
			Reason:  "AsExpected",
		})
}

func getLogLevel(logLevel operatorv1.LogLevel) int {
	switch logLevel {
	case operatorv1.Normal, "":
		return 2
	case operatorv1.Debug:
		return 4
	case operatorv1.Trace:
		return 6
	case operatorv1.TraceAll:
		return 100
	default:
		return 2
	}
}
