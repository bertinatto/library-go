package csidrivercontrollerservicecontroller

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	coreinformers "k8s.io/client-go/informers"
	fakecore "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"

	"github.com/google/go-cmp/cmp"
	configv1 "github.com/openshift/api/config/v1"
	opv1 "github.com/openshift/api/operator/v1"
	fakeconfig "github.com/openshift/client-go/config/clientset/versioned/fake"
	configinformers "github.com/openshift/client-go/config/informers/externalversions"

	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/resource/resourceread"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

const (
	controllerName   = "TestCSIDriverControllerServiceController"
	deploymentName   = "test-csi-driver-controller"
	operandName      = "test-csi-driver"
	operandNamespace = "openshift-test-csi-driver"

	csiDriverContainerName     = "csi-driver"
	provisionerContainerName   = "csi-provisioner"
	attacherContainerName      = "csi-attacher"
	resizerContainerName       = "csi-resizer"
	snapshotterContainerName   = "csi-snapshotter"
	livenessProbeContainerName = "csi-liveness-probe"
	kubeRBACProxyContainerName = "provisioner-kube-rbac-proxy"

	// From github.com/openshift/library-go/pkg/operator/resource/resourceapply/apps.go
	specHashAnnotation = "operator.openshift.io/spec-hash"
	defaultClusterID   = "ID1234"

	hookDeploymentAnnKey = "operator.openshift.io/foo"
	hookDeploymentAnnVal = "bar"
)

var (
	conditionAvailable   = controllerName + opv1.OperatorStatusTypeAvailable
	conditionProgressing = controllerName + opv1.OperatorStatusTypeProgressing
)

type images struct {
	csiDriver     string
	attacher      string
	provisioner   string
	resizer       string
	snapshotter   string
	livenessProbe string
	kubeRBACProxy string
}

type testCase struct {
	name            string
	images          images
	initialObjects  testObjects
	expectedObjects testObjects
	expectErr       bool
}

type testObjects struct {
	deployment *appsv1.Deployment
	driver     *fakeDriverInstance
}

type testContext struct {
	controller     factory.Controller
	operatorClient v1helpers.OperatorClient
	coreClient     *fakecore.Clientset
	coreInformers  coreinformers.SharedInformerFactory
}

func newTestContext(test testCase, t *testing.T) *testContext {
	// Add deployment to informer
	var initialObjects []runtime.Object
	if test.initialObjects.deployment != nil {
		resourceapply.SetSpecHashAnnotation(&test.initialObjects.deployment.ObjectMeta, test.initialObjects.deployment.Spec)
		initialObjects = append(initialObjects, test.initialObjects.deployment)
	}

	coreClient := fakecore.NewSimpleClientset(initialObjects...)
	coreInformerFactory := coreinformers.NewSharedInformerFactory(coreClient, 0 /*no resync */)

	// Fill the informer
	if test.initialObjects.deployment != nil {
		coreInformerFactory.Apps().V1().Deployments().Informer().GetIndexer().Add(test.initialObjects.deployment)
	}

	// Add global reactors
	addGenerationReactor(coreClient)

	// Add a fake Infrastructure object to informer. This is not
	// optional because it is always present in the cluster.
	initialInfras := []runtime.Object{makeInfra()}
	configClient := fakeconfig.NewSimpleClientset(initialInfras...)
	configInformerFactory := configinformers.NewSharedInformerFactory(configClient, 0)
	configInformerFactory.Config().V1().Infrastructures().Informer().GetIndexer().Add(initialInfras[0])

	// fakeDriverInstance also fulfils the OperatorClient interface
	fakeOperatorClient := v1helpers.NewFakeOperatorClient(
		&test.initialObjects.driver.Spec,
		&test.initialObjects.driver.Status,
		nil, /*triggerErr func*/
	)
	controller := NewCSIDriverControllerServiceController(
		controllerName,
		makeFakeManifest(),
		fakeOperatorClient,
		coreClient,
		coreInformerFactory.Apps().V1().Deployments(),
		configInformerFactory,
		events.NewInMemoryRecorder(operandName),
	)

	// Pretend env vars are set
	// TODO: inject these in New() instead
	os.Setenv(driverImageEnvName, test.images.csiDriver)
	os.Setenv(provisionerImageEnvName, test.images.provisioner)
	os.Setenv(attacherImageEnvName, test.images.attacher)
	os.Setenv(snapshotterImageEnvName, test.images.snapshotter)
	os.Setenv(resizerImageEnvName, test.images.resizer)
	os.Setenv(livenessProbeImageEnvName, test.images.livenessProbe)
	os.Setenv(kubeRBACProxyImageEnvName, test.images.kubeRBACProxy)

	return &testContext{
		controller:     controller,
		operatorClient: fakeOperatorClient,
		coreClient:     coreClient,
		coreInformers:  coreInformerFactory,
	}
}

// Drivers

type driverModifier func(*fakeDriverInstance) *fakeDriverInstance

func makeFakeDriverInstance(modifiers ...driverModifier) *fakeDriverInstance {
	instance := &fakeDriverInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cluster",
			Generation: 0,
		},
		Spec: opv1.OperatorSpec{
			ManagementState: opv1.Managed,
		},
		Status: opv1.OperatorStatus{},
	}
	for _, modifier := range modifiers {
		instance = modifier(instance)
	}
	return instance
}

func withLogLevel(logLevel opv1.LogLevel) driverModifier {
	return func(i *fakeDriverInstance) *fakeDriverInstance {
		i.Spec.LogLevel = logLevel
		return i
	}
}

func withGeneration(generations ...int64) driverModifier {
	return func(i *fakeDriverInstance) *fakeDriverInstance {
		i.Generation = generations[0]
		if len(generations) > 1 {
			i.Status.ObservedGeneration = generations[1]
		}
		return i
	}
}

func withGenerations(deployment int64) driverModifier {
	return func(i *fakeDriverInstance) *fakeDriverInstance {
		i.Status.Generations = []opv1.GenerationStatus{
			{
				Group:          appsv1.GroupName,
				LastGeneration: deployment,
				Name:           deploymentName,
				Namespace:      operandNamespace,
				Resource:       "deployments",
			},
		}
		return i
	}
}

func withTrueConditions(conditions ...string) driverModifier {
	return func(i *fakeDriverInstance) *fakeDriverInstance {
		if i.Status.Conditions == nil {
			i.Status.Conditions = []opv1.OperatorCondition{}
		}
		for _, cond := range conditions {
			i.Status.Conditions = append(i.Status.Conditions, opv1.OperatorCondition{
				Type:   cond,
				Status: opv1.ConditionTrue,
			})
		}
		return i
	}
}

func withFalseConditions(conditions ...string) driverModifier {
	return func(i *fakeDriverInstance) *fakeDriverInstance {
		if i.Status.Conditions == nil {
			i.Status.Conditions = []opv1.OperatorCondition{}
		}
		for _, c := range conditions {
			i.Status.Conditions = append(i.Status.Conditions, opv1.OperatorCondition{
				Type:   c,
				Status: opv1.ConditionFalse,
			})
		}
		return i
	}
}

func getIndex(containers []v1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}

// Deployments

type deploymentModifier func(*appsv1.Deployment) *appsv1.Deployment

func makeDeployment(clusterID string, logLevel int, images images, modifiers ...deploymentModifier) *appsv1.Deployment {
	manifest := makeFakeManifest()
	dep := resourceread.ReadDeploymentV1OrDie(manifest)

	// Replace the placeholders in the manifest (, ${DRIVER_IMAGE}, ${LOG_LEVEL})
	containers := dep.Spec.Template.Spec.Containers
	if images.csiDriver != "" {
		if idx := getIndex(containers, csiDriverContainerName); idx > -1 {
			containers[idx].Image = images.csiDriver
			for j, arg := range containers[idx].Args {
				if strings.HasPrefix(arg, "--k8s-tag-cluster-id=") {
					dep.Spec.Template.Spec.Containers[idx].Args[j] = fmt.Sprintf("--k8s-tag-cluster-id=%s", clusterID)
				}
			}
		}
	}

	if images.provisioner != "" {
		if idx := getIndex(containers, provisionerContainerName); idx > -1 {
			containers[idx].Image = images.provisioner
		}
	}

	if images.attacher != "" {
		if idx := getIndex(containers, attacherContainerName); idx > -1 {
			containers[idx].Image = images.attacher
		}
	}

	if images.resizer != "" {
		if idx := getIndex(containers, resizerContainerName); idx > -1 {
			containers[idx].Image = images.resizer
		}
	}

	if images.snapshotter != "" {
		if idx := getIndex(containers, snapshotterContainerName); idx > -1 {
			containers[idx].Image = images.snapshotter
		}
	}

	if images.livenessProbe != "" {
		if idx := getIndex(containers, livenessProbeContainerName); idx > -1 {
			containers[idx].Image = images.livenessProbe
		}
	}

	if images.kubeRBACProxy != "" {
		if idx := getIndex(containers, kubeRBACProxyContainerName); idx > -1 {
			containers[idx].Image = images.kubeRBACProxy
		}
	}

	for i, container := range dep.Spec.Template.Spec.Containers {
		for j, arg := range container.Args {
			if strings.HasPrefix(arg, "--v=") {
				dep.Spec.Template.Spec.Containers[i].Args[j] = fmt.Sprintf("--v=%d", logLevel)
			}
		}
	}

	var one int32 = 1
	dep.Spec.Replicas = &one

	for _, modifier := range modifiers {
		dep = modifier(dep)
	}

	return dep
}

func withDeploymentStatus(availableReplicas, updatedReplicas int32) deploymentModifier {
	return func(instance *appsv1.Deployment) *appsv1.Deployment {
		instance.Status.AvailableReplicas = availableReplicas
		instance.Status.UpdatedReplicas = updatedReplicas
		return instance
	}
}

func withDeploymentReplicas(replicas int32) deploymentModifier {
	return func(instance *appsv1.Deployment) *appsv1.Deployment {
		instance.Spec.Replicas = &replicas
		return instance
	}
}

func withDeploymentGeneration(generations ...int64) deploymentModifier {
	return func(instance *appsv1.Deployment) *appsv1.Deployment {
		instance.Generation = generations[0]
		if len(generations) > 1 {
			instance.Status.ObservedGeneration = generations[1]
		}
		return instance
	}
}

// Infrastructure
func makeInfra() *configv1.Infrastructure {
	return &configv1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      infraConfigName,
			Namespace: v1.NamespaceAll,
		},
		Status: configv1.InfrastructureStatus{
			InfrastructureName: defaultClusterID,
			Platform:           configv1.AWSPlatformType,
			PlatformStatus: &configv1.PlatformStatus{
				AWS: &configv1.AWSPlatformStatus{},
			},
		},
	}
}

// This reactor is always enabled and bumps Deployment generation when it gets updated.
func addGenerationReactor(client *fakecore.Clientset) {
	client.PrependReactor("*", "deployments", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		switch a := action.(type) {
		case core.CreateActionImpl:
			object := a.GetObject()
			deployment := object.(*appsv1.Deployment)
			deployment.Generation++
			return false, deployment, nil
		case core.UpdateActionImpl:
			object := a.GetObject()
			deployment := object.(*appsv1.Deployment)
			deployment.Generation++
			return false, deployment, nil
		}
		return false, nil, nil
	})
}

func deploymentAnnotationHook(opSpec *opv1.OperatorSpec, instance *appsv1.Deployment) error {
	if instance.Annotations == nil {
		instance.Annotations = map[string]string{}
	}
	instance.Annotations[hookDeploymentAnnKey] = hookDeploymentAnnVal
	return nil
}

func TestDeploymentHook(t *testing.T) {
	// Initialize
	coreClient := fakecore.NewSimpleClientset()
	coreInformerFactory := coreinformers.NewSharedInformerFactory(coreClient, 0 /*no resync */)
	driverInstance := makeFakeDriverInstance()
	fakeOperatorClient := v1helpers.NewFakeOperatorClient(&driverInstance.Spec, &driverInstance.Status, nil /*triggerErr func*/)
	controller := NewCSIDriverControllerServiceController(
		controllerName,
		makeFakeManifest(),
		fakeOperatorClient,
		coreClient,
		coreInformerFactory.Apps().V1().Deployments(),
		nil, /* config informer*/
		events.NewInMemoryRecorder(operandName),
		deploymentAnnotationHook,
	)

	// Act
	err := controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("test-csi-driver")))
	if err != nil {
		t.Fatalf("sync() returned unexpected error: %v", err)
	}

	// Assert
	actualDeployment, err := coreClient.AppsV1().Deployments(operandNamespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Deployment %s: %v", deploymentName, err)
	}

	// Deployment should have the annotation specified in the hook function
	if actualDeployment.Annotations[hookDeploymentAnnKey] != hookDeploymentAnnVal {
		t.Fatalf("Annotation %q not found in Deployment", hookDeploymentAnnKey)
	}
}

func TestSync(t *testing.T) {
	var (
		argsLevel2 = 2
		argsLevel6 = 6
	)

	testCases := []testCase{
		{
			// Only CR exists, everything else is created
			name:   "only CR exists, everything  else is created",
			images: defaultImages(),
			initialObjects: testObjects{
				driver: makeFakeDriverInstance(),
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 0 /* ObservedGeneration*/)),
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withTrueConditions(conditionProgressing), // Progressing because ObservedGeneration != Generation
					withFalseConditions(conditionAvailable)), // Degraded is not set during Sync(), it's set based on the return of this method
			},
		},
		{
			name:   "deployment is fully deployed and its status is synced to CR",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(1 /* AvailableReplicas */, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(withGenerations(1)),
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(1 /* AvailableReplicas */, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withTrueConditions(conditionAvailable),
					withFalseConditions(conditionProgressing)),
			},
		},
		{
			name:   "user changes nr. of replicas and gets replaced by the controller",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentReplicas(2),                                               // User changed replicas
					withDeploymentGeneration(2 /* Generation */, 1 /* ObservedGeneration*/), // ... which changed Generation
					withDeploymentStatus(1 /* AvailableReplicas */, 1 /* UpdatedReplicas */)),

				driver: makeFakeDriverInstance(withGenerations(1)), // The operator knows the old generation of the Deployment
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentReplicas(1),                                               // The operator fixed replica count
					withDeploymentGeneration(3 /* Generation */, 1 /* ObservedGeneration*/), // ... which bumps generation again
					withDeploymentStatus(1 /* AvailableReplicas */, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(3), // Now the operator knows generation 3
					withTrueConditions(conditionAvailable, conditionProgressing), // Progressing due to Generation change
				),
			},
		},
		{
			name:   "deployment gets degraded",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)), // the Deployment has no pods
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withTrueConditions(conditionAvailable),
					withFalseConditions(conditionProgressing)),
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)), // no change to the Deployment
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withFalseConditions(conditionAvailable),    // The operator is not Available because there are no available pods
					withFalseConditions(conditionProgressing)), // The operator is not Progressing because the config being rolled out is not new
			},
		},
		{
			name:   "user changes log level in CR and it's projected into the deployment",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(1 /* AvailableReplicas*/, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withLogLevel(opv1.Trace), // User changed the log level...
					withGeneration(2, 1)),    //... which caused the Generation to increase
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel6, // The operator changed cmdline arguments with a new log level
					defaultImages(),
					withDeploymentGeneration(2 /* Generation */, 1 /* ObservedGeneration*/), // Generation bump to to log level change
					withDeploymentStatus(1 /* AvailableReplicas*/, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withLogLevel(opv1.Trace),
					withGenerations(2),
					withGeneration(2, 1), // TODO: should I increase the observed generation?
					withTrueConditions(conditionAvailable, conditionProgressing)), // Progressing due to Generation change
			},
		},
		{
			name:   "deployment updates images",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					oldImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(1 /* AvailableReplicas*/, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withTrueConditions(conditionAvailable),
					withFalseConditions(conditionProgressing)),
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(2 /* Generation */, 1 /* ObservedGeneration*/),
					withDeploymentStatus(1 /* AvailableReplicas*/, 1 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(2),
					withTrueConditions(conditionAvailable, conditionProgressing)),
			},
		},
		{
			name:   "deployment is rolling out pods with a known config, falls back to non-progressing condition",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration */),
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)), // Not all pods have been deployed yet
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withTrueConditions(conditionAvailable),
					withFalseConditions(conditionProgressing)), // It's known that we are NOT Progressing
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration */),
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)), // No change to the Deployment
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withFalseConditions(conditionAvailable),    // Not Available because AvailableReplicas == 0
					withFalseConditions(conditionProgressing)), // Not Progressing based on previous condition
			},
		},
		{
			name:   "deployment is rolling out pods with a known config, falls back to progressing condition",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration */),
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)), // Not all pods have been deployed yet
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withTrueConditions(conditionAvailable, conditionProgressing)), // It's known that we are Progressing
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(1 /* Generation */, 1 /* ObservedGeneration */), // No change to deployment
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withFalseConditions(conditionAvailable),   // Not Available because AvailableReplicas == 0
					withTrueConditions(conditionProgressing)), // Progressing based on previous condition
			},
		},
		{
			name:   "deployment is rolling out pods with new config (working towards correct generation)",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(2 /* Generation */, 1 /* ObservedGeneration */),  // Deployment controller hasn't synced yet
					withDeploymentStatus(1 /* AvailableReplicas */, 1 /* UpdatedReplicas */)), // Deployment is not done rolling out
				driver: makeFakeDriverInstance(
					withGenerations(1),
					withGeneration(1, 1),
					withTrueConditions(conditionAvailable),
					withFalseConditions(conditionProgressing)),
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(3 /* Generation */, 1 /* ObservedGeneration */),  // Generation was bumped because deployment was updated due to the CR having an outdated generation
					withDeploymentStatus(1 /* AvailableReplicas */, 1 /* UpdatedReplicas */)), // No change to the deployment
				driver: makeFakeDriverInstance(
					withGenerations(3), // Now CR knows about the newer generation
					withGeneration(1, 1),
					withTrueConditions(conditionAvailable),
					withTrueConditions(conditionProgressing)), // Progressing because the config is new (deploy.spec.generation != deploy.status.observedGeneration)
			},
		},
		{
			name:   "CR has outdated deployment generation",
			images: defaultImages(),
			initialObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					// Trick: bumped ObservedGeneration so that an early check
					// (e.g., if deploy.Generation != deploy.Status.ObservedGeneration {progressing=true}})
					//  doesn't prevent us from reaching the code we want to test
					withDeploymentGeneration(1 /* Generation */, 2 /* ObservedGeneration */),
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)),
				driver: makeFakeDriverInstance(
					withGenerations(0), // CR only knows about an outdated DaemonSet generation
					withGeneration(1, 1),
					withTrueConditions(conditionAvailable),
					withFalseConditions(conditionProgressing)),
			},
			expectedObjects: testObjects{
				deployment: makeDeployment(
					defaultClusterID,
					argsLevel2,
					defaultImages(),
					withDeploymentGeneration(2 /* Generation */, 2 /* ObservedGeneration */),  // Generation was bumped because Deployment was updated due to the CR having an outdated Deployment generation
					withDeploymentStatus(0 /* AvailableReplicas */, 0 /* UpdatedReplicas */)), // No change to the Deployment
				driver: makeFakeDriverInstance(
					withGenerations(2), // Now the CR has been updated with the updated Deployment generation
					withGeneration(1, 1),
					withFalseConditions(conditionAvailable),   // Not Available because there are no available replicas
					withTrueConditions(conditionProgressing)), // Progressing because Deployment controller is not done rolling out pods AND it has a different generation than the one that CR knows about
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Initialize
			ctx := newTestContext(test, t)

			// Act
			err := ctx.controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("test-csi-driver")))

			// Assert
			// Check error
			if err != nil && !test.expectErr {
				t.Errorf("sync() returned unexpected error: %v", err)
			}
			if err == nil && test.expectErr {
				t.Error("sync() unexpectedly succeeded when error was expected")
			}

			// Check expectedObjects.deployment
			if test.expectedObjects.deployment != nil {
				deployName := test.expectedObjects.deployment.Name
				actualDeployment, err := ctx.coreClient.AppsV1().Deployments(operandNamespace).Get(context.TODO(), deployName, metav1.GetOptions{})
				if err != nil {
					t.Errorf("Failed to get Deployment %s: %v", deployName, err)
				}
				sanitizeDeployment(actualDeployment)
				sanitizeDeployment(test.expectedObjects.deployment)
				if !equality.Semantic.DeepEqual(test.expectedObjects.deployment, actualDeployment) {
					t.Errorf("Unexpected Deployment %+v content:\n%s", operandName, cmp.Diff(test.expectedObjects.deployment, actualDeployment))
				}
			}

			// Check expectedObjects.driver.Status
			if test.expectedObjects.driver != nil {
				_, actualStatus, _, err := ctx.operatorClient.GetOperatorState()
				if err != nil {
					t.Errorf("Failed to get Driver: %v", err)
				}
				sanitizeInstanceStatus(actualStatus)
				sanitizeInstanceStatus(&test.expectedObjects.driver.Status)
				if !equality.Semantic.DeepEqual(test.expectedObjects.driver.Status, *actualStatus) {
					t.Errorf("Unexpected Driver %+v content:\n%s", operandName, cmp.Diff(test.expectedObjects.driver.Status, *actualStatus))
				}
			}
		})
	}
}

func sanitizeDeployment(deployment *appsv1.Deployment) {
	// nil and empty array are the same
	if len(deployment.Labels) == 0 {
		deployment.Labels = nil
	}
	if len(deployment.Annotations) == 0 {
		deployment.Annotations = nil
	}
	// Remove random annotations set by ApplyDeployment
	delete(deployment.Annotations, specHashAnnotation)
}

func sanitizeInstanceStatus(status *opv1.OperatorStatus) {
	// Remove condition texts
	for i := range status.Conditions {
		status.Conditions[i].LastTransitionTime = metav1.Time{}
		status.Conditions[i].Message = ""
		status.Conditions[i].Reason = ""
	}
	// Sort the conditions by name to have consistent position in the array
	sort.Slice(status.Conditions, func(i, j int) bool {
		return status.Conditions[i].Type < status.Conditions[j].Type
	})
}

func defaultImages() images {
	return images{
		csiDriver:     "quay.io/openshift/origin-test-csi-driver:latest",
		provisioner:   "quay.io/openshift/origin-csi-external-provisioner:latest",
		attacher:      "quay.io/openshift/origin-csi-external-attacher:latest",
		resizer:       "quay.io/openshift/origin-csi-external-resizer:latest",
		snapshotter:   "quay.io/openshift/origin-csi-external-snapshotter:latest",
		livenessProbe: "quay.io/openshift/origin-csi-livenessprobe:latest",
		kubeRBACProxy: "quay.io/openshift/origin-kube-rbac-proxy:latest",
	}
}

func oldImages() images {
	return images{
		csiDriver:     "quay.io/openshift/origin-test-csi-driver:old",
		provisioner:   "quay.io/openshift/origin-csi-external-provisioner:old",
		attacher:      "quay.io/openshift/origin-csi-external-attacher:old",
		resizer:       "quay.io/openshift/origin-csi-external-resizer:old",
		snapshotter:   "quay.io/openshift/origin-csi-external-snapshotter:old",
		livenessProbe: "quay.io/openshift/origin-csi-livenessprobe:old",
		kubeRBACProxy: "quay.io/openshift/origin-kube-rbac-proxy:old",
	}
}

// fakeInstance is a fake CSI driver instance that also fullfils the OperatorClient interface
type fakeDriverInstance struct {
	metav1.ObjectMeta
	Spec   opv1.OperatorSpec
	Status opv1.OperatorStatus
}

func makeFakeManifest() []byte {
	return []byte(`
kind: Deployment
apiVersion: apps/v1
metadata:
  name: test-csi-driver-controller
  namespace: openshift-test-csi-driver
spec:
  selector:
    matchLabels:
      app: test-csi-driver-controller
  serviceName: test-csi-driver-controller
  replicas: 1
  template:
    metadata:
      labels:
        app: test-csi-driver-controller
    spec:
      containers:
        - name: csi-driver
          image: ${DRIVER_IMAGE}
          args:
            - --endpoint=$(CSI_ENDPOINT)
            - --k8s-tag-cluster-id=${CLUSTER_ID}
            - --logtostderr
            - --v=${LOG_LEVEL}
          env:
            - name: CSI_ENDPOINT
              value: unix:///var/lib/csi/sockets/pluginproxy/csi.sock
          ports:
            - name: healthz
              containerPort: 19808
              protocol: TCP
          volumeMounts:
            - name: socket-dir
              mountPath: /var/lib/csi/sockets/pluginproxy/
        - name: csi-provisioner
          image: ${PROVISIONER_IMAGE}
          args:
            - --provisioner=test.csi.openshift.io
            - --csi-address=$(ADDRESS)
            - --feature-gates=Topology=true
            - --http-endpoint=localhost:8202
            - --v=${LOG_LEVEL}
          env:
            - name: ADDRESS
              value: /var/lib/csi/sockets/pluginproxy/csi.sock
          volumeMounts:
            - name: socket-dir
              mountPath: /var/lib/csi/sockets/pluginproxy/
        # In reality, each sidecar needs its own kube-rbac-proxy. Using just one for the unit tests.
        - name: provisioner-kube-rbac-proxy
          args:
          - --secure-listen-address=0.0.0.0:9202
          - --upstream=http://127.0.0.1:8202/
          - --tls-cert-file=/etc/tls/private/tls.crt
          - --tls-private-key-file=/etc/tls/private/tls.key
          - --logtostderr=true
          image: ${KUBE_RBAC_PROXY_IMAGE}
          imagePullPolicy: IfNotPresent
          ports:
          - containerPort: 9202
            name: provisioner-m
            protocol: TCP
          resources:
            requests:
              memory: 20Mi
              cpu: 10m
          volumeMounts:
          - mountPath: /etc/tls/private
            name: metrics-serving-cert
        - name: csi-attacher
          image: ${ATTACHER_IMAGE}
          args:
            - --csi-address=$(ADDRESS)
            - --v=${LOG_LEVEL}
          env:
            - name: ADDRESS
              value: /var/lib/csi/sockets/pluginproxy/csi.sock
          volumeMounts:
            - name: socket-dir
              mountPath: /var/lib/csi/sockets/pluginproxy/
        - name: csi-resizer
          image: ${RESIZER_IMAGE}
          args:
            - --csi-address=$(ADDRESS)
            - --v=${LOG_LEVEL}
          env:
            - name: ADDRESS
              value: /var/lib/csi/sockets/pluginproxy/csi.sock
          volumeMounts:
            - name: socket-dir
              mountPath: /var/lib/csi/sockets/pluginproxy/
        - name: csi-snapshotter
          image: ${SNAPSHOTTER_IMAGE}
          args:
            - --csi-address=$(ADDRESS)
            - --v=${LOG_LEVEL}
          env:
          - name: ADDRESS
            value: /var/lib/csi/sockets/pluginproxy/csi.sock
          volumeMounts:
          - mountPath: /var/lib/csi/sockets/pluginproxy/
            name: socket-dir
      volumes:
        - name: socket-dir
          emptyDir: {}
        - name: metrics-serving-cert
          secret:
            secretName: gcp-pd-csi-driver-controller-metrics-serving-cert
`)
}
