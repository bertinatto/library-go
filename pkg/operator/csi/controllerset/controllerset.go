package controllerset

import (
	"context"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"github.com/openshift/library-go/pkg/controller/factory"
	credentialscontroller "github.com/openshift/library-go/pkg/operator/csi/controller/credentials"
	csidrivercontroller "github.com/openshift/library-go/pkg/operator/csi/controller/driver"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/loglevel"
	"github.com/openshift/library-go/pkg/operator/management"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/staticresourcecontroller"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

// CSIDriverControllerOptions contains file names for manifests for both CSI Driver Deployment and DaemonSet.
type CSIDriverControllerOptions struct {
	controllerManifest string
	nodeManifest       string
}

// CSIDriverControllerOption is a modifier function for CSIDriverControllerOptions.
type CSIDriverControllerOption func(*CSIDriverControllerOptions)

// WithControllerService returns a CSIDriverControllerOption
// with a Deployment (CSI Controller Service) manifest file.
func WithControllerService(file string) CSIDriverControllerOption {
	return func(o *CSIDriverControllerOptions) {
		o.controllerManifest = file
	}
}

// WithNodeService returns a CSIDriverControllerOption
// with a DaemonSet (CSI Node Service) manifest file.
func WithNodeService(file string) CSIDriverControllerOption {
	return func(o *CSIDriverControllerOptions) {
		o.nodeManifest = file
	}
}

// ControllerSet contains a set of controllers that are usually used to deploy CSI Drivers.
type ControllerSet struct {
	logLevelController        factory.Controller
	managementStateController factory.Controller
	staticResourcesController factory.Controller
	credentialsController     factory.Controller
	csiDriverController       *csidrivercontroller.Controller

	operatorClient v1helpers.OperatorClient
	eventRecorder  events.Recorder
}

// Run starts all controllers initialized in the set.
func (c *ControllerSet) Run(ctx context.Context, workers int) {
	for _, controller := range []interface {
		Run(context.Context, int)
	}{
		c.logLevelController,
		c.managementStateController,
		c.staticResourcesController,
		c.credentialsController,
		c.csiDriverController,
	} {
		if controller != nil {
			go controller.Run(ctx, 1)
		}
	}
}

// WithLogLevelController returns a *ControllerSet with a log level controller initialized.
func (c *ControllerSet) WithLogLevelController() *ControllerSet {
	c.logLevelController = loglevel.NewClusterOperatorLoggingController(c.operatorClient, c.eventRecorder)
	return c
}

// WithManagementStateController returns a *ControllerSet with a management state controller initialized.
func (c *ControllerSet) WithManagementStateController(operandName string, supportsOperandRemoval bool) *ControllerSet {
	c.managementStateController = management.NewOperatorManagementStateController(operandName, c.operatorClient, c.eventRecorder)
	if supportsOperandRemoval {
		management.SetOperatorNotRemovable()
	}
	return c
}

// WithStaticResourcesController returns a *ControllerSet with a static resources controller initialized.
func (c *ControllerSet) WithStaticResourcesController(
	name string,
	kubeClient kubernetes.Interface,
	kubeInformersForNamespace v1helpers.KubeInformersForNamespaces,
	manifests resourceapply.AssetFunc,
	files []string,
) *ControllerSet {
	c.staticResourcesController = staticresourcecontroller.NewStaticResourceController(
		name,
		manifests,
		files,
		(&resourceapply.ClientHolder{}).WithKubernetes(kubeClient),
		c.operatorClient,
		c.eventRecorder,
	).AddKubeInformers(kubeInformersForNamespace)
	return c
}

// WithCredentialsController returns a *ControllerSet with a credentials controller initialized.
func (c *ControllerSet) WithCredentialsController(
	name string,
	operandNamespace string,
	assetFunc func(string) []byte,
	file string,
	dynamicClient dynamic.Interface,
) *ControllerSet {
	manifestFile := assetFunc(file)
	c.credentialsController = credentialscontroller.New(name, operandNamespace, manifestFile, dynamicClient, c.operatorClient, c.eventRecorder)
	return c
}

// WithCSIDriverController returns a *ControllerSet with a CSI Driver controller initialized.
func (c *ControllerSet) WithCSIDriverController(
	name string,
	csiDriverName string,
	csiDriverNamespace string,
	assetFunc func(string) []byte,
	kubeClient kubernetes.Interface,
	namespacedInformerFactory informers.SharedInformerFactory,
	setters ...CSIDriverControllerOption,
) *ControllerSet {
	cdc := csidrivercontroller.New(
		name,
		csiDriverName,
		csiDriverNamespace,
		c.operatorClient,
		assetFunc,
		kubeClient,
		c.eventRecorder,
	)

	opts := &CSIDriverControllerOptions{}
	for _, setter := range setters {
		setter(opts)
	}

	if opts.controllerManifest != "" {
		cdc = cdc.WithControllerService(namespacedInformerFactory.Apps().V1().Deployments(), opts.controllerManifest)
	}

	if opts.nodeManifest != "" {
		cdc = cdc.WithNodeService(namespacedInformerFactory.Apps().V1().DaemonSets(), opts.nodeManifest)
	}

	c.csiDriverController = cdc

	return c
}

// New returns a basic *ControllerSet without any controller.
func New(operatorClient v1helpers.OperatorClient, eventRecorder events.Recorder) *ControllerSet {
	return &ControllerSet{
		operatorClient: operatorClient,
		eventRecorder:  eventRecorder,
	}
}
