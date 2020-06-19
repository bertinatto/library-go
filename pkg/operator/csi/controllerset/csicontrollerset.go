package controllerset

import (
	"context"

	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/loglevel"
	"github.com/openshift/library-go/pkg/operator/management"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/staticresourcecontroller"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	"k8s.io/client-go/kubernetes"
)

type ControllerSet struct {
	logLevelController        factory.Controller
	managementStateController factory.Controller
	staticResourcesController factory.Controller
	csiDriverController       factory.Controller

	operatorClient v1helpers.OperatorClient
	eventRecorder  events.Recorder
}

func (c *ControllerSet) Run(ctx context.Context, workers int) {
	controllers := []factory.Controller{
		c.logLevelController,
		c.managementStateController,
		c.staticResourcesController,
		c.csiDriverController,
	}
	for i := range controllers {
		if controllers[i] != nil {
			go controllers[i].Run(ctx, workers)
		}
	}
}

func (c *ControllerSet) WithLogLevelController() *ControllerSet {
	c.logLevelController = loglevel.NewClusterOperatorLoggingController(c.operatorClient, c.eventRecorder)
	return c
}

func (c *ControllerSet) WithManagementStateController(operandName string, supportsOperandRemoval bool) *ControllerSet {
	c.managementStateController = management.NewOperatorManagementStateController(operandName, c.operatorClient, c.eventRecorder)

	if supportsOperandRemoval {
		management.SetOperatorNotRemovable()
	}

	return c
}

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

func (c *ControllerSet) WithCSIDriverController() *ControllerSet {
	return c
}

func New(operatorClient v1helpers.OperatorClient, eventRecorder events.Recorder) *ControllerSet {
	return &ControllerSet{
		operatorClient: operatorClient,
		eventRecorder:  eventRecorder,
	}
}
