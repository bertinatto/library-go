package csicontrollerset

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

type CSIControllerSet struct {
	logLevelController        factory.Controller
	managementStateController factory.Controller
	staticResourcesController factory.Controller
	csiDriverController       factory.Controller

	operatorClient v1helpers.OperatorClient
	eventRecorder  events.Recorder
}

func (c *CSIControllerSet) Run(ctx context.Context, workers int) {
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

func (c *CSIControllerSet) WithLogLevelController() *CSIControllerSet {
	c.logLevelController = loglevel.NewClusterOperatorLoggingController(c.operatorClient, c.eventRecorder)
	return c
}

func (c *CSIControllerSet) WithManagementStateController(operandName string, supportsOperandRemoval bool) *CSIControllerSet {
	c.managementStateController = management.NewOperatorManagementStateController(operandName, c.operatorClient, c.eventRecorder)

	if supportsOperandRemoval {
		management.SetOperatorNotRemovable()
	}

	return c
}

func (c *CSIControllerSet) WithStaticResourcesController(
	name string,
	kubeClient kubernetes.Interface,
	kubeInformersForNamespace v1helpers.KubeInformersForNamespaces,
	manifests resourceapply.AssetFunc,
	files []string,
) *CSIControllerSet {
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

func (c *CSIControllerSet) WithCSIDriverController() *CSIControllerSet {
	return c
}

func NewCSIControllerSet(operatorClient v1helpers.OperatorClient, eventRecorder events.Recorder) *CSIControllerSet {
	return &CSIControllerSet{
		operatorClient: operatorClient,
		eventRecorder:  eventRecorder,
	}
}
