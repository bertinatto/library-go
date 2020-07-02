package credentials

import (
	"context"
	"time"

	operatorv1 "github.com/openshift/api/operator/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/resource/resourcemerge"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	operatorv1helpers "github.com/openshift/library-go/pkg/operator/v1helpers"
)

type Controller struct {
	operatorName    string
	operatorClient  operatorv1helpers.OperatorClient
	targetNamespace string
	manifest        []byte
	dynamicClient   dynamic.Interface
}

func New(name, targetNamespace string, manifest []byte, dynamicClient dynamic.Interface, operatorClient operatorv1helpers.OperatorClient, recorder events.Recorder) factory.Controller {
	c := &Controller{
		operatorName:    name,
		operatorClient:  operatorClient,
		targetNamespace: targetNamespace,
		manifest:        manifest,
		dynamicClient:   dynamicClient,
	}
	return factory.New().WithInformers(
		operatorClient.Informer(),
	).WithSync(
		c.sync,
	).ResyncEvery(
		time.Second,
	).ToController(
		"CredentialsController",
		recorder.WithComponentSuffix("credentials-recorder"),
	)
}

func (c Controller) syncCredentialsRequest(status *operatorv1.OperatorStatus, syncContext factory.SyncContext) (*unstructured.Unstructured, error) {
	cr := readCredentialRequestsOrDie(c.manifest)

	// Set spec.secretRef.namespace
	err := unstructured.SetNestedField(cr.Object, c.targetNamespace, "spec", "secretRef", "namespace")
	if err != nil {
		return nil, err
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

	cr, _, err = applyCredentialsRequest(c.dynamicClient, syncContext.Recorder(), cr, expectedGeneration)
	return cr, err
}

func (c Controller) sync(ctx context.Context, syncContext factory.SyncContext) error {
	_, status, _, err := c.operatorClient.GetOperatorState()
	if apierrors.IsNotFound(err) {
		syncContext.Recorder().Warningf("StatusNotFound", "Unable to determine current operator status for %s", c.operatorName)
		return nil
	}

	cond := operatorv1.OperatorCondition{
		// TODO: check condition
		// Type:   condition.ResourceSyncControllerDegradedConditionType,
		Type:   "CredentialsSyncControllerDegraded",
		Status: operatorv1.ConditionFalse,
	}

	_, err = c.syncCredentialsRequest(status, syncContext)
	if err != nil {
		cond.Status = operatorv1.ConditionTrue
		// TODO: not unkown
		cond.Reason = "Unknown"
		cond.Message = err.Error()
	}

	if _, _, updateError := v1helpers.UpdateStatus(c.operatorClient, v1helpers.UpdateConditionFn(cond)); updateError != nil {
		if err == nil {
			return updateError
		}
	}

	return nil
}
