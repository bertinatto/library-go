package controllers

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	configv1 "github.com/openshift/api/config/v1"
	operatorv1 "github.com/openshift/api/operator/v1"
	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"

	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
	"github.com/openshift/library-go/pkg/operator/encryption/secrets"
	"github.com/openshift/library-go/pkg/operator/encryption/statemachine"
	operatorv1helpers "github.com/openshift/library-go/pkg/operator/v1helpers"
)

const preflightKMSSocketEndpoint = "unix:///var/run/kmsplugin/kms.sock"

// newPreflightEncryptionComputer builds an EncryptionComputer from the same inputs
// used by the key and state controllers. The encryption deployer is treated as
// converged so preflight is not blocked on API server revision rollout (matching
// in-place KMS field updates).
func newPreflightEncryptionComputer(
	instanceName string,
	unsupportedConfigPrefix []string,
	provider Provider,
	encryptionDeployer statemachine.Deployer,
	operatorClient operatorv1helpers.OperatorClient,
	apiServerClient configv1client.APIServerInterface,
	secretsClient corev1client.SecretsGetter,
	configMapsClient corev1client.ConfigMapsGetter,
	encryptionSecretSelector metav1.ListOptions,
) *EncryptionComputer {
	deployedEncryptionConfigSecretFn := forceConvergedDeployedEncryptionConfigSecret(encryptionDeployer)
	listKeySecretsFn := func(ctx context.Context) ([]*corev1.Secret, error) {
		return secrets.ListKeySecrets(ctx, secretsClient, encryptionSecretSelector)
	}

	keyCtrl := &keyController{
		operatorClient:           operatorClient,
		apiServerClient:          apiServerClient,
		instanceName:             instanceName,
		unsupportedConfigPrefix:  unsupportedConfigPrefix,
		encryptionSecretSelector: encryptionSecretSelector,
		provider:                 provider,
		secretClient:             secretsClient,
		configMapClient:          configMapsClient,
		getAPIServerAndOperatorSpecFn: func(ctx context.Context) (*configv1.APIServer, *operatorv1.OperatorSpec, error) {
			apiServer, err := apiServerClient.Get(ctx, "cluster", metav1.GetOptions{})
			if err != nil {
				return nil, nil, err
			}
			operatorSpec, _, _, err := operatorClient.GetOperatorState()
			if err != nil {
				return nil, nil, err
			}
			return apiServer, operatorSpec, nil
		},
		deployedEncryptionConfigSecretFn: deployedEncryptionConfigSecretFn,
		listKeySecretsFn:                 listKeySecretsFn,
		getKMSPluginSecretFn: func(ctx context.Context, name string) (*corev1.Secret, error) {
			return secretsClient.Secrets(openshiftConfigNS).Get(ctx, name, metav1.GetOptions{})
		},
		getKMSPluginConfigMapFn: func(ctx context.Context, name string) (*corev1.ConfigMap, error) {
			return configMapsClient.ConfigMaps(openshiftConfigNS).Get(ctx, name, metav1.GetOptions{})
		},
	}

	stateCtrl := &stateController{
		instanceName:                     instanceName,
		encryptionSecretSelector:         encryptionSecretSelector,
		provider:                         provider,
		secretClient:                     secretsClient,
		deployedEncryptionConfigSecretFn: deployedEncryptionConfigSecretFn,
		listKeySecretsFn:                 listKeySecretsFn,
	}

	return NewEncryptionComputer(keyCtrl, stateCtrl)
}

// forceConvergedDeployedEncryptionConfigSecret wraps a Deployer's secret lookup so
// the computation always proceeds as if operands have acknowledged the config.
func forceConvergedDeployedEncryptionConfigSecret(deployer statemachine.Deployer) func(context.Context) (*corev1.Secret, bool, error) {
	return func(ctx context.Context) (*corev1.Secret, bool, error) {
		secret, _, err := deployer.DeployedEncryptionConfigSecret(ctx)
		if err != nil {
			return nil, false, err
		}
		return secret, true, nil
	}
}

// rewritePreflightKeyEndpoint rewrites the candidate key's KMS endpoint from the
// per-key production socket (kms-{id}.sock) to the fixed preflight socket
// (kms.sock). Today's preflight checker dials that fixed path, while the plugin
// builder uses each provider's Endpoint as -listen-address, so the secret must
// agree with the checker.
//
// On the first transitional pass the new key is often only a read key (identity
// write); matching by key-ID name prefix ("{id}_") is intentional.
//
// TODO: once preflight dials the write-key socket (kms-{id}.sock) directly,
// this rewrite can be removed and the secret can be used as-is.
func rewritePreflightKeyEndpoint(secret *corev1.Secret, keyID uint64) (*corev1.Secret, error) {
	cfg, err := encryptiondata.FromSecret(secret)
	if err != nil {
		return nil, err
	}
	if cfg == nil || cfg.Encryption == nil {
		return nil, fmt.Errorf("encryption configuration is empty")
	}

	wantNamePrefix := fmt.Sprintf("%d_", keyID)
	rewrote := false
	for i := range cfg.Encryption.Resources {
		for j := range cfg.Encryption.Resources[i].Providers {
			kms := cfg.Encryption.Resources[i].Providers[j].KMS
			if kms == nil || !strings.HasPrefix(kms.Name, wantNamePrefix) {
				continue
			}
			kms.Endpoint = preflightKMSSocketEndpoint
			rewrote = true
		}
	}
	if !rewrote {
		return nil, fmt.Errorf("KMS provider with name prefix %q not found in encryption config", wantNamePrefix)
	}

	return encryptiondata.ToSecret(secret.Namespace, secret.Name, cfg)
}
