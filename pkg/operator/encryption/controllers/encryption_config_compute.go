package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	configv1 "github.com/openshift/api/config/v1"
	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"

	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
	"github.com/openshift/library-go/pkg/operator/encryption/state"
	"github.com/openshift/library-go/pkg/operator/encryption/statemachine"
	operatorv1helpers "github.com/openshift/library-go/pkg/operator/v1helpers"
)

// ComputeDesiredEncryptionConfigOptions controls whether a would-be next key is
// included when assembling desired encryption configuration.
type ComputeDesiredEncryptionConfigOptions struct {
	// IncludePlannedKey runs key planning and, when a new key is required,
	// builds it in memory and includes it in desired state. Key controller and
	// preflight set this true; state controller sets it false so it only
	// reflects persisted key secrets.
	IncludePlannedKey bool
	// KMSEndpointOverride, when non-empty, is used as the KMS listen endpoint on
	// a newly planned key (e.g. preflightKMSSocketEndpoint). Empty keeps the
	// production per-key socket path.
	KMSEndpointOverride string
}

// plannedEncryptionKey is an in-memory key Secret the key controller would create.
type plannedEncryptionKey struct {
	Secret  *corev1.Secret
	KeyID   uint64
	Reasons []string
}

// ComputeDesiredEncryptionConfigResult is the shared output of desired encryption
// config assembly. Callers decide whether to Create the planned key, Apply the
// encryption-config Secret, or rewrite endpoints for preflight.
type ComputeDesiredEncryptionConfigResult struct {
	// ProgressingReason is non-empty when the encryption deployer has not
	// converged; callers should requeue and not persist.
	ProgressingReason string
	// PlannedKey is set when IncludePlannedKey is true and a new key is required.
	PlannedKey *plannedEncryptionKey
	// CurrentConfig is the parsed deployed encryption config, if any.
	CurrentConfig *encryptiondata.Config
	// DesiredState is the encryption state after optional planned-key inclusion.
	DesiredState map[schema.GroupResource]state.GroupResourceState
	// EncryptionConfig is FromEncryptionState(DesiredState); nil when there is
	// nothing to apply yet (no current config and no key secrets).
	EncryptionConfig *encryptiondata.Config
	// EncryptionSecret is ToSecret of EncryptionConfig; nil when EncryptionConfig is nil.
	EncryptionSecret *corev1.Secret
	// KeySecrets are the live key secrets, plus the planned key when included.
	KeySecrets []*corev1.Secret
	// CurrentMode is the resolved encryption mode (only set when IncludePlannedKey).
	CurrentMode state.Mode
}

// ComputeDesiredEncryptionConfig assembles the desired encryption-config content
// shared by the key controller, state controller, and KMS preflight. It never
// Creates or Applies Secrets.
func ComputeDesiredEncryptionConfig(
	ctx context.Context,
	instanceName string,
	unsupportedConfigPrefix []string,
	encryptedGRs []schema.GroupResource,
	deployer statemachine.Deployer,
	secretClient corev1client.SecretsGetter,
	configMapClient corev1client.ConfigMapsGetter,
	apiServerClient configv1client.APIServerInterface,
	operatorClient operatorv1helpers.OperatorClient,
	encryptionSecretSelector metav1.ListOptions,
	opts ComputeDesiredEncryptionConfigOptions,
) (ComputeDesiredEncryptionConfigResult, error) {
	var out ComputeDesiredEncryptionConfigResult

	// When planning a key, resolve mode first (same order as the key controller)
	// so missing APIServer fails before deployer/list side effects.
	var (
		currentMode    state.Mode
		externalReason string
		apiEncryption  configv1.APIServerEncryption
	)
	if opts.IncludePlannedKey {
		if apiServerClient == nil || operatorClient == nil {
			return out, fmt.Errorf("apiServerClient and operatorClient are required when IncludePlannedKey is set")
		}
		if configMapClient == nil {
			return out, fmt.Errorf("configMapClient is required when IncludePlannedKey is set")
		}
		var err error
		currentMode, externalReason, apiEncryption, err = resolveEncryptionModeAndConfig(ctx, apiServerClient, operatorClient, unsupportedConfigPrefix)
		if err != nil {
			return out, err
		}
		out.CurrentMode = currentMode
	}

	currentConfig, desiredBeforePlan, keySecrets, progressingReason, err := statemachine.GetEncryptionConfigAndState(
		ctx, deployer, secretClient, encryptionSecretSelector, encryptedGRs,
	)
	if err != nil {
		return out, err
	}
	if len(progressingReason) > 0 {
		out.ProgressingReason = progressingReason
		return out, nil
	}
	out.CurrentConfig = currentConfig
	out.KeySecrets = keySecrets

	if !opts.IncludePlannedKey {
		return finishComputeDesiredEncryptionConfig(instanceName, encryptedGRs, currentConfig, keySecrets, out)
	}

	// Avoid intended start of encryption (same gate as the key controller).
	hasBeenOnBefore := currentConfig != nil || len(keySecrets) > 0
	if currentMode == state.Identity && !hasBeenOnBefore {
		return out, nil
	}

	var desiredProviderCfg kmsProviderConfig = noopKMSProviderConfig{}
	if currentMode == state.KMS {
		desiredProviderCfg, err = newKMSProviderConfig(apiEncryption.KMS)
		if err != nil {
			return out, err
		}
	}

	keyPlan, err := planNextEncryptionKey(desiredBeforePlan, currentMode, externalReason, encryptedGRs, desiredProviderCfg)
	if err != nil {
		return out, err
	}
	if !keyPlan.needed {
		return finishComputeDesiredEncryptionConfig(instanceName, encryptedGRs, currentConfig, keySecrets, out)
	}

	keySecret, err := buildEncryptionKeySecret(
		ctx,
		instanceName,
		keyPlan.keyID,
		currentMode,
		apiEncryption,
		desiredProviderCfg,
		secretClient,
		configMapClient,
		keyPlan.internalReason,
		externalReason,
		opts.KMSEndpointOverride,
	)
	if err != nil {
		return out, plannedKeyBuildError{err: err}
	}
	out.PlannedKey = &plannedEncryptionKey{
		Secret:  keySecret,
		KeyID:   keyPlan.keyID,
		Reasons: keyPlan.reasons,
	}
	keySecrets = append(keySecrets, keySecret)
	out.KeySecrets = keySecrets

	return finishComputeDesiredEncryptionConfig(instanceName, encryptedGRs, currentConfig, keySecrets, out)
}

// plannedKeyBuildError marks failures while materializing an in-memory planned key
// (missing referenced Secrets/ConfigMaps, etc.). The key controller wraps these as
// "failed to create key" for historical degraded messages.
type plannedKeyBuildError struct {
	err error
}

func (e plannedKeyBuildError) Error() string { return e.err.Error() }
func (e plannedKeyBuildError) Unwrap() error { return e.err }

func finishComputeDesiredEncryptionConfig(
	instanceName string,
	encryptedGRs []schema.GroupResource,
	currentConfig *encryptiondata.Config,
	keySecrets []*corev1.Secret,
	out ComputeDesiredEncryptionConfigResult,
) (ComputeDesiredEncryptionConfigResult, error) {
	if currentConfig == nil && len(keySecrets) == 0 {
		return out, nil
	}

	desiredState := statemachine.GetDesiredEncryptionState(currentConfig, keySecrets, encryptedGRs)
	out.DesiredState = desiredState

	cfg, err := encryptiondata.FromEncryptionState(desiredState)
	if err != nil {
		return out, fmt.Errorf("failed to build encryption config: %w", err)
	}
	out.EncryptionConfig = cfg

	secretName := fmt.Sprintf("%s-%s", encryptiondata.EncryptionConfSecretName, instanceName)
	secret, err := encryptiondata.ToSecret("openshift-config-managed", secretName, cfg)
	if err != nil {
		return out, err
	}
	out.EncryptionSecret = secret
	return out, nil
}