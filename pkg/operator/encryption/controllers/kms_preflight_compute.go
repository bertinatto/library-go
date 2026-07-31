package controllers

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"

	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
	"github.com/openshift/library-go/pkg/operator/encryption/state"
	"github.com/openshift/library-go/pkg/operator/encryption/statemachine"
	operatorv1helpers "github.com/openshift/library-go/pkg/operator/v1helpers"
)

const (
	openshiftConfigManagedNS   = "openshift-config-managed"
	preflightKMSSocketEndpoint = "unix:///var/run/kmsplugin/kms.sock"
)

// computeDesiredEncryptionConfigSecret builds the encryption-config Secret that the
// key and state controllers would produce for the current desired encryption state,
// without persisting key or encryption-config Secrets.
//
// If a new key is required, it is generated in memory and included when assembling
// desired state. If the live encryption deployer reports !converged, requeue is true
// and secret is nil.
func computeDesiredEncryptionConfigSecret(
	ctx context.Context,
	instanceName string,
	unsupportedConfigPrefix []string,
	provider Provider,
	encryptionDeployer statemachine.Deployer,
	operatorClient operatorv1helpers.OperatorClient,
	apiServerClient configv1client.APIServerInterface,
	coreClient corev1client.CoreV1Interface,
	encryptionSecretSelector metav1.ListOptions,
) (requeue bool, secret *corev1.Secret, err error) {
	_, converged, err := encryptionDeployer.DeployedEncryptionConfigSecret(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get deployed encryption config: %w", err)
	}
	if !converged {
		return true, nil, nil
	}

	encryptedGRs := provider.EncryptedGRs()
	progressingReason, planned, err := planNewEncryptionKey(
		ctx,
		instanceName,
		unsupportedConfigPrefix,
		encryptedGRs,
		encryptionDeployer,
		coreClient,
		coreClient,
		apiServerClient,
		operatorClient,
		encryptionSecretSelector,
	)
	if err != nil {
		return false, nil, err
	}
	if len(progressingReason) > 0 {
		// Defensive: DeployedEncryptionConfigSecret already reported converged above.
		// GetEncryptionConfigAndState can still return a progressing reason if the
		// deployer races; treat it the same as !converged.
		return true, nil, nil
	}

	currentConfig, _, keySecrets, progressingReason, err := statemachine.GetEncryptionConfigAndState(
		ctx, encryptionDeployer, coreClient, encryptionSecretSelector, encryptedGRs,
	)
	if err != nil {
		return false, nil, err
	}
	if len(progressingReason) > 0 {
		return true, nil, nil
	}

	if planned != nil {
		keySecrets = append(keySecrets, planned.Secret)
	}

	if currentConfig == nil && len(keySecrets) == 0 {
		return false, nil, fmt.Errorf("no encryption key secrets available to compute preflight encryption config")
	}

	desiredState := statemachine.DesiredEncryptionState(currentConfig, keySecrets, encryptedGRs)
	desiredSecretData, err := encryptiondata.FromEncryptionState(desiredState)
	if err != nil {
		return false, nil, err
	}

	expectedName := fmt.Sprintf("%s-%s", encryptiondata.EncryptionConfSecretName, instanceName)
	encryptionSecret, err := encryptiondata.ToSecret(openshiftConfigManagedNS, expectedName, desiredSecretData)
	if err != nil {
		return false, nil, err
	}

	writeKeyID, err := latestKeyIDFromSecrets(keySecrets)
	if err != nil {
		return false, nil, err
	}

	rewritten, err := rewritePreflightWriteKeyEndpoint(encryptionSecret, writeKeyID)
	if err != nil {
		return false, nil, fmt.Errorf("failed to rewrite preflight KMS endpoint: %w", err)
	}
	return false, rewritten, nil
}

func latestKeyIDFromSecrets(keySecrets []*corev1.Secret) (uint64, error) {
	var latestKeyID uint64
	foundKey := false
	for _, s := range keySecrets {
		id, ok := state.NameToKeyID(s.Name)
		if !ok {
			continue
		}
		if !foundKey || id > latestKeyID {
			latestKeyID = id
			foundKey = true
		}
	}
	if !foundKey {
		return 0, fmt.Errorf("no encryption key secrets found after computing desired encryption config")
	}
	return latestKeyID, nil
}

// rewritePreflightWriteKeyEndpoint rewrites the write-key KMS endpoint from the
// per-key production socket (kms-{id}.sock) to the fixed preflight socket
// (kms.sock). Today's preflight checker dials that fixed path, while the plugin
// builder uses each provider's Endpoint as -listen-address, so the computed
// secret must agree with the checker.
//
// TODO: once preflight dials the write-key socket (kms-{id}.sock) directly,
// this rewrite can be removed and the computed secret can be used as-is.
func rewritePreflightWriteKeyEndpoint(secret *corev1.Secret, keyID uint64) (*corev1.Secret, error) {
	cfg, err := encryptiondata.FromSecret(secret)
	if err != nil {
		return nil, err
	}
	if cfg == nil || cfg.Encryption == nil {
		return nil, fmt.Errorf("encryption configuration is empty")
	}

	wantKeyID := strconv.FormatUint(keyID, 10)
	rewrote := false
	for i := range cfg.Encryption.Resources {
		for j := range cfg.Encryption.Resources[i].Providers {
			kms := cfg.Encryption.Resources[i].Providers[j].KMS
			if kms == nil {
				continue
			}
			// Provider names are "{keyID}_{resource}" (e.g. "2_secrets").
			nameKeyID, _, ok := strings.Cut(kms.Name, "_")
			if !ok || nameKeyID != wantKeyID {
				continue
			}
			kms.Endpoint = preflightKMSSocketEndpoint
			rewrote = true
		}
	}
	if !rewrote {
		return nil, fmt.Errorf("write-key KMS provider with key ID %s not found in encryption config", wantKeyID)
	}

	return encryptiondata.ToSecret(secret.Namespace, secret.Name, cfg)
}
