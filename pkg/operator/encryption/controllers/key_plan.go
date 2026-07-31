package controllers

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"

	configv1 "github.com/openshift/api/config/v1"
	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"

	"github.com/openshift/library-go/pkg/operator/encryption/crypto"
	"github.com/openshift/library-go/pkg/operator/encryption/secrets"
	"github.com/openshift/library-go/pkg/operator/encryption/state"
	"github.com/openshift/library-go/pkg/operator/encryption/statemachine"
	operatorv1helpers "github.com/openshift/library-go/pkg/operator/v1helpers"
)

// plannedEncryptionKey is an in-memory key Secret the key controller would create.
// Nothing is persisted; callers decide whether to Create it.
type plannedEncryptionKey struct {
	Secret  *corev1.Secret
	KeyID   uint64
	Reasons []string
}

// planNewEncryptionKey decides whether a new encryption key is required and, if so,
// builds the Secret that would be created. It reads live state only and never writes.
//
// If progressingReason is non-empty, the encryption deployer has not converged and
// the caller should retry later (planned is nil).
// If planned is nil and progressingReason is empty, no new key is needed.
func planNewEncryptionKey(
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
) (progressingReason string, planned *plannedEncryptionKey, err error) {
	currentMode, externalReason, apiEncryptionConfiguration, err := resolveEncryptionModeAndConfig(ctx, apiServerClient, operatorClient, unsupportedConfigPrefix)
	if err != nil {
		return "", nil, err
	}

	currentConfig, desiredEncryptionState, keySecrets, progressingReason, err := statemachine.GetEncryptionConfigAndState(ctx, deployer, secretClient, encryptionSecretSelector, encryptedGRs)
	if err != nil {
		return "", nil, err
	}
	if len(progressingReason) > 0 {
		return progressingReason, nil, nil
	}

	// avoid intended start of encryption
	hasBeenOnBefore := currentConfig != nil || len(keySecrets) > 0
	if currentMode == state.Identity && !hasBeenOnBefore {
		return "", nil, nil
	}

	var desiredProviderCfg kmsProviderConfig = noopKMSProviderConfig{}
	if currentMode == state.KMS {
		desiredProviderCfg, err = newKMSProviderConfig(apiEncryptionConfiguration.KMS)
		if err != nil {
			return "", nil, err
		}
	}

	var (
		newKeyRequired bool
		newKeyID       uint64
		reasons        []string
		commonReason   *string
	)
	for gr, grKeys := range desiredEncryptionState {
		latestKeyID, internalReason, needed, err := needsNewKey(grKeys, currentMode, externalReason, encryptedGRs, desiredProviderCfg)
		if err != nil {
			return "", nil, err
		}
		if !needed {
			continue
		}

		if commonReason == nil {
			commonReason = &internalReason
		} else if *commonReason != internalReason {
			commonReason = ptr.To("") // this means we have no common reason
		}

		newKeyRequired = true
		nextKeyID := latestKeyID + 1
		if newKeyID < nextKeyID {
			newKeyID = nextKeyID
		}
		reasons = append(reasons, fmt.Sprintf("%s-%s", gr.Resource, internalReason))
	}
	if !newKeyRequired {
		return "", nil, nil
	}
	if commonReason != nil && len(*commonReason) > 0 && len(reasons) > 1 {
		reasons = []string{*commonReason} // don't repeat reasons
	}

	sort.Sort(sort.StringSlice(reasons))
	internalReason := strings.Join(reasons, ", ")
	keySecret, err := buildEncryptionKeySecret(ctx, instanceName, secretClient, configMapClient, newKeyID, currentMode, apiEncryptionConfiguration, desiredProviderCfg, internalReason, externalReason)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create key: %v", err)
	}
	return "", &plannedEncryptionKey{Secret: keySecret, KeyID: newKeyID, Reasons: reasons}, nil
}

func resolveEncryptionModeAndConfig(
	ctx context.Context,
	apiServerClient configv1client.APIServerInterface,
	operatorClient operatorv1helpers.OperatorClient,
	unsupportedConfigPrefix []string,
) (state.Mode, string, configv1.APIServerEncryption, error) {
	apiServer, err := apiServerClient.Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		return "", "", configv1.APIServerEncryption{}, err
	}

	operatorSpec, _, _, err := operatorClient.GetOperatorState()
	if err != nil {
		return "", "", configv1.APIServerEncryption{}, err
	}

	encryptionConfig, err := structuredUnsupportedConfigFrom(operatorSpec.UnsupportedConfigOverrides.Raw, unsupportedConfigPrefix)
	if err != nil {
		return "", "", configv1.APIServerEncryption{}, err
	}

	encryption := apiServer.Spec.Encryption
	reason := encryptionConfig.Encryption.Reason
	switch currentMode := state.Mode(encryption.Type); currentMode {
	case state.AESCBC, state.AESGCM, state.Identity: // secretbox is disabled for now
		return currentMode, reason, encryption, nil
	case state.KMS:
		return currentMode, reason, encryption, nil
	case "": // unspecified means use the default (which can change over time)
		return state.DefaultMode, reason, encryption, nil
	default:
		return "", "", configv1.APIServerEncryption{}, fmt.Errorf("unknown encryption mode configured: %s", currentMode)
	}
}

func buildEncryptionKeySecret(
	ctx context.Context,
	instanceName string,
	secretClient corev1client.SecretsGetter,
	configMapClient corev1client.ConfigMapsGetter,
	keyID uint64,
	currentMode state.Mode,
	apiServerEncryption configv1.APIServerEncryption,
	desiredProviderCfg kmsProviderConfig,
	internalReason, externalReason string,
) (*corev1.Secret, error) {
	bs := crypto.ModeToNewKeyFunc[currentMode]()
	ks := state.KeyState{
		Key: apiserverv1.Key{
			Name:   fmt.Sprintf("%d", keyID),
			Secret: base64.StdEncoding.EncodeToString(bs),
		},
		Mode:           currentMode,
		InternalReason: internalReason,
		ExternalReason: externalReason,
	}
	if currentMode == state.KMS {
		ks.KMS = &state.KMSState{
			Encryption: &apiserverv1.KMSConfiguration{
				APIVersion: "v2",
				Name:       fmt.Sprintf("%d", keyID),
				Endpoint:   fmt.Sprintf(kmsEndpointFormat, keyID),
				Timeout:    &metav1.Duration{Duration: defaultKMSTimeout},
			},
			Plugin: apiServerEncryption.KMS,
		}

		if secretName, expectedKeys, err := desiredProviderCfg.referencedSecretName(); err != nil {
			return nil, err
		} else if len(secretName) > 0 {
			refSecret, err := secretClient.Secrets(openshiftConfigNS).Get(ctx, secretName, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get secret %s in %s: %w", secretName, openshiftConfigNS, err)
			}
			for _, key := range expectedKeys {
				v, ok := refSecret.Data[key]
				if !ok {
					return nil, fmt.Errorf("secret %s in %s is missing required key %q", secretName, openshiftConfigNS, key)
				}
				if err := ks.KMS.PluginSecretData.Set(secretName, key, v); err != nil {
					return nil, err
				}
			}
		}

		if cmName, expectedKeys, err := desiredProviderCfg.referencedConfigMapName(); err != nil {
			return nil, err
		} else if len(cmName) > 0 {
			refCM, err := configMapClient.ConfigMaps(openshiftConfigNS).Get(ctx, cmName, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get configmap %s in %s: %w", cmName, openshiftConfigNS, err)
			}
			for _, key := range expectedKeys {
				v, ok := refCM.Data[key]
				if !ok {
					return nil, fmt.Errorf("configmap %s in %s is missing required key %q", cmName, openshiftConfigNS, key)
				}
				if err := ks.KMS.PluginConfigMapData.Set(cmName, key, []byte(v)); err != nil {
					return nil, err
				}
			}
		}
	}
	return secrets.FromKeyState(instanceName, ks)
}
