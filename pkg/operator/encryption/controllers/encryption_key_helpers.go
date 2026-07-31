package controllers

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"

	configv1 "github.com/openshift/api/config/v1"
	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/openshift/library-go/pkg/operator/encryption/crypto"
	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
	"github.com/openshift/library-go/pkg/operator/encryption/secrets"
	"github.com/openshift/library-go/pkg/operator/encryption/state"
	operatorv1helpers "github.com/openshift/library-go/pkg/operator/v1helpers"
)

type encryptionKeyPlan struct {
	needed         bool
	keyID          uint64
	reasons        []string
	internalReason string
}

const preflightKMSSocketEndpoint = "unix:///var/run/kmsplugin/kms.sock"

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

func planNextEncryptionKey(
	desiredEncryptionState map[schema.GroupResource]state.GroupResourceState,
	currentMode state.Mode,
	externalReason string,
	encryptedGRs []schema.GroupResource,
	desiredProviderCfg kmsProviderConfig,
) (*encryptionKeyPlan, error) {
	plan := &encryptionKeyPlan{}
	reasons := []string{}

	var (
		commonReason        string
		hasCommonReason     bool
		commonReasonDiffers bool
	)

	for gr, grKeys := range desiredEncryptionState {
		latestKeyID, internalReason, needed, err := needsNewKey(grKeys, currentMode, externalReason, encryptedGRs, desiredProviderCfg)
		if err != nil {
			return nil, err
		}
		if !needed {
			continue
		}

		if !hasCommonReason {
			commonReason = internalReason
			hasCommonReason = true
		} else if commonReason != internalReason {
			commonReasonDiffers = true
		}

		plan.needed = true
		nextKeyID := latestKeyID + 1
		if plan.keyID < nextKeyID {
			plan.keyID = nextKeyID
		}
		reasons = append(reasons, fmt.Sprintf("%s-%s", gr.Resource, internalReason))
	}

	if !plan.needed {
		return plan, nil
	}
	if hasCommonReason && !commonReasonDiffers && len(reasons) > 1 {
		reasons = []string{commonReason}
	}

	sort.Strings(reasons)
	plan.reasons = reasons
	plan.internalReason = strings.Join(reasons, ", ")
	return plan, nil
}

func buildEncryptionKeyState(
	ctx context.Context,
	keyID uint64,
	currentMode state.Mode,
	apiServerEncryption configv1.APIServerEncryption,
	desiredProviderCfg kmsProviderConfig,
	secretClient corev1client.SecretsGetter,
	configMapClient corev1client.ConfigMapsGetter,
	internalReason string,
	externalReason string,
	kmsEndpointOverride string,
) (state.KeyState, error) {
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

	if currentMode != state.KMS {
		return ks, nil
	}

	endpoint := kmsEndpointOverride
	if len(endpoint) == 0 {
		endpoint = fmt.Sprintf(kmsEndpointFormat, keyID)
	}
	ks.KMS = &state.KMSState{
		Encryption: &apiserverv1.KMSConfiguration{
			APIVersion: "v2",
			Name:       fmt.Sprintf("%d", keyID),
			Endpoint:   endpoint,
			Timeout:    &metav1.Duration{Duration: defaultKMSTimeout},
		},
		Plugin: apiServerEncryption.KMS,
	}

	if secretName, expectedKeys, err := desiredProviderCfg.referencedSecretName(); err != nil {
		return state.KeyState{}, err
	} else if len(secretName) > 0 {
		refSecret, err := secretClient.Secrets(openshiftConfigNS).Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			return state.KeyState{}, fmt.Errorf("failed to get secret %s in %s: %w", secretName, openshiftConfigNS, err)
		}
		for _, key := range expectedKeys {
			v, ok := refSecret.Data[key]
			if !ok {
				return state.KeyState{}, fmt.Errorf("secret %s in %s is missing required key %q", secretName, openshiftConfigNS, key)
			}
			if err := ks.KMS.PluginSecretData.Set(secretName, key, v); err != nil {
				return state.KeyState{}, err
			}
		}
	}

	if cmName, expectedKeys, err := desiredProviderCfg.referencedConfigMapName(); err != nil {
		return state.KeyState{}, err
	} else if len(cmName) > 0 {
		refCM, err := configMapClient.ConfigMaps(openshiftConfigNS).Get(ctx, cmName, metav1.GetOptions{})
		if err != nil {
			return state.KeyState{}, fmt.Errorf("failed to get configmap %s in %s: %w", cmName, openshiftConfigNS, err)
		}
		for _, key := range expectedKeys {
			v, ok := refCM.Data[key]
			if !ok {
				return state.KeyState{}, fmt.Errorf("configmap %s in %s is missing required key %q", cmName, openshiftConfigNS, key)
			}
			if err := ks.KMS.PluginConfigMapData.Set(cmName, key, []byte(v)); err != nil {
				return state.KeyState{}, err
			}
		}
	}

	return ks, nil
}

func buildEncryptionKeySecret(
	ctx context.Context,
	instanceName string,
	keyID uint64,
	currentMode state.Mode,
	apiServerEncryption configv1.APIServerEncryption,
	desiredProviderCfg kmsProviderConfig,
	secretClient corev1client.SecretsGetter,
	configMapClient corev1client.ConfigMapsGetter,
	internalReason string,
	externalReason string,
	kmsEndpointOverride string,
) (*corev1.Secret, error) {
	ks, err := buildEncryptionKeyState(
		ctx,
		keyID,
		currentMode,
		apiServerEncryption,
		desiredProviderCfg,
		secretClient,
		configMapClient,
		internalReason,
		externalReason,
		kmsEndpointOverride,
	)
	if err != nil {
		return nil, err
	}
	return secrets.FromKeyState(instanceName, ks)
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
		return 0, fmt.Errorf("no encryption key secrets found")
	}
	return latestKeyID, nil
}

// rewriteWriteKeyKMSEndpoint rewrites KMS providers for the given write-key ID from
// per-key production sockets to endpoint. Used when preflight reuses an existing
// write key (no new key planned) so the checker can dial the fixed preflight socket.
func rewriteWriteKeyKMSEndpoint(secret *corev1.Secret, keyID uint64, endpoint string) (*corev1.Secret, error) {
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
			nameKeyID, _, ok := strings.Cut(kms.Name, "_")
			if !ok || nameKeyID != wantKeyID {
				continue
			}
			kms.Endpoint = endpoint
			rewrote = true
		}
	}
	if !rewrote {
		return nil, fmt.Errorf("write-key KMS provider with key ID %s not found in encryption config", wantKeyID)
	}
	return encryptiondata.ToSecret(secret.Namespace, secret.Name, cfg)
}
