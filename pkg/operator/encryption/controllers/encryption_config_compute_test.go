package controllers

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserverconfigv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/client-go/kubernetes/fake"

	configv1 "github.com/openshift/api/config/v1"
	operatorv1 "github.com/openshift/api/operator/v1"
	configv1clientfake "github.com/openshift/client-go/config/clientset/versioned/fake"

	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
	"github.com/openshift/library-go/pkg/operator/encryption/secrets"
	"github.com/openshift/library-go/pkg/operator/encryption/state"
	"github.com/openshift/library-go/pkg/operator/encryption/statemachine"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

// TestComputeDesiredEncryptionConfigDriftGuard documents the anti-drift contract:
// preflight's IncludePlannedKey compute (without endpoint override) must match what
// the state controller would Apply after the planned key Secret is persisted.
func TestComputeDesiredEncryptionConfigDriftGuard(t *testing.T) {
	apiServerWithKMS := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: configv1.APIServerSpec{
			Encryption: configv1.APIServerEncryption{
				Type: "KMS",
				KMS: configv1.KMSPluginConfig{
					Type:  configv1.VaultKMSProvider,
					Vault: wellKnownBaseVaultConfig,
				},
			},
		},
	}
	encryptedGRs := []schema.GroupResource{{Group: "", Resource: "secrets"}}
	instanceName := "test"

	newExistingKeySecret := func(t *testing.T, keyID string) *corev1.Secret {
		t.Helper()
		oldPlugin := apiServerWithKMS.Spec.Encryption.KMS
		oldPlugin.Vault.VaultKeyPath = "transit/keys/old-key"
		ks := state.KeyState{
			Key:  apiserverconfigv1.Key{Name: keyID, Secret: base64.StdEncoding.EncodeToString(make([]byte, 16))},
			Mode: state.KMS,
			Migrated: state.MigrationState{
				Resources: encryptedGRs,
			},
			KMS: &state.KMSState{
				Encryption: &apiserverconfigv1.KMSConfiguration{
					APIVersion: "v2",
					Name:       keyID,
					Endpoint:   fmt.Sprintf("unix:///var/run/kmsplugin/kms-%s.sock", keyID),
					Timeout:    &metav1.Duration{Duration: 10 * time.Second},
				},
				Plugin: oldPlugin,
			},
		}
		if err := ks.KMS.PluginSecretData.Set("vault-approle", "role-id", []byte("old-role-id")); err != nil {
			t.Fatalf("failed to set plugin secret data: %v", err)
		}
		if err := ks.KMS.PluginSecretData.Set("vault-approle", "secret-id", []byte("old-secret-id")); err != nil {
			t.Fatalf("failed to set plugin secret data: %v", err)
		}
		if err := ks.KMS.PluginConfigMapData.Set("vault-ca-bundle", "ca-bundle.crt", []byte("old-ca-cert")); err != nil {
			t.Fatalf("failed to set plugin configmap data: %v", err)
		}
		s, err := secrets.FromKeyState(instanceName, ks)
		if err != nil {
			t.Fatalf("failed to build existing key secret: %v", err)
		}
		return s
	}

	newDeployedEncryptionConfig := func(t *testing.T, keySecrets ...*corev1.Secret) *corev1.Secret {
		t.Helper()
		desired := statemachine.GetDesiredEncryptionState(nil, keySecrets, encryptedGRs)
		cfg, err := encryptiondata.FromEncryptionState(desired)
		if err != nil {
			t.Fatalf("failed to build intermediate encryption config: %v", err)
		}
		desired = statemachine.GetDesiredEncryptionState(cfg, keySecrets, encryptedGRs)
		cfg, err = encryptiondata.FromEncryptionState(desired)
		if err != nil {
			t.Fatalf("failed to build deployed encryption config: %v", err)
		}
		secret, err := encryptiondata.ToSecret("openshift-config-managed", "encryption-config-"+instanceName, cfg)
		if err != nil {
			t.Fatalf("failed to serialize deployed encryption config: %v", err)
		}
		return secret
	}

	existingKey := newExistingKeySecret(t, "3")
	deployed := newDeployedEncryptionConfig(t, existingKey)
	coreObjects := []runtime.Object{&wellKnownBaseSecret, &wellKnownBaseConfigMap, existingKey}
	fakeKubeClient := fake.NewSimpleClientset(coreObjects...)
	fakeConfigClient := configv1clientfake.NewSimpleClientset(apiServerWithKMS)
	fakeOperatorClient := v1helpers.NewFakeStaticPodOperatorClient(
		&operatorv1.StaticPodOperatorSpec{OperatorSpec: operatorv1.OperatorSpec{ManagementState: operatorv1.Managed}},
		&operatorv1.StaticPodOperatorStatus{},
		nil,
		nil,
	)
	deployer := &fakeEncryptionDeployer{converged: true, secret: deployed}
	secretSelector := metav1.ListOptions{}

	// Preflight/key path: include planned key, production endpoints (no override).
	withPlanned, err := ComputeDesiredEncryptionConfig(
		context.TODO(),
		instanceName,
		nil,
		encryptedGRs,
		deployer,
		fakeKubeClient.CoreV1(),
		fakeKubeClient.CoreV1(),
		fakeConfigClient.ConfigV1().APIServers(),
		fakeOperatorClient,
		secretSelector,
		ComputeDesiredEncryptionConfigOptions{IncludePlannedKey: true},
	)
	if err != nil {
		t.Fatalf("IncludePlannedKey compute failed: %v", err)
	}
	if withPlanned.PlannedKey == nil {
		t.Fatal("expected a planned key for provider change")
	}
	if withPlanned.EncryptionSecret == nil {
		t.Fatal("expected encryption secret from IncludePlannedKey compute")
	}

	// Persist the planned key, then recompute as the state controller would.
	if _, err := fakeKubeClient.CoreV1().Secrets("openshift-config-managed").Create(context.TODO(), withPlanned.PlannedKey.Secret, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to persist planned key: %v", err)
	}
	asState, err := ComputeDesiredEncryptionConfig(
		context.TODO(),
		instanceName,
		nil,
		encryptedGRs,
		deployer,
		fakeKubeClient.CoreV1(),
		nil,
		nil,
		nil,
		secretSelector,
		ComputeDesiredEncryptionConfigOptions{IncludePlannedKey: false},
	)
	if err != nil {
		t.Fatalf("state-style compute failed: %v", err)
	}
	if asState.EncryptionSecret == nil {
		t.Fatal("expected encryption secret from state-style compute")
	}

	if !equality.Semantic.DeepEqual(withPlanned.EncryptionSecret.Data, asState.EncryptionSecret.Data) {
		t.Errorf("drift: IncludePlannedKey encryption-config data diverges from state compute after key Create")
	}
}

func TestComputeDesiredEncryptionConfigFirstKey(t *testing.T) {
	apiServerWithKMS := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: configv1.APIServerSpec{
			Encryption: configv1.APIServerEncryption{
				Type: "KMS",
				KMS: configv1.KMSPluginConfig{
					Type:  configv1.VaultKMSProvider,
					Vault: wellKnownBaseVaultConfig,
				},
			},
		},
	}
	encryptedGRs := []schema.GroupResource{{Group: "", Resource: "secrets"}}
	fakeKubeClient := fake.NewSimpleClientset(&wellKnownBaseSecret, &wellKnownBaseConfigMap)
	fakeConfigClient := configv1clientfake.NewSimpleClientset(apiServerWithKMS)
	fakeOperatorClient := v1helpers.NewFakeStaticPodOperatorClient(
		&operatorv1.StaticPodOperatorSpec{OperatorSpec: operatorv1.OperatorSpec{ManagementState: operatorv1.Managed}},
		&operatorv1.StaticPodOperatorStatus{},
		nil,
		nil,
	)

	result, err := ComputeDesiredEncryptionConfig(
		context.TODO(),
		"test",
		nil,
		encryptedGRs,
		&fakeEncryptionDeployer{converged: true},
		fakeKubeClient.CoreV1(),
		fakeKubeClient.CoreV1(),
		fakeConfigClient.ConfigV1().APIServers(),
		fakeOperatorClient,
		metav1.ListOptions{},
		ComputeDesiredEncryptionConfigOptions{
			IncludePlannedKey:   true,
			KMSEndpointOverride: "unix:///var/run/kmsplugin/kms.sock",
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ProgressingReason != "" {
		t.Fatalf("unexpected progressing: %s", result.ProgressingReason)
	}
	if result.PlannedKey == nil || result.PlannedKey.KeyID != 1 {
		t.Fatalf("expected planned key ID 1, got %+v", result.PlannedKey)
	}
	if result.EncryptionSecret == nil {
		t.Fatal("expected encryption secret")
	}
	cfg, err := encryptiondata.FromSecret(result.EncryptionSecret)
	if err != nil {
		t.Fatalf("failed to parse secret: %v", err)
	}
	kmsConfigs, err := encryptiondata.ExtractUniqueAndSortedKMSConfigurations(cfg)
	if err != nil {
		t.Fatalf("failed to extract KMS configs: %v", err)
	}
	if len(kmsConfigs) != 1 || kmsConfigs[0].Name != "1" {
		t.Fatalf("expected key ID 1, got %+v", kmsConfigs)
	}
	if kmsConfigs[0].Endpoint != "unix:///var/run/kmsplugin/kms.sock" {
		t.Errorf("expected preflight endpoint override, got %s", kmsConfigs[0].Endpoint)
	}
}
