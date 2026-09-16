package kms

import (
	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion identifies the stored configuration; it is not a served API.
var SchemeGroupVersion = schema.GroupVersion{Group: "encryption.operator.openshift.io", Version: "v1"}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &KMSPluginConfig{})
	return nil
}

// KMSPluginConfig holds resolved plugin configuration.
// Remove this type when the encryption lifecycle uses unstructured configuration.
type KMSPluginConfig struct {
	metav1.TypeMeta `json:",inline"`
	Type            configv1.KMSProviderType      `json:"type"`
	Vault           configv1.VaultKMSPluginConfig `json:"vault,omitempty,omitzero"`
}

// Aliases preserve the exported names while reusing the API definitions.
type KMSProviderType = configv1.KMSProviderType
type VaultSecretReference = configv1.VaultSecretReference
type VaultConfigMapReference = configv1.VaultConfigMapReference
type VaultAuthentication = configv1.VaultAuthentication
type VaultAuthenticationType = configv1.VaultAuthenticationType
type VaultAppRoleAuthentication = configv1.VaultAppRoleAuthentication
type VaultKMSPluginConfig = configv1.VaultKMSPluginConfig
type VaultTLSConfig = configv1.VaultTLSConfig

const (
	VaultKMSProvider               = configv1.VaultKMSProvider
	VaultAuthenticationTypeAppRole = configv1.VaultAuthenticationTypeAppRole
)

func (in *KMSPluginConfig) DeepCopy() *KMSPluginConfig {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func (in *KMSPluginConfig) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}
