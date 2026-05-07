package plugins

import (
	"fmt"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	"github.com/stretchr/testify/require"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
)

func TestVaultSidecarProvider_BuildSidecarContainer(t *testing.T) {
	tests := []struct {
		name          string
		vaultConfig   *configv1.VaultKMSConfig
		roleID        string
		containerName string
		kmsConfig     *apiserverv1.KMSConfiguration
		wantErr       string
	}{
		{
			name:   "builds container with correct args",
			roleID: "test-role-id",
			vaultConfig: &configv1.VaultKMSConfig{
				KMSPluginImage: "quay.io/test/vault:v2",
				VaultAddress:   "https://vault.example.com:8200",
				VaultNamespace: "my-namespace",
				TransitKey:     "my-key",
				TransitMount:   "transit",
			},
			containerName: "kms-plugin",
			kmsConfig: &apiserverv1.KMSConfiguration{
				APIVersion: "v2",
				Name:       "555_secrets",
				Endpoint:   "unix:///var/run/kmsplugin/kms-555.sock",
			},
		},
		{
			name:          "nil vault config",
			roleID:        "test-role-id",
			vaultConfig:   nil,
			containerName: "kms-plugin",
			kmsConfig:     &apiserverv1.KMSConfiguration{},
			wantErr:       "vault config cannot be nil",
		},
		{
			name: "empty role ID",
			vaultConfig: &configv1.VaultKMSConfig{
				KMSPluginImage: "quay.io/test/vault:v2",
				VaultAddress:   "https://vault.example.com:8200",
			},
			containerName: "kms-plugin",
			kmsConfig: &apiserverv1.KMSConfiguration{
				Endpoint: "unix:///var/run/kmsplugin/kms-555.sock",
			},
			wantErr: "vault role ID cannot be empty",
		},
		{
			name:   "invalid endpoint",
			roleID: "test-role-id",
			vaultConfig: &configv1.VaultKMSConfig{
				KMSPluginImage: "quay.io/test/vault:v2",
				VaultAddress:   "https://vault.example.com:8200",
			},
			containerName: "kms-plugin",
			kmsConfig: &apiserverv1.KMSConfiguration{
				Endpoint: "invalid-endpoint",
			},
			wantErr: "failed to parse key ID from endpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &VaultSidecarProvider{
				Config: tt.vaultConfig,
				RoleID: tt.roleID,
			}

			container, err := provider.BuildSidecarContainer(tt.containerName, tt.kmsConfig)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.containerName, container.Name)
			require.Equal(t, tt.vaultConfig.KMSPluginImage, container.Image)
			require.Equal(t, []string{"/bin/sh", "-c"}, container.Command)

			credentialsFile := "/etc/kubernetes/static-pod-resources/secrets/encryption-config/kms-secret-data-555"
			expectedArgs := fmt.Sprintf(`
	sed -n 's/.*"VAULT_SECRET_ID":"\([^"]*\)".*/\1/p' %s > /tmp/secret-id
	exec /vault-kube-kms \
	-listen-address=%s \
	-vault-address=%s \
	-vault-namespace=%s \
	-transit-mount=%s \
	-transit-key=%s \
	-approle-role-id=%s \
	-approle-secret-id-path=/tmp/secret-id`,
				credentialsFile,
				tt.kmsConfig.Endpoint,
				tt.vaultConfig.VaultAddress,
				tt.vaultConfig.VaultNamespace,
				tt.vaultConfig.TransitMount,
				tt.vaultConfig.TransitKey,
				tt.roleID,
			)
			require.Len(t, container.Args, 1)
			require.Equal(t, expectedArgs, container.Args[0])
		})
	}
}
