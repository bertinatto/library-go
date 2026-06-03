package pluginlifecycle

import (
	"path/filepath"
	"testing"

	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
	"github.com/stretchr/testify/require"
)

func TestCredentialResolver(t *testing.T) {
	var pluginsSecretData encryptiondata.KMSPluginsSecretData
	require.NoError(t, pluginsSecretData.SetFromRawKey("key-1", "vault-approle_role-id", []byte("my-role")))
	require.NoError(t, pluginsSecretData.SetFromRawKey("key-1", "vault-approle_secret-id", []byte("my-secret")))

	tests := []struct {
		name           string
		keyID          string
		credentialsDir string
		secretName     string
		dataKey        string
		wantValue      string
		wantFilePath   string
		wantValueErr   string
		wantFileErr    string
	}{
		{
			name:           "returns credential value and file path",
			keyID:          "key-1",
			credentialsDir: "/etc/kubernetes/static-pod-resources/secrets",
			secretName:     "vault-approle",
			dataKey:        "secret-id",
			wantValue:      "my-secret",
			wantFilePath: filepath.Join("/etc/kubernetes/static-pod-resources/secrets",
				encryptiondata.FormatKMSSecretDataKey("vault-approle_secret-id", "key-1")),
		},
		{
			name:         "error when keyID is missing",
			keyID:        "missing-key",
			secretName:   "vault-approle",
			dataKey:      "role-id",
			wantValueErr: "missing secret data for keyID missing-key",
			wantFileErr:  "missing secret data for keyID missing-key",
		},
		{
			name:         "error when dataKey is missing",
			keyID:        "key-1",
			secretName:   "vault-approle",
			dataKey:      "nonexistent",
			wantValueErr: "missing nonexistent in secret vault-approle for keyID key-1",
			wantFileErr:  "missing nonexistent in secret vault-approle for keyID key-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds := &credentialResolver{
				pluginsSecretData: pluginsSecretData,
				credentialsDir:    tt.credentialsDir,
				keyID:             tt.keyID,
			}

			gotValue, err := creds.Value(tt.secretName, tt.dataKey)
			if tt.wantValueErr != "" {
				require.EqualError(t, err, tt.wantValueErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantValue, gotValue)
			}

			gotPath, err := creds.FilePath(tt.secretName, tt.dataKey)
			if tt.wantFileErr != "" {
				require.EqualError(t, err, tt.wantFileErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantFilePath, gotPath)
			}
		})
	}
}
