package controllers

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiserverconfigv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"

	"github.com/openshift/library-go/pkg/operator/encryption/encryptiondata"
)

func TestRewritePreflightKeyEndpoint(t *testing.T) {
	timeout := &metav1.Duration{Duration: 10 * time.Second}

	scenarios := []struct {
		name          string
		keyID         uint64
		providers     []apiserverconfigv1.ProviderConfiguration
		wantEndpoint  string
		expectedError string
	}{
		{
			name:  "rewrites candidate key endpoint from kms-1.sock to kms.sock",
			keyID: 1,
			providers: []apiserverconfigv1.ProviderConfiguration{
				{Identity: &apiserverconfigv1.IdentityConfiguration{}},
				{KMS: &apiserverconfigv1.KMSConfiguration{
					APIVersion: "v2",
					Name:       "1_secrets",
					Endpoint:   "unix:///var/run/kmsplugin/kms-1.sock",
					Timeout:    timeout,
				}},
			},
			wantEndpoint: preflightKMSSocketEndpoint,
		},
		{
			name:  "rewrites only the matching key ID when multiple KMS providers exist",
			keyID: 8,
			providers: []apiserverconfigv1.ProviderConfiguration{
				{KMS: &apiserverconfigv1.KMSConfiguration{
					APIVersion: "v2",
					Name:       "7_secrets",
					Endpoint:   "unix:///var/run/kmsplugin/kms-7.sock",
					Timeout:    timeout,
				}},
				{KMS: &apiserverconfigv1.KMSConfiguration{
					APIVersion: "v2",
					Name:       "8_secrets",
					Endpoint:   "unix:///var/run/kmsplugin/kms-8.sock",
					Timeout:    timeout,
				}},
			},
			wantEndpoint: preflightKMSSocketEndpoint,
		},
		{
			name:  "errors when no KMS provider matches the key ID",
			keyID: 2,
			providers: []apiserverconfigv1.ProviderConfiguration{
				{Identity: &apiserverconfigv1.IdentityConfiguration{}},
				{KMS: &apiserverconfigv1.KMSConfiguration{
					APIVersion: "v2",
					Name:       "1_secrets",
					Endpoint:   "unix:///var/run/kmsplugin/kms-1.sock",
					Timeout:    timeout,
				}},
			},
			expectedError: `KMS provider with name prefix "2_" not found`,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			in, err := encryptiondata.ToSecret("openshift-config-managed", "encryption-config-test", &encryptiondata.Config{
				Encryption: &apiserverconfigv1.EncryptionConfiguration{
					TypeMeta: metav1.TypeMeta{
						Kind:       "EncryptionConfiguration",
						APIVersion: "apiserver.config.k8s.io/v1",
					},
					Resources: []apiserverconfigv1.ResourceConfiguration{{
						Resources: []string{"secrets"},
						Providers: scenario.providers,
					}},
				},
			})
			if err != nil {
				t.Fatalf("ToSecret: %v", err)
			}

			out, err := rewritePreflightKeyEndpoint(in, scenario.keyID)
			if scenario.expectedError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", scenario.expectedError)
				}
				if !strings.Contains(err.Error(), scenario.expectedError) {
					t.Fatalf("expected error containing %q, got %v", scenario.expectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			cfg, err := encryptiondata.FromSecret(out)
			if err != nil {
				t.Fatalf("FromSecret: %v", err)
			}

			wantPrefix := fmt.Sprintf("%d_", scenario.keyID)
			foundMatching := false
			for _, provider := range cfg.Encryption.Resources[0].Providers {
				if provider.KMS == nil {
					continue
				}
				if strings.HasPrefix(provider.KMS.Name, wantPrefix) {
					foundMatching = true
					if provider.KMS.Endpoint != scenario.wantEndpoint {
						t.Errorf("matching provider %q endpoint: got %q, want %q", provider.KMS.Name, provider.KMS.Endpoint, scenario.wantEndpoint)
					}
					continue
				}
				// Non-matching providers must keep their original per-key socket.
				if provider.KMS.Endpoint == preflightKMSSocketEndpoint {
					t.Errorf("non-matching provider %q was unexpectedly rewritten to %q", provider.KMS.Name, provider.KMS.Endpoint)
				}
			}
			if !foundMatching {
				t.Fatalf("did not find rewritten provider for key ID %d", scenario.keyID)
			}

			if out.Namespace != in.Namespace || out.Name != in.Name {
				t.Errorf("secret identity changed: got %s/%s, want %s/%s", out.Namespace, out.Name, in.Namespace, in.Name)
			}
			if _, ok := out.Data["encryption-config"]; !ok {
				t.Fatal("expected encryption-config data key")
			}
		})
	}
}
