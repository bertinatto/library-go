package encryption

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	configv1 "github.com/openshift/api/config/v1"
	operatorv1 "github.com/openshift/api/operator/v1"
)

func TestKMSLifecycleGroupStepOrder(t *testing.T) {
	ctx := context.Background()
	lifecycle := []KMSPreflightLifecycleScenario{
		fakeLifecycleScenario("SecretOfLife", "openshift-kube-apiserver-operator",
			"invalid-a", "invalid-b"),
	}
	rotation := []RotationScenario{
		fakeRotationScenario("SecretOfLife", "openshift-kube-apiserver-operator", true),
	}

	names := stepNames(kmsLifecycleGroupTestSteps(ctx, t, lifecycle, rotation, nil))

	mustContainInOrder(t, names,
		"CreateAndStoreSecretOfLife",
		"Apply invalid-a",
		"RevertToIdentitySecretOfLife",
		"Apply invalid-b",
		"ApplyValidKMSConfig",
		"RecoverKMSEncryptionSecretOfLife",
		"MarkValidKMSReached",
		"ApplyInPlaceUpdate[0]",
		"ApplyInPlaceUpdate[1]",
		"ContinueEncryptedSecretOfLife",
		"ForceKeyRotation",
		"AssertSecretOfLifeReencrypted",
	)

	if containsName(names, "EncryptWithKMSSecretOfLife") {
		t.Fatalf("grouped rotation must not re-enable encryption, found EncryptWith in %#v", names)
	}
	if containsName(names, "FinalIdentity") || containsName(names, "OffIdentity") {
		t.Fatalf("grouped lifecycle must leave KMS enabled; OnOff owns identity coverage, got %#v", names)
	}
	if idx := indexContaining(names, "RevertToIdentitySecretOfLife"); idx >= 0 {
		// Intermediate identity between invalids is expected; a revert after updates is not.
		afterUpdates := indexContaining(names, "ApplyInPlaceUpdate[1]")
		if afterUpdates >= 0 && idx > afterUpdates {
			t.Fatalf("unexpected identity reset after updates at step %q", names[idx])
		}
	}
	for i, name := range names {
		if strings.HasPrefix(name, "RevertToIdentity") {
			forceIdx := indexContaining(names, "ForceKeyRotation")
			if forceIdx >= 0 && i > forceIdx {
				t.Fatalf("grouped rotation must not append RevertToIdentity after ForceKeyRotation; got %q", name)
			}
		}
	}
}

func TestKMSLifecycleStandaloneStillEndsInIdentity(t *testing.T) {
	ctx := context.Background()
	lifecycle := []KMSPreflightLifecycleScenario{
		fakeLifecycleScenario("SecretOfLife", "openshift-kube-apiserver-operator", "invalid-a"),
	}
	steps, cleanup := kmsPreflightLifecycleTestSteps(ctx, t, kmsPreflightLifecycleOptions{includeFinalIdentity: true}, lifecycle...)
	names := stepNames(steps)
	mustContainInOrder(t, names,
		"ApplyValidKMSConfig",
		"ApplyInPlaceUpdate[0]",
		"RevertToIdentitySecretOfLife",
	)
	if len(cleanup) == 0 {
		t.Fatal("standalone lifecycle must register identity cleanup steps")
	}
}

func TestRotationContinuationOmitsCreateEncryptAndIdentity(t *testing.T) {
	ctx := context.Background()
	standalone := stepNames(rotationTestSteps(ctx, t, rotationStepOptions{
		includeCreateEncrypt: true,
		includeFinalIdentity: true,
	}, fakeRotationScenario("SecretOfLife", "openshift-kube-apiserver-operator", true)))
	mustContainInOrder(t, standalone,
		"CreateAndStoreSecretOfLife",
		"EncryptWithKMSSecretOfLife",
		"ForceKeyRotation",
		"RevertToIdentitySecretOfLife",
	)

	continued := stepNames(rotationTestSteps(ctx, t, rotationStepOptions{
		includeCreateEncrypt: false,
		includeFinalIdentity: false,
	}, fakeRotationScenario("SecretOfLife", "openshift-kube-apiserver-operator", true)))
	mustContainInOrder(t, continued,
		"ContinueEncryptedSecretOfLife",
		"ForceKeyRotation",
		"AssertSecretOfLifeReencrypted",
	)
	for _, name := range continued {
		if strings.HasPrefix(name, "CreateAndStore") || strings.HasPrefix(name, "EncryptWith") || strings.HasPrefix(name, "RevertToIdentity") {
			t.Fatalf("continuation steps must omit create/encrypt/identity, found %q in %#v", name, continued)
		}
	}
}

func TestMigrationGroupStepOrder(t *testing.T) {
	ctx := context.Background()
	providerA := EncryptionProvider{APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS}}
	providerB := EncryptionProvider{APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeAESCBC}}
	migration := providersMigrationTestSteps(ctx, t, providersMigrationOptions{includeFinalIdentity: false}, ProvidersMigrationScenario{
		BasicScenario:                  fakeBasicScenario("SecretOfLife", "openshift-kube-apiserver-operator"),
		ResourceName:                   "SecretOfLife",
		EncryptionProviders:            []EncryptionProvider{providerA, providerB},
		CreateResourceFunc:             func(testing.TB, ClientSet, string) runtime.Object { return nil },
		AssertResourceEncryptedFunc:    func(testing.TB, ClientSet, runtime.Object) {},
		AssertResourceNotEncryptedFunc: func(testing.TB, ClientSet, runtime.Object) {},
		ResourceFunc:                   func(testing.TB, string) runtime.Object { return nil },
	})
	names := append([]string{"PreflightDeploy"}, stepNames(migration)...)
	mustContainInOrder(t, names,
		"PreflightDeploy",
		"CreateAndStoreSecretOfLife",
		"EncryptWithKMSSecretOfLife",
		"MigrateToAESCBCSecretOfLife",
	)
	if containsName(names, "OffIdentity") {
		t.Fatalf("grouped migration must leave KMS enabled; OnOff owns identity coverage, got %#v", names)
	}
}

func TestProvidersMigrationBothDirections(t *testing.T) {
	ctx := context.Background()
	providerA := EncryptionProvider{APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS}}
	providerB := EncryptionProvider{APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeAESCBC}}

	for _, providers := range [][]EncryptionProvider{
		{providerA, providerB},
		{providerB, providerA},
	} {
		scenario := ProvidersMigrationScenario{
			BasicScenario:                  fakeBasicScenario("SecretOfLife", "openshift-kube-apiserver-operator"),
			ResourceName:                   "SecretOfLife",
			EncryptionProviders:            providers,
			CreateResourceFunc:             func(testing.TB, ClientSet, string) runtime.Object { return nil },
			AssertResourceEncryptedFunc:    func(testing.TB, ClientSet, runtime.Object) {},
			AssertResourceNotEncryptedFunc: func(testing.TB, ClientSet, runtime.Object) {},
			ResourceFunc:                   func(testing.TB, string) runtime.Object { return nil },
		}
		standalone := stepNames(providersMigrationTestSteps(ctx, t, providersMigrationOptions{includeFinalIdentity: true}, scenario))
		grouped := stepNames(providersMigrationTestSteps(ctx, t, providersMigrationOptions{includeFinalIdentity: false}, scenario))
		first := strings.ToUpper(string(providers[0].Type))
		second := strings.ToUpper(string(providers[1].Type))
		mustContainInOrder(t, standalone,
			"CreateAndStoreSecretOfLife",
			"EncryptWith"+first+"SecretOfLife",
			"MigrateTo"+second+"SecretOfLife",
			"OffIdentityAndAssertSecretOfLifeNotEncrypted",
		)
		mustContainInOrder(t, grouped,
			"CreateAndStoreSecretOfLife",
			"EncryptWith"+first+"SecretOfLife",
			"MigrateTo"+second+"SecretOfLife",
		)
		if containsName(grouped, "OffIdentity") {
			t.Fatalf("grouped migration must omit terminal identity, got %#v", grouped)
		}
	}
}

func TestRunTestStepsStopsOnFailure(t *testing.T) {
	var completed atomic.Int32
	rec := &recordingTB{T: t}
	runTestSteps(rec, []testStep{
		{name: "ok", testFunc: func(t testing.TB) { completed.Add(1) }},
		{name: "fail", testFunc: func(t testing.TB) {
			completed.Add(1)
			t.Errorf("boom")
		}},
		{name: "skipped", testFunc: func(t testing.TB) { completed.Add(1) }},
	})
	if !rec.Failed() {
		t.Fatal("expected failure")
	}
	if got := completed.Load(); got != 2 {
		t.Fatalf("expected 2 completed steps before stop, got %d", got)
	}
}

func fakeBasicScenario(resourceName, operatorNamespace string) BasicScenario {
	return BasicScenario{
		Namespace:                       "openshift-config-managed",
		LabelSelector:                   "encryption.apiserver.operator.openshift.io/component=test",
		EncryptionConfigSecretName:      "encryption-config-test",
		EncryptionConfigSecretNamespace: "openshift-config-managed",
		OperatorNamespace:               operatorNamespace,
		TargetGRs:                       []schema.GroupResource{{Group: "", Resource: "secrets"}},
		AssertFunc:                      func(testing.TB, ClientSet, configv1.EncryptionType, string, string) {},
	}
}

func fakeLifecycleScenario(resourceName, operatorNamespace string, invalidNames ...string) KMSPreflightLifecycleScenario {
	invalids := make([]KMSPreflightNegativeScenario, 0, len(invalidNames))
	for _, name := range invalidNames {
		// Single-scenario fakes own the cluster-wide InvalidProvider for every batch,
		// matching the real KAS lifecycle scenario.
		invalids = append(invalids, KMSPreflightNegativeScenario{
			Name: name,
			InvalidProvider: EncryptionProvider{
				APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS},
			},
			AssertFailedFunc: func(context.Context, testing.TB, ClientSet, string, operatorv1.KMSPreflightCheck) {
			},
		})
	}
	return KMSPreflightLifecycleScenario{
		InPlaceUpdateScenario: InPlaceUpdateScenario{
			BasicScenario:               fakeBasicScenario(resourceName, operatorNamespace),
			ResourceName:                resourceName,
			CreateResourceFunc:          func(testing.TB, ClientSet, string) runtime.Object { return nil },
			AssertResourceEncryptedFunc: func(testing.TB, ClientSet, runtime.Object) {},
			ResourceFunc:                func(testing.TB, string) runtime.Object { return nil },
			EncryptionProvider: EncryptionProvider{
				APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS},
			},
			UpdatedEncryptionProviders: []EncryptionProvider{
				{APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS}},
				{APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS}},
			},
			AssertInPlaceUpdateFunc: func(testing.TB, ClientSet, EncryptionKeyMeta) {},
		},
		InvalidConfigs: invalids,
	}
}

func fakeRotationScenario(resourceName, operatorNamespace string, withProvider bool) RotationScenario {
	s := RotationScenario{
		BasicScenario:      fakeBasicScenario(resourceName, operatorNamespace),
		ResourceName:       resourceName,
		CreateResourceFunc: func(testing.TB, ClientSet, string) runtime.Object { return nil },
		GetRawResourceFunc: func(testing.TB, ClientSet, string) string { return "cipher" },
		ForceRotationFunc:  func(testing.TB, context.Context) {},
		WaitForRotationCompleteFunc: func(testing.TB, ClientSet, EncryptionKeyMeta, BasicScenario) {
		},
	}
	if withProvider {
		s.EncryptionProvider = EncryptionProvider{
			APIServerEncryption: configv1.APIServerEncryption{Type: configv1.EncryptionTypeKMS},
		}
	}
	return s
}

func stepNames(steps []testStep) []string {
	names := make([]string, 0, len(steps))
	for _, step := range steps {
		names = append(names, step.name)
	}
	return names
}

func mustContainInOrder(t *testing.T, names []string, want ...string) {
	t.Helper()
	pos := 0
	for _, w := range want {
		found := -1
		for i := pos; i < len(names); i++ {
			if names[i] == w || strings.Contains(names[i], w) {
				found = i
				break
			}
		}
		if found < 0 {
			t.Fatalf("expected %q after position %d in %#v", w, pos, names)
		}
		pos = found + 1
	}
}

func containsName(names []string, want string) bool {
	return indexContaining(names, want) >= 0
}

func indexContaining(names []string, want string) int {
	for i, name := range names {
		if name == want || strings.Contains(name, want) {
			return i
		}
	}
	return -1
}
