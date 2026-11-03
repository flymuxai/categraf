package config

import (
	"strings"
	"testing"
)

func TestLinkEnvResolvesReferences(t *testing.T) {
	t.Setenv("CATEGRAF_TEST_KEY", "s3cr3t")
	secret := NewSecret([]byte("Bearer @{env:CATEGRAF_TEST_KEY}"))

	if err := secret.LinkEnv(); err != nil {
		t.Fatalf("LinkEnv() error = %v", err)
	}
	buffer, err := secret.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer buffer.Destroy()
	if got := buffer.String(); got != "Bearer s3cr3t" {
		t.Fatalf("Get() = %q", got)
	}
	// Linking again is a no-op.
	if err := secret.LinkEnv(); err != nil {
		t.Fatalf("second LinkEnv() error = %v", err)
	}
}

func TestLinkEnvLeavesPlainSecretsAlone(t *testing.T) {
	secret := NewSecret([]byte("plain-value"))
	if err := secret.LinkEnv(); err != nil {
		t.Fatalf("LinkEnv() error = %v", err)
	}
	buffer, err := secret.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer buffer.Destroy()
	if got := buffer.String(); got != "plain-value" {
		t.Fatalf("Get() = %q", got)
	}
}

func TestLinkEnvReportsMissingVariable(t *testing.T) {
	secret := NewSecret([]byte("@{env:CATEGRAF_TEST_DEFINITELY_UNSET}"))
	err := secret.LinkEnv()
	if err == nil || !strings.Contains(err.Error(), "CATEGRAF_TEST_DEFINITELY_UNSET") {
		t.Fatalf("LinkEnv() error = %v, want missing variable name", err)
	}
}

func TestLinkEnvRejectsOtherStores(t *testing.T) {
	secret := NewSecret([]byte("@{vault:token}"))
	err := secret.LinkEnv()
	if err == nil || !strings.Contains(err.Error(), `unsupported secret store "vault"`) {
		t.Fatalf("LinkEnv() error = %v, want unsupported store", err)
	}
}
