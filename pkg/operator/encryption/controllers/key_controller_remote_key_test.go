package controllers

import (
	"testing"
	"time"

	"github.com/openshift/library-go/pkg/operator/encryption/state"
)

func TestRecordRemoteKeyConvergence(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	rk := state.RemoteKeyState{TargetRemoteKeyID: "remote-old", MigratedRemoteKeyID: "remote-old"}

	got, changed := recordRemoteKeyConvergence(rk, "remote-new", now)
	if !changed {
		t.Fatal("expected change when recording a new candidate")
	}
	if got.ConvergedID != "remote-new" || !got.ConvergedAt.Equal(now) {
		t.Fatalf("unexpected convergence: %#v", got)
	}

	again, changed := recordRemoteKeyConvergence(got, "remote-new", now.Add(time.Minute))
	if changed {
		t.Fatal("expected no change when candidate already recorded")
	}
	if !again.ConvergedAt.Equal(now) {
		t.Fatal("expected converged-at to remain unchanged")
	}
}

func TestClearRemoteKeyConvergence(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	rk := state.RemoteKeyState{
		TargetRemoteKeyID:   "remote-new",
		MigratedRemoteKeyID: "remote-old",
		ConvergedID:         "remote-new",
		ConvergedAt:         now,
	}

	got, changed := clearRemoteKeyConvergence(rk)
	if !changed {
		t.Fatal("expected change when clearing convergence")
	}
	if got.ConvergedID != "" || !got.ConvergedAt.IsZero() {
		t.Fatalf("expected convergence cleared, got %#v", got)
	}
	if got.TargetRemoteKeyID != "remote-new" {
		t.Fatal("expected target to be preserved")
	}

	if _, changed := clearRemoteKeyConvergence(got); changed {
		t.Fatal("expected no change when already clear")
	}
}
