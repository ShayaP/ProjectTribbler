package driver

import (
	"fawn/implementation"
	"fawntest/checks"
	"testing"
	"time"
	"util"
)

func TestManagerJoinAndLeave(t *testing.T) {
	backAddr1 := util.Local()
	backAddr2 := util.Local()
	backAddr3 := util.Local()
	frontAddr0 := util.Local()
	frontAddr1 := util.Local()
	frontAddr2 := util.Local()

	t.Logf("Frontend 0: %s, Frontend 1: %s, Frontend 2: %s", frontAddr0, frontAddr1, frontAddr2)
	readyBack1 := make(chan bool)
	readyBack2 := make(chan bool)
	readyBack3 := make(chan bool)

	readyFront0 := make(chan bool)
	readyFront1 := make(chan bool)
	readyFront2 := make(chan bool)

	V := 1
	backs := []string{backAddr1, backAddr2, backAddr3}
	back1 := implementation.NewBackend(backAddr1, backs, util.R, V)
	back2 := implementation.NewBackend(backAddr2, backs, util.R, V)
	back3 := implementation.NewBackend(backAddr3, backs, util.R, V)

	go func() {
		e := implementation.ServeBackend(backAddr1, V, readyBack1, back1)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r := <-readyBack1
	if !r {
		t.Fatal("not ready")
	}

	go func() {
		e := implementation.ServeBackend(backAddr2, V, readyBack2, back2)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyBack2
	if !r {
		t.Fatal("not ready")
	}

	go func() {
		e := implementation.ServeBackend(backAddr3, V, readyBack3, back3)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyBack3
	if !r {
		t.Fatal("not ready")
	}

	client0 := implementation.NewClient(frontAddr0, backs)
	client1 := implementation.NewClient(frontAddr1, backs)
	client2 := implementation.NewClient(frontAddr2, backs)

	// Serve the Frontends:
	go func() {
		e := implementation.ServeFrontend(frontAddr0, readyFront0, client0)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyFront0
	if !r {
		t.Fatal("not ready")
	}

	go func() {
		e := implementation.ServeFrontend(frontAddr1, readyFront1, client1)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyFront1
	if !r {
		t.Fatal("not ready")
	}

	go func() {
		e := implementation.ServeFrontend(frontAddr2, readyFront2, client2)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyFront2
	if !r {
		t.Fatal("not ready")
	}

	fronts := []string{frontAddr0, frontAddr1, frontAddr2}
	readyManager0 := make(chan bool)
	readyManager1 := make(chan bool)
	managerAddr0 := util.Local()
	managerAddr1 := util.Local()
	managers := []string{managerAddr0, managerAddr1}
	manager0 := implementation.NewManager(0, managerAddr0, managers, 2, fronts)
	manager1 := implementation.NewManager(1, managerAddr1, managers, 2, fronts)

	// Serve the Manager:
	go func() {
		e := implementation.ServeManager(readyManager0, manager0)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyManager0
	if !r {
		t.Fatal("not ready")
	}

	// Serve the Manager:
	go func() {
		e := implementation.ServeManager(readyManager1, manager1)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyManager1
	if !r {
		t.Fatal("not ready")
	}

	t.Logf("Done serving manager")
	time.Sleep(5 * time.Second)

	checks.CheckManagerJoinAndLeave(t, client0, client1, client2, manager0, manager1)
}
