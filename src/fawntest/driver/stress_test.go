package driver

import (
	"fawn/implementation"
	"fawntest/checks"
	"testing"
	"time"
	"util"
)

func TestStressTest(t *testing.T) {
	backAddr0 := "localhost:30180"
	backAddr1 := "localhost:30263"
	backAddr2 := "localhost:30984"
	backAddr3 := "localhost:31999"
	backAddr4 := "localhost:32098"
	backAddr5 := "localhost:34908"

	frontAddr0 := util.Local()
	frontAddr1 := util.Local()
	frontAddr2 := util.Local()
	frontAddr3 := util.Local()

	readyBack0 := make(chan bool)
	readyBack1 := make(chan bool)
	readyBack2 := make(chan bool)
	readyBack3 := make(chan bool)
	readyBack4 := make(chan bool)
	readyBack5 := make(chan bool)

	readyFront0 := make(chan bool)
	readyFront1 := make(chan bool)
	readyFront2 := make(chan bool)
	readyFront3 := make(chan bool)

	V := 1
	backs := []string{backAddr0, backAddr1, backAddr2, backAddr3, backAddr4, backAddr5}

	back0 := implementation.NewBackend(backAddr0, backs, util.R, V)
	back1 := implementation.NewBackend(backAddr1, backs, util.R, V)
	back2 := implementation.NewBackend(backAddr2, backs, util.R, V)
	back3 := implementation.NewBackend(backAddr3, backs, util.R, V)
	back4 := implementation.NewBackend(backAddr4, backs, util.R, V)
	back5 := implementation.NewBackend(backAddr5, backs, util.R, V)

	go func() {
		e := implementation.ServeBackend(backAddr0, V, readyBack0, back0)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r := <-readyBack0
	if !r {
		t.Fatal("not ready")
	}

	go func() {
		e := implementation.ServeBackend(backAddr1, V, readyBack1, back1)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyBack1
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

	go func() {
		e := implementation.ServeBackend(backAddr4, V, readyBack4, back4)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyBack4
	if !r {
		t.Fatal("not ready")
	}

	go func() {
		e := implementation.ServeBackend(backAddr5, V, readyBack5, back5)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyBack5
	if !r {
		t.Fatal("not ready")
	}

	client0 := implementation.NewClient(frontAddr0, backs)
	client1 := implementation.NewClient(frontAddr1, backs)
	client2 := implementation.NewClient(frontAddr2, backs)
	client3 := implementation.NewClient(frontAddr3, backs)

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

	// Serve the Frontends:
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

	// Serve the Frontends:
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

	// Serve the Frontends:
	go func() {
		e := implementation.ServeFrontend(frontAddr3, readyFront3, client3)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyFront3
	if !r {
		t.Fatal("not ready")
	}

	fronts := []string{frontAddr0, frontAddr1, frontAddr2, frontAddr3}
	readyManager0 := make(chan bool)
	readyManager1 := make(chan bool)
	readyManager2 := make(chan bool)

	managerAddr0 := util.Local()
	managerAddr1 := util.Local()
	managerAddr2 := util.Local()

	managers := []string{managerAddr0, managerAddr1, managerAddr2}

	manager0 := implementation.NewManager(0, managerAddr0, managers, len(managers), fronts)
	manager1 := implementation.NewManager(1, managerAddr1, managers, len(managers), fronts)
	manager2 := implementation.NewManager(2, managerAddr2, managers, len(managers), fronts)

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

	// Serve the Manager:
	go func() {
		e := implementation.ServeManager(readyManager2, manager2)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyManager2
	if !r {
		t.Fatal("not ready")
	}

	t.Logf("Done serving manager")
	time.Sleep(5 * time.Second)

	checks.CheckStressTest(t, client0, client1, client2, client3, back0, back2, back2, back3, back4, back5, manager0, manager1, manager2)
}
