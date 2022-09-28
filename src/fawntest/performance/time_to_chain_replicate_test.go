package performance

import (
	"fawn/implementation"
	"fawntest/performance_check"
	"testing"
	"time"
	"util"
)

func TestTimeToChainReplicate(t *testing.T) {
	backAddr1 := util.Local()
	backAddr2 := util.Local()
	backAddr3 := util.Local()
	frontAddr1 := util.Local()

	readyBack1 := make(chan bool)
	readyBack2 := make(chan bool)
	readyBack3 := make(chan bool)
	readyFront1 := make(chan bool)

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

	client1 := implementation.NewClient(frontAddr1, backs)

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

	t.Logf("Done serving the frontend")

	fronts := []string{frontAddr1}

	readyManager := make(chan bool)
	managerAddr := util.Local()
	manager := implementation.NewManager(0, managerAddr, []string{managerAddr}, 1, fronts)

	// Serve the Manager:
	go func() {
		e := implementation.ServeManager(readyManager, manager)
		if e != nil {
			t.Fatal(e)
		}
	}()
	r = <-readyManager
	if !r {
		t.Fatal("not ready")
	}

	t.Logf("Done serving manager")
	time.Sleep(5 * time.Second)

	// Run this experiment for 1000 times.
	total := int64(0)
	num_samples := 100
	for i := 0; i < num_samples; i++ {
		if i%10 == 0 {
			t.Logf("Iteration: %d", i)
		}
		time_nanos := performance_check.CheckTimeToChainReplicate(t, client1)
		total += time_nanos
	}

	// Average the results
	t.Logf("Average for chain replication: %d", total/int64(num_samples))
}
