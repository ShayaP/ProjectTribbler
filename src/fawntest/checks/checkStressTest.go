package checks

import (
	"fawn/interfaces"
	"runtime/debug"
	"testing"
	"time"
	"util"
)

func CheckStressTest(t *testing.T, client0, client1, client2, client3 interfaces.ClientInterface,
	back0, back1, back2, back3, back4, back5 interfaces.BackendInterface,
	manager0, manager1, manager2 interfaces.ManagerInterface) {
	ne := func(e error) {
		if e != nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}

	as := func(cond bool) {
		if !cond {
			debug.PrintStack()
			t.Fatal("assertion failed")
		}
	}

	var ret util.ResponseId
	var succ bool

	t.Logf("\nCalling Put with Key alice")
	ne(client0.Put(util.KVIR("alice", "test", 0, util.R), &ret))

	t.Logf("\nCalling Put with Key rick")
	ne(client0.Put(util.KVIR("rick", "rick=test", 0, util.R), &ret))

	// Kill Manager first
	t.Logf("\nKilling manager 0")
	ne(manager0.Crash(20, &succ))
	time.Sleep(5 * time.Second)

	// Kill frontend next
	t.Logf("\nKilling frontend 3")
	ne(client3.Crash(20, &succ))
	time.Sleep(5 * time.Second)

	// Kill backend next
	t.Logf("\nKilling backend 0")
	ne(back0.Crash(20, &succ))
	time.Sleep(5 * time.Second)

	// At this point since back0 is down and client 3 is down. rick needs a new frontend and a new backend
	ne(client0.Get(util.KVIR("rick", "", 0, util.R), &ret))
	as(ret.Response == "rick=test")
}
