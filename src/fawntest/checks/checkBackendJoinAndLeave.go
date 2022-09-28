package checks

import (
	"fawn/interfaces"
	"runtime/debug"
	"testing"
	"time"
	"util"
)

func CheckBackendJoinAndLeave(t *testing.T, client0 interfaces.ClientInterface, back0, back1, back2, back3 interfaces.BackendInterface) {
	ne := func(e error) {
		if e != nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}

	// er := func(e error) {
	// 	if e == nil {
	// 		debug.PrintStack()
	// 		t.Fatal(e)
	// 	}
	// }

	as := func(cond bool) {
		if !cond {
			debug.PrintStack()
			t.Fatal("assertion failed")
		}
	}

	// Key ranges
	// back1 --> localhost:30263 --> 411175356
	// back2 --> localhost:30984 --> 725735882
	// back3 --> localhost:31999 --> 3847622741 -- X
	// back0 --> localhost:30180 --> 4261444134

	// SortedListOfBackendHashIds = [411175356 725735882 4261444134]
	// 														deadNodeIdx

	// back2 --> 725735882
	// alice --> 2267157479
	// back3 --> 3847622741

	// Alice --> hash: 752715143 --> maps to frontend 0 | Keyrange: &{0 2147483647}
	// Bob --> hash: 3955990900 --> maps to frontend 1 | Keyrange: &{2147483647 4294967295}

	var ret util.ResponseId
	t.Logf("\nCalling Get with Key Alice")
	ne(client0.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "")

	t.Logf("\nCalling Put with Key alice")
	ne(client0.Put(util.KVIR("alice", "test", 0, util.R), &ret))

	t.Logf("\nCalling Put with Key alice")
	ne(client0.Put(util.KVIR("rick", "rick=test", 0, util.R), &ret))

	t.Logf("\nCalling Get with Key alice")
	ne(client0.Get(util.KVIR("alice", "", 0, util.R), &ret))
	as(ret.Response == "test")

	var succ bool
	t.Logf("Crashing backend 3")
	ne(back3.Crash(10, &succ))

	time.Sleep(5 * time.Second)

	t.Logf("\nCalling Get with Key alice after crash")
	ne(client0.Get(util.KVIR("alice", "", 0, util.R), &ret))
	as(ret.Response == "test")

	time.Sleep(15 * time.Second)

}
