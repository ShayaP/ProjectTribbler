package checks

import (
	"fawn/interfaces"
	"runtime/debug"
	"testing"
	"time"
	"util"
)

func CheckFrontendJoinAndLeave(t *testing.T, client0, client1, client2 interfaces.ClientInterface) {
	ne := func(e error) {
		if e != nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}

	er := func(e error) {
		if e == nil {
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

	// Alice --> hash: 752715143 --> maps to frontend 0 | Keyrange: &{0 2147483647}
	// Bob --> hash: 3955990900 --> maps to frontend 1 | Keyrange: &{2147483647 4294967295}

	var ret util.ResponseId
	t.Logf("\nCalling Get on Client 0 with Key Alice")
	ne(client0.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "")

	var succ bool
	// Crash the client 0
	t.Logf("\nCrashing client0 for 10 secs")
	ne(client0.Crash(15, &succ))

	// Sleep 3 seconds as a buffer for key range migrations
	time.Sleep(5 * time.Second)

	t.Logf("\nCalling Get on Client 0 with Key Alice")
	er(client0.Get(util.KVIR("Alice", "", 0, util.R), &ret))

	t.Logf("\nCalling Get on Client 1 with Key Alice")
	ne(client1.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "")

	t.Logf("\nCalling Put on Client 1 with Key Alice and Value alice")
	ne(client1.Put(util.KVIR("Alice", "alice", 0, util.R), &ret))

	// Crash the client 1
	t.Logf("\nCrashing client1 for 10 secs")
	ne(client1.Crash(20, &succ))

	// Sleep 3 seconds as a buffer for key range migrations
	time.Sleep(5 * time.Second)

	t.Logf("\nCalling Get on Client 1 with Key Alice")
	er(client1.Get(util.KVIR("Alice", "", 0, util.R), &ret))

	t.Logf("\nCalling Get on Client 2 with Key Alice")
	ne(client2.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "alice")

	// Make sure that client0 is alive again
	time.Sleep(5 * time.Second)

	t.Logf("Calling Get on Client 0 with Key Alice\n")
	ne(client0.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "alice")

	t.Logf("Calling Get on Client 0 with Key Bob\n")
	ne(client0.Get(util.KVIR("Bob", "", 0, util.R), &ret))
	as(ret.Response == "")

	t.Logf("Calling Put on Client 1 with Key Alice\n")
	er(client1.Put(util.KVIR("Alice", "Should be handled by fe 0", 0, util.R), &ret))

	t.Logf("Calling Get on Client 0 with Key Alice\n")
	ne(client0.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "alice")

	// Make sure that client 1 is alive again
	time.Sleep(15 * time.Second)

	// This should hit client0s cache
	t.Logf("Calling Get on Client 1 with Key Alice\n")
	ne(client1.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "alice")

}
