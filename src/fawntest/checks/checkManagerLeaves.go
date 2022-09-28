package checks

import (
	"fawn/interfaces"
	"runtime/debug"
	"testing"
	"time"
)

func CheckManagerLeaves(t *testing.T, client0, client1, client2, client3 interfaces.ClientInterface,
	back0, back1, back2 interfaces.BackendInterface,
	manager0, manager1, manager2 interfaces.ManagerInterface) {
	ne := func(e error) {
		if e != nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}

	var succ bool

	t.Logf("Kill frontend 0")
	ne(client0.Crash(30, &succ))

	// time.Sleep(1 * time.Second)

	t.Logf("Kill manager 0")
	ne(manager0.Crash(30, &succ))

	time.Sleep(5 * time.Second)

	t.Logf("Kill frontend 1")
	ne(client1.Crash(30, &succ))

	time.Sleep(5 * time.Second)

	t.Logf("Kill manager 1")
	ne(manager1.Crash(30, &succ))

	time.Sleep(5 * time.Second)

	// System should be broken because managers cannot form a majority.

	t.Logf("Kill client 2")
	ne(client2.Crash(30, &succ))

	time.Sleep(5 * time.Second)

}
