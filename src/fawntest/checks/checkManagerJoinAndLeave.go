package checks

import (
	"fawn/interfaces"
	"runtime/debug"
	"testing"
	"time"
)

func CheckManagerJoinAndLeave(t *testing.T, client0, client1, client2 interfaces.ClientInterface, manager0, manager1 interfaces.ManagerInterface) {
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

	// as := func(cond bool) {
	// 	if !cond {
	// 		debug.PrintStack()
	// 		t.Fatal("assertion failed")
	// 	}
	// }

	var succ bool
	time.Sleep(3 * time.Second)

	ne(client0.Crash(10, &succ))

	time.Sleep(3 * time.Second)
	ne(manager0.Crash(10, &succ))

	time.Sleep(5 * time.Second)
}
