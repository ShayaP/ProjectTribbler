package checks

import (
	"fawn/interfaces"
	"runtime/debug"
	"testing"
	"util"
)

func CheckReplication(t *testing.T, client interfaces.ClientInterface) {
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

	t.Logf("Calling Put with Key Alice\n")
	ne(client.Put(util.KVIR("Alice", "Testing value", 0, util.R), &ret))

	ne(client.Get(util.KVIR("Alice", "", 0, util.R), &ret))
	as(ret.Response == "Testing value")
}
