package performance_check

import (
	"fawn/interfaces"
	"testing"
	"time"
	"util"
)

func CheckTimeToSplit(t *testing.T, client interfaces.ClientInterface, back0, back1, back2, back3 interfaces.BackendInterface) {
	var ret util.ResponseId
	amount := 50

	// Run this test for X amounts
	for i := 0; i < amount; i++ {
		client.Put(util.KVIR("alice", "Testing value", 0, util.R), &ret)
	}

	// Crash backend0
	var succ bool
	back3.Crash(5, &succ)

	time.Sleep(10 * time.Second)
}
