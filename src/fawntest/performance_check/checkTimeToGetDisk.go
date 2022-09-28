package performance_check

import (
	"fawn/interfaces"
	"testing"
	"time"
	"util"
)

func CheckTimeToGetDisk(t *testing.T, client0, client1 interfaces.ClientInterface) int64 {
	var ret util.ResponseId
	client0.Put(util.KVIR("Alice", "Testing value", 0, util.R), &ret)

	// Kill frontend 0
	var succ bool
	client0.Crash(20, &succ)

	// Start timer
	start := time.Now()

	client1.Get(util.KVIR("Alice", "", 0, util.R), &ret)

	// End the timer
	end := time.Now()

	diff := end.Sub(start)

	return diff.Nanoseconds()
}
