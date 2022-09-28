package performance_check

import (
	"fawn/interfaces"
	"testing"
	"time"
	"util"
)

func CheckTimeToGetCache(t *testing.T, client interfaces.ClientInterface) int64 {
	var ret util.ResponseId
	client.Put(util.KVIR("Alice", "Testing value", 0, util.R), &ret)

	// Start timer
	start := time.Now()

	client.Get(util.KVIR("Alice", "", 0, util.R), &ret)

	// End the timer
	end := time.Now()

	diff := end.Sub(start)

	return diff.Nanoseconds()
}
