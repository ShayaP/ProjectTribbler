package interfaces

import "util"

/**
* Backend Interface
* Defines methods that FAWN backends support.
 */
type BackendInterface interface {
	// retrieves the hash entry containing the offset, indexes
	// into the Data Log, and returns the data blob
	Lookup(kvir *util.KeyValueIdReplication, value *util.ResponseId) error

	// appends an entry to the log, updates the corresponding hash
	// table entry to point to this offset within the Data Log, and sets the
	// valid bit to true. If the key written already existed, the old value is
	// now orphaned (no hash entry points to it) for later garbage collection.
	Store(kvir *util.KeyValueIdReplication, value *util.ResponseId) error

	// invalidates the hash entry corresponding to the key by
	// clearing the valid flag and writing a Delete entry to the end of the data
	// file.
	Delete(kvir *util.KeyValueIdReplication, value *util.ResponseId) error

	// Simulate a crash on the frontend.
	Crash(seconds int, success *bool) error

	// check if node is crashed on the frontend.
	IsCrashed(_ string, isCrashed *bool) error

	HasLogFile(_ string, succ *bool) error

	SendLogFile(args util.SendLogArgs, succ *bool) error

	ReceiveLogFile(args util.ReceieveLogArgs, succ *bool) error

	// SPLIT
	// Split parses the Data Log sequentially, writing each entry in a
	// new datastore if its key falls in the new datastore’s range.

	// MERGE
	// Mergewrites every log entry from one datastore into the other datastore;
	// because the key ranges are independent, it does so as an append.
	// Split and Merge propagate delete entries into the new datastore.

	// COMPACT
	// cleans up entries in a datastore, similar to garbage
	// collection in a log-structured filesystem. It skips entries that fall
	// outside of the datastore’s key range, which may be left-over after a
	// split. It also skips orphaned entries that no in-memory hash table
	// entry points to, and then skips any delete entries corresponding to
	// those entries. It writes all other valid entries into the output datastore
}
