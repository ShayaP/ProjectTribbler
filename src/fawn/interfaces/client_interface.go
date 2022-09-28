package interfaces

import "util"

/**
* Client Interface
* Defines methods that FAWN clients can call.
 */

type ClientInterface interface {
	// Get the data stored at this key.
	Get(kvir *util.KeyValueIdReplication, ret *util.ResponseId) error

	// Put data in the KV store by a key.
	Put(kvir *util.KeyValueIdReplication, ret *util.ResponseId) error

	// Update the KeyRange map for this client.
	UpdateKeyRange(keyRanges map[string]*util.KeyRange, succ *bool) error

	// Simulate a crash on the frontend.
	Crash(seconds int, success *bool) error

	// check if node is crashed on the frontend.
	IsCrashed(_ string, isCrashed *bool) error
}
