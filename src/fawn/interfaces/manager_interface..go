package interfaces

import "util"

type UpdateArgs struct {
	AliveMap  map[string]bool
	NumAlive  int
	KeyRanges map[string]*util.KeyRange
}

type ManagerInterface interface {
	GetId(_ string, ret *int) error

	// Simulate a crash on the frontend.
	Crash(seconds int, success *bool) error

	// check if node is crashed on the frontend.
	IsCrashed(_ string, isCrashed *bool) error

	UpdateManagerFrontendMetadata(args *UpdateArgs, succ *bool) error
}
