package implementation

import (
	"errors"
	"fawn/interfaces"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
	"util"
)

var MAX_KEY_LIMIT float32 = 4294967295.0

type Manager struct {
	Id                int
	isCrashed         bool
	Addr              string
	Managers          []string
	NumManagers       int
	Fronts            []string
	FrontendAliveMap  map[string]bool
	NumFrontendAlive  int
	FrontendKeyRanges map[string]*util.KeyRange
}

func (self *Manager) GetId(_ string, ret *int) error {
	// log.Printf("Calling manager GetId")
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		// log.Printf("%s is currently crashed", self.Addr)
		return errors.New("Server is crashed.")
	}

	*ret = self.Id
	return nil
}

func (m *Manager) UpdateManagerFrontendMetadata(args *interfaces.UpdateArgs, succ *bool) error {
	// Check if we are crashed, if so just return an error
	if m.isCrashed {
		log.Printf("%s is currently crashed", m.Addr)
		return errors.New("Server is crashed.")
	}

	m.FrontendAliveMap = args.AliveMap
	m.FrontendKeyRanges = args.KeyRanges
	m.NumFrontendAlive = args.NumAlive
	*succ = true
	return nil
}

//Makes server unavailable for some seconds
func (m *Manager) Crash(seconds int, success *bool) error {
	log.Printf("%s is now crashed for %d seconds", m.Addr, seconds)
	m.isCrashed = true

	// Start a new thread and restart after the time is up.
	go func(m *Manager) {
		time.Sleep(time.Duration(seconds) * time.Second)
		m.isCrashed = false
		m.Id += m.NumManagers
	}(m)
	*success = true
	return nil
}

// Health check function for this frontend.
func (m *Manager) IsCrashed(_ string, isCrashed *bool) error {
	*isCrashed = m.isCrashed
	return nil
}

var _ interfaces.ManagerInterface = new(Manager)

func NewManager(Id int, addr string, managers []string, numManagers int, fronts []string) *Manager {
	FrontendAliveMap := make(map[string]bool)
	FrontendKeyRanges := make(map[string]*util.KeyRange)
	return &Manager{Id, false, addr, managers, numManagers, fronts, FrontendAliveMap, 0, FrontendKeyRanges}
}

// Assumption: Fronts contains all frontneds that will ever be in the system
// Assumption: No frontend fail before the heatbeat signal begins.
// Assumption: No frontend fail after ComputeKeyRanges starts, and before UpdateFrontendsWithKeyRanges ends.
// Serve a manager to detect frontend failure.
func ServeManager(ready chan<- bool, manager *Manager) error {
	rpcServer := rpc.NewServer()
	rpcServer.Register(manager)

	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	http.DefaultServeMux = oldMux

	l, e := net.Listen("tcp", manager.Addr)
	if e != nil {
		if ready != nil {
			ready <- false
		}
		return e
	}

	if ready != nil {
		ready <- true
	}

	// have a go func that checks on backends.
	go StartFrontendHealthCheck(manager)

	e = http.Serve(l, mux)

	return e
}

// At this point every frontend has most updated key ranges
// heartbreat
func StartFrontendHealthCheck(manager *Manager) {
	// time.Sleep(500 * time.Millisecond)
	log.Printf("Calling StartFrontendHealthCheck")
	// Create the alive map
	alive_map, num_alive := CreateFrontendAliveMap(manager.Fronts)
	if num_alive == -1 {
		log.Panicf("CreateFrontendAliveMap returned with error: couldn't connect to frontend.")
	}

	// Compute key ranges for alive frontends and update them.
	key_ranges := ComputeKeyRanges(num_alive, alive_map, manager.Fronts)
	err := UpdateFrontendsWithKeyRanges(key_ranges)
	if err != nil {
		log.Panicf("Got error in StartFrontendHealthCheck: %v", err)
	}

	manager.FrontendAliveMap = alive_map
	manager.NumFrontendAlive = num_alive
	manager.FrontendKeyRanges = key_ranges

	majority := false
	master_id := -1

	for {
		// Check if this node is crashed
		if manager.isCrashed {
			time.Sleep(1 * time.Second)
			continue
		}

		// Check if I am the master or successor
		master_id, majority = IdentifyMasterManager(manager)

		// Check if this node can reach majority of other management nodes.
		for _, addr := range manager.Fronts {
			// detect Joins and Leaves of frontends.
			// NOTE: Joins and Leaves key range updates are correct,
			// because, we update the keyranges from smallest to largest.
			// If we receieve a request for larger key, then that client will
			// pass the key to the smaller region front end. But the smaller region
			// Frontend just forwards the request again.
			// This happens untill everyone is updated and eventually, that request
			// Is handled correctly.
			if majority && manager.Id == master_id {
				// Attempt to connect to a frontend.
				is_crashed, err := CheckFrontendCrashed(addr)
				if err != nil {
					log.Panicf("panic: CheckFrontendCrashed returned with error: %v", err)
				}

				// get previous status and current status for this frontend
				was_dead := !manager.FrontendAliveMap[addr]
				manager.FrontendAliveMap[addr] = !is_crashed
				if (was_dead && !is_crashed) || (!was_dead && is_crashed) {
					// Update the num_alive, because a frontend joined.
					if was_dead && !is_crashed {
						log.Printf("Manager %d identified %s joined", manager.Id, addr)
						manager.NumFrontendAlive++
					} else {
						// Update the num_alive because a frontend left.
						log.Printf("Manager %d identified %s left", manager.Id, addr)
						manager.NumFrontendAlive--
					}

					// compute new key range for each frontend
					manager.FrontendKeyRanges = ComputeKeyRanges(manager.NumFrontendAlive, manager.FrontendAliveMap, manager.Fronts)

					log.Printf("Manager %d is doing updating frontend key ranges", manager.Id)
					err := UpdateFrontendsWithKeyRanges(manager.FrontendKeyRanges)
					if err != nil {
						log.Panicf("panic: UpdateFrontendsWithKeyRanges returned with error: %v", err)
					}

					// Update other managers frontend meta data
					UpdateAllManagerFrontendMetadata(manager, manager.FrontendAliveMap, manager.NumFrontendAlive, manager.FrontendKeyRanges)
				}
			}
		}

		// Execute this loop every second
		time.Sleep(1 * time.Second)
	}
}

func CreateFrontendAliveMap(fronts []string) (map[string]bool, int) {
	// Create map for addresses to frontend status
	alive_map := make(map[string]bool)

	// number of frontends currently alive
	num_alive := 0

	// for each frontend address
	for _, addr := range fronts {
		// Attempt to connect to a frontend
		conn, e := rpc.DialHTTP("tcp", addr)
		if e != nil {
			return alive_map, -1
		}

		// perform the call
		var is_crashed bool
		e = conn.Call("Client.IsCrashed", "", &is_crashed)
		if e != nil {
			conn.Close()
			return alive_map, -1
		}

		// Check which frontends are dead
		alive_map[addr] = !is_crashed
		// increment count of number alive if frontend is alive
		if !is_crashed {
			num_alive++
		}
	}

	return alive_map, num_alive
}

// Compute the key ranges for each alive frontend and return a mapping from frontend to KeyRange
func ComputeKeyRanges(num_alive int, alive_map map[string]bool, fronts []string) map[string]*util.KeyRange {
	// Initialize a map for return
	keyRanges := make(map[string]*util.KeyRange)

	// Computing the keyrange size based on num_alive.
	// Even if math.MaxUint32 / num_alive is a decimal, we are fine because
	// all hashes are ints and they still fall within bounds.
	// NOTE: Key range partitioning could cause errors due to incorrect division. example: 2147483648.000000, Max: 4294967296.000000
	key_range_size := MAX_KEY_LIMIT / float32(num_alive)
	partition_start := float32(0)

	// For all frontends, if they are alive, compute their next key space.
	for _, addr := range fronts {
		if alive_map[addr] {
			kr := &util.KeyRange{Min: partition_start, Max: partition_start + key_range_size}
			log.Printf("\tKey range for address %s is: Min: %f, Max: %f", addr, kr.Min, kr.Max)
			keyRanges[addr] = kr
			partition_start += key_range_size
		}
	}

	return keyRanges
}

// Loop through each frontend that is alive, and update their key range mapping for all frontends.
func UpdateFrontendsWithKeyRanges(key_ranges map[string]*util.KeyRange) error {
	// Make RPC call to each frontned and update their key range mapping for all frontends.
	for addr, _ := range key_ranges {
		// connect to the server
		conn, e := rpc.DialHTTP("tcp", addr)
		if e != nil {
			return e
		}

		// perform the call
		var succ bool
		e = conn.Call("Client.UpdateKeyRange", key_ranges, &succ)
		if e != nil {
			conn.Close()
			return e
		}
	}

	return nil
}

// Check the health of a frontend.
func CheckFrontendCrashed(addr string) (bool, error) {
	// Make RPC call to each frontned and update their key range mapping for all frontends.
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", addr)
	if e != nil {
		return true, e
	}

	// perform the call
	var is_crashed bool
	e = conn.Call("Client.IsCrashed", "", &is_crashed)
	if e != nil {
		conn.Close()
		return true, e
	}

	return is_crashed, nil
}

// Function to identify the master manager node, also returns if there is a majority.
func IdentifyMasterManager(manager *Manager) (int, bool) {

	manager_indicies := make([]int, 0)
	// Iterate through every manager node.
	for _, addr := range manager.Managers {
		is_alive, idx := GetManagerId(addr)
		if is_alive {
			manager_indicies = append(manager_indicies, idx)
		}
	}

	majority := false

	// Check if this node can reach majority of other management nodes.
	if len(manager_indicies) >= ((len(manager.Managers) / 2) + 1) {
		majority = true
	}

	sort.Ints(manager_indicies)
	return manager_indicies[0], majority
}

func GetManagerId(addr string) (bool, int) {
	// connect to the keeper
	conn, e := rpc.DialHTTP("tcp", addr)
	if e != nil {
		return false, -1
	}

	// perform the call
	var managerId int
	e = conn.Call("Manager.GetId", "", &managerId)
	// log.Printf("")
	if e != nil {
		conn.Close()
		return false, -1
	}

	// close the connection
	return true, managerId
}

func UpdateAllManagerFrontendMetadata(manager *Manager, alive_map map[string]bool, num_alive int, key_ranges map[string]*util.KeyRange) {
	log.Printf("Manager %s updating all managers", manager.Addr)
	for _, addr := range manager.Managers {
		// connect to the keeper
		conn, e := rpc.DialHTTP("tcp", addr)
		if e != nil {
			log.Printf("Got error updating managers: %v", e)
			continue
		}

		// perform the call
		var succ bool
		args := interfaces.UpdateArgs{AliveMap: alive_map, NumAlive: num_alive, KeyRanges: key_ranges}
		e = conn.Call("Manager.UpdateManagerFrontendMetadata", &args, &succ)
		if e != nil {
			conn.Close()
			log.Printf("Got error updating managers: %v", e)
			continue
		}
	}
}
