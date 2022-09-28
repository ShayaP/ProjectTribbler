package implementation

import (
	"errors"
	"fawn/interfaces"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"util"
)

type Client struct {
	// My address
	Addr string

	// All the backends
	Backs []string

	// Key ranges for all frontends.
	KeyRanges map[string]*util.KeyRange

	// Sorted List of backends. This is all the backends that will ever be in the system.
	SortedListOfBackendHashIds []int

	// Map of backends to their hash
	BackendHashToBackendAddrMap map[int]string

	// Cache
	Cache map[string]string

	// Crashed flag
	isCrashed bool
}

//Makes server unavailable for some seconds
func (c *Client) Crash(seconds int, success *bool) error {
	// log.Printf("%s is now crashed for %d seconds", c.Addr, seconds)
	c.isCrashed = true

	// Start a new thread and restart after the time is up.
	go func(s *Client) {
		time.Sleep(time.Duration(seconds) * time.Second)
		c.isCrashed = false
	}(c)
	*success = true
	return nil
}

// Health check function for this frontend.
func (c *Client) IsCrashed(_ string, isCrashed *bool) error {
	*isCrashed = c.isCrashed
	return nil
}

func (c *Client) Put(kvir *util.KeyValueIdReplication, ret *util.ResponseId) error {
	// Check if we are crashed, if so just return an error
	if c.isCrashed {
		// log.Printf("%s is currently crashed", c.Addr)
		return errors.New("Server is crashed.")
	}

	// Check the range.
	if util.CheckKeyInFrontendRange(kvir.Key, c.KeyRanges, c.Addr) {
		// log.Printf("\n \nFrontend %s, handling the PUT request itself.", c.Addr)
		// Determine which backend to call by checking which backend key range handles this key.
		backAddr, err := util.FindPrimaryBackendForKey(kvir.Key, c.BackendHashToBackendAddrMap, c.SortedListOfBackendHashIds)
		if err != nil {
			return err
		}

		// connect to the server
		conn, e := rpc.DialHTTP("tcp", backAddr)
		if e != nil {
			return e
		}

		// perform the call
		var value util.ResponseId
		e = conn.Call("Backend.Store", kvir, &value)
		if e != nil {
			conn.Close()
			return e
		}
		*ret = value

		// Store the value into the cache
		// log.Printf("updating the cache on frontend %s with key %s = %s", c.Addr, kvir.Key, kvir.Value)
		c.Cache[kvir.Key] = kvir.Value

		// close the connection
		return conn.Close()
	} else {
		// Key was not in this frontends range. We should forward to another frontend to handle this.
		return ForwardKeyToAnotherFrontend(kvir, ret, c, "Put")
	}
}

func (c *Client) Get(kvir *util.KeyValueIdReplication, ret *util.ResponseId) error {
	// Check if we are crashed, if so just return an error
	if c.isCrashed {
		// log.Printf("%s is currently crashed", c.Addr)
		return errors.New("Server is crashed.")
	}

	// Check the range.
	if util.CheckKeyInFrontendRange(kvir.Key, c.KeyRanges, c.Addr) {

		// Check your cache
		if value, ok := c.Cache[kvir.Key]; ok {
			// log.Printf("\n \nReached the cache for key %s", kvir.Key)
			// Value is already in cache, just return it.
			*ret = *util.RI(value, kvir.Id)
			return nil
		}

		// log.Printf("\n \nFrontend %s, handling the GET request itself.", c.Addr)

		// Determine which backend to call for this key.
		backAddr, err := util.FindPrimaryBackendForKey(kvir.Key, c.BackendHashToBackendAddrMap, c.SortedListOfBackendHashIds)
		if err != nil {
			return err
		}

		// connect to the server
		conn, e := rpc.DialHTTP("tcp", backAddr)
		if e != nil {
			return e
		}

		// perform the call
		var value util.ResponseId
		e = conn.Call("Backend.Lookup", kvir, &value)
		if e != nil {
			conn.Close()
			return e
		}

		*ret = value

		// log.Printf("Value of ret in client Get function: %v", ret)

		// close the connection
		return conn.Close()
	} else {
		// Key was not in this frontends range. We should forward to another frontend to handle this.
		return ForwardKeyToAnotherFrontend(kvir, ret, c, "Get")
	}
}

// Update the key range map for this frontend. Manager is the only one who calls this function.
// This function shifts the key range for this frontend.
func (c *Client) UpdateKeyRange(keyRanges map[string]*util.KeyRange, succ *bool) error {
	// Check if we are crashed, if so just return an error
	if c.isCrashed {
		log.Printf("%s is currently crashed", c.Addr)
		return errors.New("Server is crashed.")
	}

	log.Printf("Calling update Keyrange on addr %s, with keyranges %v", c.Addr, keyRanges)

	// Invalidate the cache.
	c.Cache = make(map[string]string)

	// Update our keyranges
	c.KeyRanges = keyRanges

	*succ = true
	return nil
}

// debug to confirm correct implementation of the interface for client
var _ interfaces.ClientInterface = new(Client)

func NewClient(addr string, backs []string) *Client {
	// Create the cache
	Cache := make(map[string]string)

	// Creates mapping of backend hash ID to backend address, and sorts the backend hashes.
	BackendHashToBackendAddrMap, SortedListOfBackendHashIds := util.CreateSortedBackendHashListAndMap(backs)

	log.Printf("SortedListOfBackendHashIds: %v, BackendHashToBackendAddrMap: %v", SortedListOfBackendHashIds, BackendHashToBackendAddrMap)

	return &Client{Addr: addr, Backs: backs, SortedListOfBackendHashIds: SortedListOfBackendHashIds, BackendHashToBackendAddrMap: BackendHashToBackendAddrMap, Cache: Cache, isCrashed: false}
}

func ForwardKeyToAnotherFrontend(kvir *util.KeyValueIdReplication, ret *util.ResponseId, c *Client, op string) error {
	// Find another frontend who is responsible for this key.
	clientAddr, err := util.FindPrimaryFrontendForKey(kvir.Key, c.KeyRanges)
	if err != nil {
		// Should not be nil
		return err
	}
	// log.Printf("Forwarding %s request to %s.\n", op, clientAddr)

	// Just forward the call to other frontend via RPC.
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", clientAddr)
	if e != nil {
		return e
	}

	// perform the call
	var value util.ResponseId
	e = conn.Call("Client."+op, kvir, &value)
	if e != nil {
		conn.Close()
		return e
	}
	*ret = value

	// close the connection
	return conn.Close()
}

func ServeFrontend(addr string, ready chan<- bool, front *Client) error {
	rpcServer := rpc.NewServer()
	rpcServer.Register(front)

	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	http.DefaultServeMux = oldMux

	l, e := net.Listen("tcp", addr)
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
	go StartBackendHealthCheck(front)

	e = http.Serve(l, mux)

	return e
}

func StartBackendHealthCheck(front *Client) {
	// Create the alive map
	alive_map, num_alive := CreateBackendAliveMap(front.Backs)

	for {
		// Wait until this backend has an initialized KeyRange map.
		// log.Printf("In frontend StartBackendHealthCheck, keyranges: %v", front.KeyRanges)
		if len(front.KeyRanges) > 0 {

			for _, addr := range front.Backs {
				// Check if this frontend is responsible for this backend.
				if util.CheckKeyInFrontendRange(addr, front.KeyRanges, front.Addr) {
					is_crashed, err := util.CheckBackendCrashed(addr)
					if err != nil {
						log.Panicf("frontend %s got unexpected error from backend %s: %v", front.Addr, addr, err)
					}

					// get previous status and current status for this backend
					was_dead := !alive_map[addr]
					alive_map[addr] = !is_crashed

					// This case checks when either a backend comes to life or just died.
					if (was_dead && !is_crashed) || (!was_dead && is_crashed) {
						// Update the num_alive, because a backend joined.
						if was_dead && !is_crashed {
							log.Printf("Frontend identified backend %s joined", addr)
							num_alive++

							// Check if it has a log, if so, we don't need to do join protocol
							// This case occurs when our keyspace has grown
							// and a node that we initially marked as dead in our alive_map is now alive.
							// Now we need to be sure, no one else handled join protocol for this backend.
							// If no one has done join, then this node has no log.
							hasLogFile, err := util.CheckBackendCrashed(addr)

							if err != nil {
								log.Panicf("frontend %s got unexpected error from backend %s: %v", front.Addr, addr, err)
							}

							if !hasLogFile {
								// Do join protocol
								err := JoinProtocol(addr, front.SortedListOfBackendHashIds, front.BackendHashToBackendAddrMap)
								if err != nil {
									log.Panicf("frontend %s got unexpected error from backend %s during join protocol: %v", front.Addr, addr, err)
								}
							}
						} else {
							// Update the num_alive because a backend left.
							log.Printf("Frontend identified backend %s left", addr)
							num_alive--

							err := LeaveProtocol(addr, front.SortedListOfBackendHashIds, front.BackendHashToBackendAddrMap)
							if err != nil {
								log.Panicf("frontend %s got unexpected error from backend %s during leave protocol: %v", front.Addr, addr, err)
							}
						}
					}
				}
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func CreateBackendAliveMap(backs []string) (map[string]bool, int) {
	// Create map for addresses to backend status
	alive_map := make(map[string]bool)

	// number of backends currently alive
	num_alive := 0

	// for each backend address
	for _, addr := range backs {
		// Attempt to connect to a backend
		conn, e := rpc.DialHTTP("tcp", addr)
		if e != nil {
			return alive_map, -1
		}

		// perform the call
		var is_crashed bool
		e = conn.Call("Backend.IsCrashed", "", &is_crashed)
		if e != nil {
			conn.Close()
			return alive_map, -1
		}

		// Check which backends are dead
		alive_map[addr] = !is_crashed
		// increment count of number alive if backend is alive
		if !is_crashed {
			num_alive++
		}
	}

	return alive_map, num_alive
}

// Join protocol occurs when a new backend joins the ring. In this case we need to give the new backend the data for the R chains
// for which it is a part of.
func JoinProtocol(addr string, SortedListOfBackendHashIds []int, BackendHashToBackendAddrMap map[int]string) error {
	// NOTE: These print statements are for timing purposes.
	// log.Printf("Start time of Join Protocol: %d", time.Now().Nanosecond())
	start := time.Now()

	// Get the has for the joining backend.
	hash := util.GetHashId(addr)

	// Get the index for the joining backend.
	newNodeIdx := util.GetBackendIndex(SortedListOfBackendHashIds, hash)

	// log.Printf("newNodeIdx index: %d", newNodeIdx)
	// log.Printf("Calling Join Protocol on %s", addr)

	// get logs from tail of every R chain this node is a member of
	// newNodeIdx is the index of the joining backend in the SortedListOfBackendHashIds list
	// The joining node needs to communicate with the tail nodes of every R chain it was a part of before leaving,
	// to get the correct data.
	tailToSendLogsIdx := newNodeIdx
	var err error
	for i := 1; i <= util.R; i++ {
		// Finds index of the tail of a chain for which the new backend was a part of before, and the tail sends its log
		// to the new backend.
		tailToSendLogsIdx = util.GetIndexOnRing((tailToSendLogsIdx + 1), len(SortedListOfBackendHashIds))
		// log.Printf("\n\ntailToSendLogIdx before retry in Join protocol: %d", tailToSendLogsIdx)
		tailToSendLogsIdx, err = util.FindAliveBackendWithRetryLogic(tailToSendLogsIdx, BackendHashToBackendAddrMap, SortedListOfBackendHashIds)
		if err != nil {
			log.Panicf("Got error in JoinProtocol: %v", err)
		}

		// log.Printf("(Join Protocol) Sending logs from %d to %d", tailToSendLogsIdx, newNodeIdx)

		// Find the key range, for chain we are trying to insert the new backend into.
		newKeyRange, isWrapAround := identifyKeyRangeToReplicateWhenJoining(newNodeIdx, i, SortedListOfBackendHashIds, BackendHashToBackendAddrMap)

		// Get the addresses based on the backend indicies found above.
		newNodeAddr := BackendHashToBackendAddrMap[SortedListOfBackendHashIds[newNodeIdx]]
		tailToSendLogsAddr := BackendHashToBackendAddrMap[SortedListOfBackendHashIds[tailToSendLogsIdx]]

		// Have the tail identified above, send its logs to the new backend joining the ring.
		err = CallSendLogFile(tailToSendLogsAddr, newNodeAddr, newKeyRange, isWrapAround)
		if err != nil {
			return err
		}
	}

	end := time.Now()
	log.Printf("Time For Join Protocol: %d", end.Sub(start).Nanoseconds())
	return nil
}

// send logs from tail of every R chain this node is a member of
// deadNodeIdx is the index of the leaving backend in the SortedListOfBackendHashIds list
// The Frontend needs to communicate with the tail nodes of every R chain the leaving backend was a part of
// to replicate the correct data.
func LeaveProtocol(addr string, SortedListOfBackendHashIds []int, BackendHashToBackendAddrMap map[int]string) error {
	start := time.Now()
	// Get the hash for the backend leaving
	hash := util.GetHashId(addr)

	// Get the index of the dead node in the ring.
	deadNodeIdx := util.GetBackendIndex(SortedListOfBackendHashIds, hash)

	// log.Printf("Deadnode index: %d", deadNodeIdx)
	// log.Printf("Calling Leave Protocol on %s", addr)

	// For every chain in R chains this leaving node was a part of, find the tail and send the logs at the tail to the next node after the tail
	// make R-1 calls to replicate.
	// We handle the case where this leaving node is the tail after the loop.
	for i := 1; i < util.R; i++ {
		// Find the current tail of this chain and identify a new tail for this chain.
		currTailNodeAddr, newTailNodeAddr := findCurrentAndNewTail(deadNodeIdx, i, SortedListOfBackendHashIds, BackendHashToBackendAddrMap)
		// log.Printf("(Leave Protocol) currTailNodeAddr: %s, newTailNodeAddr: %s", currTailNodeAddr, newTailNodeAddr)

		// Find the key range, for the chain we are trying to replicate onto a new tail.
		newKeyRange, isWrapAround := identifyKeyRangeToReplicateWhenLeaving(deadNodeIdx, i, SortedListOfBackendHashIds, BackendHashToBackendAddrMap)

		// log.Printf("(Leave Protocol) Tail node %s is sending to new tail node %s with keyrange: %v\n\n", currTailNodeAddr, newTailNodeAddr, newKeyRange)
		// Have the tail identified above, send its logs to the new backend joining the ring.
		err := CallSendLogFile(currTailNodeAddr, newTailNodeAddr, newKeyRange, isWrapAround)
		if err != nil {
			return err
		}
	}

	// Handle the case where the dead node is the tail of the current chain
	// In this case, we need to have the head of the current chain
	// send logs to the new tail of the current chain.

	// Find the index of the current head of the chain, that the leaving backend is a part of.
	headNodeIdx := util.GetIndexOnRing(deadNodeIdx-(util.R-1), len(SortedListOfBackendHashIds))
	headNodeIdx, err := util.FindAliveBackendWithRetryLogic(headNodeIdx, BackendHashToBackendAddrMap, SortedListOfBackendHashIds)
	if err != nil {
		log.Panicf("Got error with retry logic in special case of Leave protocol: %v", err)
	}
	headNodehash := SortedListOfBackendHashIds[headNodeIdx]
	headNodeAddr := BackendHashToBackendAddrMap[headNodehash]

	// Find the index of the new tail for this chain.
	nextTailNodeIdx := util.GetIndexOnRing((deadNodeIdx + 1), len(SortedListOfBackendHashIds))
	nextTailNodeIdx, err = util.FindAliveBackendWithRetryLogic(nextTailNodeIdx, BackendHashToBackendAddrMap, SortedListOfBackendHashIds)
	if err != nil {
		log.Panicf("Got error with retry logic in special case of Leave Protocol: %v", err)
	}
	nextTailNodehash := SortedListOfBackendHashIds[nextTailNodeIdx]
	nextTailNodeAddr := BackendHashToBackendAddrMap[nextTailNodehash]

	// log.Printf("\n\n(Leave Protocol) Special case is: Head node %s is sending to new tail node %s", headNodeAddr, nextTailNodeAddr)

	// ASSUMPTION: we are not handling the case where there are less than R nodes.
	isWrapAround := false

	// Find the min of the key range for the head node of the current chain.
	idxOfKeyRangeMinOfHeadNode := util.GetIndexOnRing((headNodeIdx - 1), len(SortedListOfBackendHashIds))

	// If headNodeIdx is less than the idx of key range min, then we are wrapping around.
	if idxOfKeyRangeMinOfHeadNode > headNodeIdx {
		isWrapAround = true
	}

	// log.Printf("(Leave Protocol) Special case is: key range min: %d, max: %d", idxOfKeyRangeMinOfHeadNode, headNodeIdx)
	// Find the key range for the head node to send
	headNodeKeyRange := util.KeyRange{Min: float32(SortedListOfBackendHashIds[idxOfKeyRangeMinOfHeadNode]), Max: float32(SortedListOfBackendHashIds[headNodeIdx])}

	// Headnode sends its logs to the next tail for this current chain.
	err = CallSendLogFile(headNodeAddr, nextTailNodeAddr, headNodeKeyRange, isWrapAround)
	if err != nil {
		return err
	}

	end := time.Now()
	log.Printf("Time For Leave Protocol: %d", end.Sub(start).Nanoseconds())
	return nil
}

// Function to find the current and new tail node in a replication chain,
// given a dead node index, and a dead node offset to tail.
func findCurrentAndNewTail(deadNodeIdx, deadNodeOffsetToTail int, SortedListOfBackendHashIds []int, BackendHashToBackendAddrMap map[int]string) (string, string) {
	// Get the index of the current tail node in the chain
	currTailNodeIdx := util.GetIndexOnRing((deadNodeIdx + deadNodeOffsetToTail), len(SortedListOfBackendHashIds))
	currTailNodeIdx, err := util.FindAliveBackendWithRetryLogic(currTailNodeIdx, BackendHashToBackendAddrMap, SortedListOfBackendHashIds)
	if err != nil {
		log.Panicf("Got error in findCurrentAndNewTail: %v", err)
	}

	// Get the hash of the current tail node in the chain
	currTailNodehash := SortedListOfBackendHashIds[currTailNodeIdx]
	// Get the address of the current tail node in the chain
	currTailNodeAddr := BackendHashToBackendAddrMap[currTailNodehash]

	// log.Printf("After retry logic the current tail addr is: %s", currTailNodeAddr)

	// Get the index of the new tail node in the chain
	newTailNodeIdx := util.GetIndexOnRing((currTailNodeIdx + 1), len(SortedListOfBackendHashIds))
	newTailNodeIdx, err = util.FindAliveBackendWithRetryLogic(newTailNodeIdx, BackendHashToBackendAddrMap, SortedListOfBackendHashIds)
	if err != nil {
		log.Panicf("Got error in findCurrentAndNewTail: %v", err)
	}

	// Get the hash of the new tail node in the chain
	newTailNodehash := SortedListOfBackendHashIds[newTailNodeIdx]
	// Get the address of the new tail node in the chain
	newTailNodeAddr := BackendHashToBackendAddrMap[newTailNodehash]

	// log.Printf("After retry logic the new tail node addr is: %s", newTailNodeAddr)

	return currTailNodeAddr, newTailNodeAddr
}

// Function to find the key range for an R chain that the backend that is leaving is currently a part of,
// given a dead node index and a dead  node offset to tail, use that to find the key range to the chain.
// This only works when the deadnode is not the tail itself.
func identifyKeyRangeToReplicateWhenLeaving(deadNodeIdx, deadNodeOffsetToTail int, SortedListOfBackendHashIds []int, BackendHashToBackendAddrMap map[int]string) (util.KeyRange, bool) {
	isWrapAround := false

	// deadNodeIdx = 2, deadNodeOffset = 2
	// log.Printf("(identifyKeyRangeToReplicateWhenLeaving) deadNodeIdx: %d, deadNodeOffset: %d", deadNodeIdx, deadNodeOffsetToTail)

	headKeyRangeMin := util.GetIndexOnRing((deadNodeIdx + deadNodeOffsetToTail - util.R), len(SortedListOfBackendHashIds))
	headKeyRangeMax := util.GetIndexOnRing((headKeyRangeMin + 1), len(SortedListOfBackendHashIds))

	// log.Printf("(identifyKeyRangeToReplicateWhenLeaving) max key range: %d, min key range: %d", headKeyRangeMax, headKeyRangeMin)

	// If the head key range Min index is greater than the max index, we need to wrap around.
	if headKeyRangeMin > headKeyRangeMax {
		isWrapAround = true
	}

	// key range of the head node of the current chain.
	headKeyRange := util.KeyRange{Min: float32(SortedListOfBackendHashIds[headKeyRangeMin]), Max: float32(SortedListOfBackendHashIds[headKeyRangeMax])}
	return headKeyRange, isWrapAround
}

// Function to find the key range for an R chain that the backend that is joining should be a part of,
// given a new node index and an to key range min, use that to find the key range to the chain.
func identifyKeyRangeToReplicateWhenJoining(newNodeIdx, offsetToKeyRangeMin int, SortedListOfBackendHashIds []int, BackendHashToBackendAddrMap map[int]string) (util.KeyRange, bool) {
	isWrapAround := false

	// log.Printf("(identifyKeyRangeToReplicateWhenJoining) newNodeIdx: %d, offsetToKeyRangeMin: %d", newNodeIdx, offsetToKeyRangeMin)

	// Get the min and max index of the key range.
	headKeyRangeMax := util.GetIndexOnRing((newNodeIdx - (util.R - offsetToKeyRangeMin)), len(SortedListOfBackendHashIds))
	headKeyRangeMin := util.GetIndexOnRing(headKeyRangeMax-1, len(SortedListOfBackendHashIds))

	// log.Printf("(identifyKeyRangeToReplicateWhenJoining) max key range: %d, min key range: %d", headKeyRangeMax, headKeyRangeMin)
	// If key range min index is greater than max, we need to wrap around.
	if headKeyRangeMin > headKeyRangeMax {
		isWrapAround = true
	}

	// log.Printf("is wrap around: %t", isWrapAround)
	// log.Printf("headKeyRangeMin: %d, len SortedListOfBackendHashIds: %d", headKeyRangeMin, len(SortedListOfBackendHashIds))

	// key range of the head node of the current chain.
	headKeyRange := util.KeyRange{Min: float32(SortedListOfBackendHashIds[headKeyRangeMin]), Max: float32(SortedListOfBackendHashIds[headKeyRangeMax])}
	return headKeyRange, isWrapAround
}

// Helper function to call send SendLogFile RPC function.
// We send the log file from addrToSendLogs to addrToReceiveLogs
func CallSendLogFile(addrToSendLogs, addrToReceiveLogs string, keyRange util.KeyRange, isWrapAround bool) error {
	// log.Printf("Calling send log file with params: %s, %s, %v, %t", addrToSendLogs, addrToReceiveLogs, keyRange, isWrapAround)
	// Connect to the tail
	conn, e := rpc.DialHTTP("tcp", addrToSendLogs)
	if e != nil {
		return e
	}

	// perform the call
	var succ bool
	sendLogArgs := util.SendLogArgs{KR: keyRange, Addr: addrToReceiveLogs, IsWrapAround: isWrapAround}
	e = conn.Call("Backend.SendLogFile", sendLogArgs, &succ)
	if e != nil {
		conn.Close()
		return e
	}

	return nil
}
