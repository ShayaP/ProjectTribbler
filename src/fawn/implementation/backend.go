// Package store provides a simple in-memory key value store.
package implementation

import (
	"bufio"
	"errors"
	"fawn/interfaces"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"util"
)

// In-memory storage implementation. All calls always returns nil.
type Backend struct {
	// Virtual IDs [0 => kjnflask, 1 => lajns, 2]
	// Virtual IDs are hashed identifiers derived from the node’s
	// address. Each VID owns the items for which it is the item’s successor
	// in the ring space (the node immediately clockwise in the ring).
	BackendAddr     string
	Backs           []string
	HashTable       map[string]int
	VIDNextLineOpen map[int]int
	// Sorted List of backends. This is all the backends that will ever be in the system.
	SortedListOfBackendHashIds []int

	// Map of backends to their hash
	BackendHashToBackendAddrMap map[int]string
	TableLock                   sync.Mutex
	VIDNextLineOpenLock         sync.Mutex
	V                           int
	isCrashed                   bool
}

//Makes server unavailable for some seconds and deletes its logs.
func (b *Backend) Crash(seconds int, success *bool) error {
	log.Printf("%s is now crashed for %d seconds", b.BackendAddr, seconds)
	b.isCrashed = true
	// delete logs for this node.
	err := util.DeleteLogs(b.BackendAddr, b.V)
	if err != nil {
		return err
	}

	go func(s *Backend) {
		time.Sleep(time.Duration(seconds) * time.Second)
		b.isCrashed = false
	}(b)
	*success = true
	return nil
}

// Health check function for this frontend.
func (b *Backend) IsCrashed(_ string, isCrashed *bool) error {
	*isCrashed = b.isCrashed
	return nil
}

// Get an item from the data store.
func (self *Backend) Lookup(kvir *util.KeyValueIdReplication, ret *util.ResponseId) error {
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		log.Printf("%s is currently crashed", self.BackendAddr)
		return errors.New("Server is crashed.")
	}

	// log.Printf("Backend %s received Store request.", self.BackendAddr)

	// Acquire the lock for the hashtable
	self.TableLock.Lock()
	defer self.TableLock.Unlock()

	// Get the offset into the log file from the hashtable.
	offset, ok := self.HashTable[kvir.Key]

	// If the key was not found in the hashtable, return empty string.
	if !ok {
		*ret = *util.RI("", kvir.Id)
		log.Printf("Key %s does not exist on vid: %d\n", kvir.Key, kvir.Id)
		return nil
	}

	// Open the log file with readonly permissions
	pathToFile := util.GetLogFileName(self.BackendAddr, kvir.Id)
	logFile, err := os.OpenFile(pathToFile, os.O_RDONLY, 0700)

	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}

	defer logFile.Close() // Close the file.

	// read the offset line.
	line, _, err := util.ReadLine(logFile, offset)
	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}

	// Set the return value.
	val := util.Unescape(line)
	*ret = *util.RI(strings.Split(val, "::")[1], kvir.Id)

	return nil
}

// Store a key value pair into the data store.
func (self *Backend) Store(kvir *util.KeyValueIdReplication, ret *util.ResponseId) error {
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		log.Printf("%s is currently crashed", self.BackendAddr)
		return errors.New("Server is crashed.")
	}

	// log.Printf("Backend %s received Store request.", self.BackendAddr)

	// Replicate first. This node just replicates to the next node in the Backend List.
	err := ReplicateOnChain(self, kvir, ret, kvir.R)
	if err != nil {
		return err
	}

	// log.Printf("Replication successful. Writing to %s disk", self.BackendAddr)

	// Acquire the lock for the hashtable
	self.TableLock.Lock()
	defer self.TableLock.Unlock()

	// Acquire the lock for the VID line number table.
	self.VIDNextLineOpenLock.Lock()
	defer self.VIDNextLineOpenLock.Unlock()

	// Open the file with Write only and Append mode.
	pathToFile := util.GetLogFileName(self.BackendAddr, kvir.Id)
	logFile, err := os.OpenFile(pathToFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}
	defer logFile.Close() // Close the file.

	// Append to the log
	writer := bufio.NewWriter(logFile)

	// Escape the key and value pair to store in the log file.
	val := util.Escape(kvir.Key + "::" + kvir.Value)
	_, err = writer.WriteString(val + "\n")
	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}

	// Flush the buffer, so that we write to the log before this function returns.
	err = writer.Flush()
	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}

	// Update the hash table entry for this key with the index of the last log entry for this key.
	lineWritenTo := self.VIDNextLineOpen[kvir.Id]
	self.HashTable[kvir.Key] = lineWritenTo

	// Update the next available line number.
	self.VIDNextLineOpen[kvir.Id] = lineWritenTo + 1

	// Set the return value.
	*ret = *util.RI("", kvir.Id)

	// log.Printf("Successfully wrote to %s disk", self.BackendAddr)

	return nil
}

// Delete a key from the KeyValue store.
// By setting the value to "", we are essentially deleting it.
func (self *Backend) Delete(kvir *util.KeyValueIdReplication, value *util.ResponseId) error {
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		log.Printf("%s is currently crashed", self.BackendAddr)
		return errors.New("Server is crashed.")
	}

	kvir.Value = ""
	return self.Store(kvir, value)
}

// Check if this backend has a log file.
func (self *Backend) HasLogFile(_ string, succ *bool) error {
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		log.Printf("%s is currently crashed", self.BackendAddr)
		return errors.New("Server is crashed.")
	}

	// Check if file exists. this node will always have a log file for vid 0, if it has any logs at all.
	logFileName := util.GetLogFileName(self.BackendAddr, 0)
	var err error
	if _, err = os.Stat(logFileName); err == nil {
		*succ = true
	} else {
		*succ = false
	}
	return err
}

// This backend will send its log files and sends it to the backend address specified in the argument.
func (self *Backend) SendLogFile(args util.SendLogArgs, succ *bool) error {
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		log.Printf("%s is currently crashed", self.BackendAddr)
		return errors.New("Server is crashed.")
	}

	// Get the logfile name for this backend.
	logFileName := util.GetLogFileName(self.BackendAddr, 0)

	// Read the content of the log file.
	log, err := ioutil.ReadFile(logFileName)
	if err != nil {
		return err
	}

	receiveLogArgs := util.ReceieveLogArgs{KR: args.KR, Log: log, IsWrapAround: args.IsWrapAround}

	// connect to the server
	conn, e := rpc.DialHTTP("tcp", args.Addr)
	if e != nil {
		return e
	}

	// perform the call
	var receiveSucc bool
	e = conn.Call("Backend.ReceiveLogFile", receiveLogArgs, &receiveSucc)
	if e != nil {
		conn.Close()
		return e
	}

	*succ = true

	return nil
}

// This function is used when a backend is receiving logs from another backend to replicate the keyspace specified
// in their data store.
func (self *Backend) ReceiveLogFile(args util.ReceieveLogArgs, succ *bool) error {
	// Check if we are crashed, if so just return an error
	if self.isCrashed {
		log.Printf("%s is currently crashed", self.BackendAddr)
		return errors.New("Server is crashed.")
	}

	// Acquire the lock for the hashtable
	self.TableLock.Lock()
	defer self.TableLock.Unlock()

	// Acquire the lock for the VID line number table.
	self.VIDNextLineOpenLock.Lock()
	defer self.VIDNextLineOpenLock.Unlock()

	// Create a new log file for this backend.
	// NOTE: Assuming VID is always 0 here. change later.
	logFileName := util.GetLogFileName(self.BackendAddr, 0)

	// If file doesn't exists, create it first.
	// log.Printf("checking if log file exists for %s", self.BackendAddr)
	if _, err := os.Stat(logFileName); err != nil {
		// log.Printf("Log file does not exist for %s", self.BackendAddr)
		// Just create the file.
		f, err := os.Create(logFileName)
		if err != nil {
			return err
		}
		f.Close()
	}

	// Just open the file.
	logFile, err := os.OpenFile(logFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}
	defer logFile.Close() // Close the file.

	// Parse the new log file.
	newLogString := string(args.Log)
	// Split the log by newlines.
	entries := strings.Split(newLogString, "\n")

	// Writter for the new log.
	writer := bufio.NewWriter(logFile)

	// For every entry in the log file, add it to our log file, if its in the keyrange specified.
	for _, entry := range entries {
		// Skip empty lines here.
		if entry == "" {
			break
		}

		// Split the entry into key and value
		key, _ := util.GetKeyValueFromLog(entry)
		key_hash := util.GetHashId(key)

		// log.Printf("should we wrap around? %t", args.IsWrapAround)

		// Check the bounds of the keyrange.
		// log.Printf("entry: %s --> key_hash %d min: %f, max: %f", entry, key_hash, args.KR.Min, args.KR.Max)
		if (args.IsWrapAround && (float32(key_hash) <= args.KR.Max || float32(key_hash) > args.KR.Min)) ||
			!args.IsWrapAround && (float32(key_hash) > args.KR.Min && float32(key_hash) <= args.KR.Max) {
			// log.Printf("entry %s is being added to backend %s", entry, self.BackendAddr)

			// Write the new value to the log file.
			_, err = writer.WriteString(entry + "\n")
			if err != nil {
				log.Printf("Got error: %v", err)
				return err
			}

			// Update the hash table entry for this key with the index of the last log entry for this key.
			// NOTE: This is only considering VID 0, change later.
			lineWritenTo := self.VIDNextLineOpen[0]
			self.HashTable[key] = lineWritenTo

			// Update the next available line number.
			self.VIDNextLineOpen[0] = lineWritenTo + 1
		}
	}

	// Flush the buffer, so that we write to the log before this function returns.
	err = writer.Flush()
	if err != nil {
		log.Printf("Got error: %v", err)
		return err
	}

	*succ = true
	return nil
}

var _ interfaces.BackendInterface = new(Backend)

func ReplicateOnChain(backend *Backend, kvir *util.KeyValueIdReplication, ret *util.ResponseId, num_replications int) error {
	// Decrement the R value, if it's 0, don't replicate just return
	kvir.R = kvir.R - 1
	if kvir.R == 0 {
		return nil
	}

	// Get my index in the Sorted list
	hash := util.GetHashId(backend.BackendAddr)
	backendIdx := util.GetBackendIndex(backend.SortedListOfBackendHashIds, hash)

	// Find the next backend with retry logic
	backendToReplicateOnIdx := util.GetIndexOnRing(backendIdx+1, len(backend.SortedListOfBackendHashIds))

	// Check it its alive
	backendToReplicateOnIdx, err := util.FindAliveBackendWithRetryLogic(backendToReplicateOnIdx,
		backend.BackendHashToBackendAddrMap, backend.SortedListOfBackendHashIds)

	if err != nil {
		return err
	}

	// Get the address of the backend at the index found above.
	backendToReplicateOnHash := backend.SortedListOfBackendHashIds[backendToReplicateOnIdx]
	backendToReplicateOnAddr := backend.BackendHashToBackendAddrMap[backendToReplicateOnHash]

	// log.Printf("Replicating to %s", backendToReplicateOnAddr)

	// Replicate on this node and recursively replicate on other nodes in the chain.
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", backendToReplicateOnAddr)
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

	return nil
}

func GetReplicationChain(addr string, backs []string, R int) []string {
	// Create a map
	BackendHashToBackendAddrMap := make(map[int]string)

	// Create the list
	SortedListOfBackendHashIds := make([]int, 0)

	// calculate the backend hashes and store them in list, and populate a map
	for _, backAddr := range backs {
		hash_id := util.GetHashId(backAddr)
		BackendHashToBackendAddrMap[hash_id] = backAddr
		SortedListOfBackendHashIds = append(SortedListOfBackendHashIds, hash_id)
	}

	// Sort the list
	sort.Ints(SortedListOfBackendHashIds)

	// Get my hash
	hash := util.GetHashId(addr)

	var my_idx int
	for my_idx = 0; my_idx < len(SortedListOfBackendHashIds); my_idx++ {
		if SortedListOfBackendHashIds[my_idx] == hash {
			break
		}
	}

	chain := make([]string, 0)
	curr_idx := my_idx + 1
	for {
		// Exit cond
		if len(chain) == R-1 {
			break
		}

		// Wrap around
		if curr_idx == len(SortedListOfBackendHashIds) {
			curr_idx = 0
		}

		// Add replication nodes.
		chain = append(chain, BackendHashToBackendAddrMap[SortedListOfBackendHashIds[curr_idx]])
		curr_idx++
	}

	return chain
}

func NewBackend(Addr string, Backs []string, R int, V int) *Backend {
	// Creates mapping of backend hash ID to backend address, and sorts the backend hashes.
	BackendHashToBackendAddrMap, SortedListOfBackendHashIds := util.CreateSortedBackendHashListAndMap(Backs)

	// Initialize the logs
	for i := 0; i < V; i++ {
		logFileName := util.GetLogFileName(Addr, i)

		// If file exists, remove it first.
		if _, err := os.Stat(logFileName); err == nil {
			err := os.Remove(logFileName)
			if err != nil {
				log.Fatal(fmt.Sprintf("Could not remove log file: %v", err))
			}
		}

		// Just open the file.
		f, err := os.Create(logFileName)
		if err != nil {
			log.Fatal(fmt.Sprintf("Could not create log file: %v", err))
		}
		if err := f.Close(); err != nil {
			log.Fatal(fmt.Sprintf("Could not close log file: %v", err))
		}
	}

	return &Backend{
		BackendAddr:                 Addr,
		Backs:                       Backs,
		BackendHashToBackendAddrMap: BackendHashToBackendAddrMap,
		SortedListOfBackendHashIds:  SortedListOfBackendHashIds,
		HashTable:                   make(map[string]int),
		VIDNextLineOpen:             make(map[int]int),
		V:                           V,
	}
}

func ServeBackend(addr string, V int, ready chan<- bool, back *Backend) error {
	rpcServer := rpc.NewServer()
	rpcServer.Register(back)

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

	e = http.Serve(l, mux)

	return e
}
