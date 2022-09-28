package util

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const R = 3

// Create the path for the log file for this VID and Addr
func GetLogFileName(Addr string, vid int) string {
	escapedAddr := strings.Replace(Addr, ":", "-", 1)
	logName := escapedAddr + "-" + strconv.Itoa(vid)
	absPath, _ := filepath.Abs("/mnt/e/UCSD/CSE223BProject/CSE223B_Final_Project/src/logs/" + logName + ".log")
	return absPath
}

func GetKeyValueFromLog(entry string) (string, string) {
	escapedEntry := Unescape(entry)
	splitEntry := strings.Split(escapedEntry, "::")
	return splitEntry[0], splitEntry[1]
}

// Function to read a specific line number from a file.
func ReadLine(r io.Reader, lineNum int) (line string, lastLine int, err error) {
	sc := bufio.NewScanner(r)
	// Read every line.
	for sc.Scan() {
		if lastLine == lineNum {
			// you can return sc.Bytes() if you need output in []bytes
			return sc.Text(), lastLine, sc.Err()
		}
		lastLine++
	}
	return line, lastLine, io.EOF
}

// Delete a nodes log files.
func DeleteLogs(addr string, V int) error {
	// Get all the logs for this backend.
	for i := 0; i < V; i++ {
		logFileName := GetLogFileName(addr, i)
		e := os.Remove(logFileName)
		if e != nil {
			return e
		}
		log.Printf("Removed file: %s", logFileName)
	}

	return nil
}

func CreateSortedBackendHashListAndMap(Backs []string) (map[int]string, []int) {
	// Create a map
	BackendHashToBackendAddrMap := make(map[int]string)

	// Create the list
	SortedListOfBackendHashIds := make([]int, 0)

	// calculate the backend hashes and store them in list, and populate a map
	for _, backAddr := range Backs {
		hash_id := GetHashId(backAddr)
		BackendHashToBackendAddrMap[hash_id] = backAddr
		SortedListOfBackendHashIds = append(SortedListOfBackendHashIds, hash_id)
	}

	// Sort the list
	sort.Ints(SortedListOfBackendHashIds)

	return BackendHashToBackendAddrMap, SortedListOfBackendHashIds
}

// Get the backend index in the sorted slice of backend hash IDs
func GetBackendIndex(slice []int, hash int) int {
	idx := 0
	for i, val := range slice {
		if val == hash {
			idx = i
		}
	}
	return idx
}

// Check the health of a frontend.
// can also check if backend has a log file.
// We call IsCrashed, because if a backend is crashed, we are guaranteed that the backend has no log file.
// We always create log file upon join, so if it is joined, it will have a log file.
func CheckBackendCrashed(addr string) (bool, error) {
	// Make RPC call to each frontned and update their key range mapping for all frontends.
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", addr)
	if e != nil {
		return true, e
	}

	// perform the call
	var is_crashed bool
	e = conn.Call("Backend.IsCrashed", "", &is_crashed)
	if e != nil {
		conn.Close()
		return true, e
	}

	return is_crashed, nil
}

func FindAliveBackendWithRetryLogic(nodeIdx int, BackendHashToBackendAddrMap map[int]string, SortedListOfBackendHashIds []int) (int, error) {
	// Retry Logic in the case that the identified new tail to send logs is dead
	for i := 0; i < len(SortedListOfBackendHashIds); i++ {
		new_idx := GetIndexOnRing((nodeIdx + i), len(SortedListOfBackendHashIds))
		is_crashed, err := CheckBackendCrashed(BackendHashToBackendAddrMap[SortedListOfBackendHashIds[new_idx]])
		if !is_crashed && err == nil {
			return new_idx, nil
		}
	}

	return -1, errors.New("Could not find any alive backends by retry logic")
}
