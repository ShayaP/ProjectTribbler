package util

type KeyRange struct {
	Min float32 // Exclusive
	Max float32 // Inclusive
}

// Input struct for FAWN backends.
type KeyValueIdReplication struct {
	Key   string
	Value string
	Id    int
	R     int
}

// Return struct for FAWN backends.
type ResponseId struct {
	Response string
	Id       int
}

// Arguemnt struct for send log file
type SendLogArgs struct {
	IsWrapAround bool
	KR           KeyRange
	Addr         string
}

// Arguemnt struct for send log file
type ReceieveLogArgs struct {
	IsWrapAround bool
	KR           KeyRange
	Log          []byte
}

// Constructors for Input and Return structs
func KVIR(k, v string, id int, r int) *KeyValueIdReplication {
	return &KeyValueIdReplication{k, v, id, r}
}
func RI(r string, id int) *ResponseId { return &ResponseId{r, id} }
