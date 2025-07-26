// Package syncapi exposes the *data* formats needed by trie-/code-sync.
// It has NO networking logic.
package syncapi

type LeafsRequest struct {
	Root  []byte
	Start []byte
	End   []byte
	Limit uint16
}

type LeafsResponse struct {
	Keys [][]byte
	Vals [][]byte
}

type CodeRequest struct {
	Hash []byte
}

type CodeResponse struct {
	Code []byte
}
