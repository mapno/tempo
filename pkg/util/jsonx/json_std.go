//go:build !((amd64 && go1.17 && !go1.27) || (arm64 && go1.20 && !go1.27))

package jsonx

// See json_sonic.go for the rationale behind this build-tag split. On archs
// where sonic cannot be imported (e.g. 32-bit linux/arm in release builds, or
// any non-amd64/non-arm64 arch), fall back to encoding/json. Output bytes are
// equivalent for the types this package is used with in Tempo (no maps with
// non-deterministic ordering, no custom Marshalers relying on sonic specifics).

import "encoding/json"

// Marshal returns the JSON encoding of v.
func Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal parses the JSON-encoded data and stores the result in v.
func Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
