//go:build (amd64 && go1.17 && !go1.27) || (arm64 && go1.20 && !go1.27)

// Package jsonx wraps the JSON encoder used across Tempo. On amd64/arm64 with
// supported Go versions it delegates to github.com/bytedance/sonic for the
// fast path; on every other arch (notably 32-bit linux/arm in our release
// matrix) it falls back to encoding/json.
//
// Why this package exists:
//
// sonic ships a compat fallback that *should* let it compile everywhere,
// but vendor/github.com/bytedance/sonic/internal/utils/skip.go contains a
// deliberate compile-time tripwire on 32-bit archs:
//
//	const _Sonic_Not_Support_32Bit_Arch__Checking_32Bit_Arch_Here = ...
//
// Because that file is transitively imported by sonic/ast (which is always
// built), any direct `import "github.com/bytedance/sonic"` breaks the
// linux/arm release build. This package isolates sonic behind build tags so
// callers can stay arch-agnostic.
//
// Do not import github.com/bytedance/sonic directly from anywhere else in
// the tree — a depguard lint rule rejects it.
//
// Build tags above mirror those in github.com/bytedance/sonic/sonic.go so
// that this file is only compiled on archs+versions where sonic itself
// compiles its native path.
package jsonx

//nolint:depguard // jsonx is the one place sonic is allowed; see package doc above
import "github.com/bytedance/sonic"

// Marshal returns the JSON encoding of v.
func Marshal(v any) ([]byte, error) { return sonic.Marshal(v) }

// Unmarshal parses the JSON-encoded data and stores the result in v.
func Unmarshal(data []byte, v any) error { return sonic.Unmarshal(data, v) }
