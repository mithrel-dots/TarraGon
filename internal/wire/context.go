package wire

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const maxContextBytes = 4096

var ErrInvalidContext = errors.New("context must be a valid UTF-8 string, at most 4096 bytes, with no NUL")

// ValidContext checks a context supplied as a Go string.
func ValidContext(value string) bool {
	return len(value) <= maxContextBytes && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// ContextField decodes an optional context without failing its enclosing JSON
// object; callers decide how to reject an invalid field.
type ContextField struct {
	Value   string
	Invalid bool
}

func (c *ContextField) UnmarshalJSON(raw []byte) error {
	if c.Invalid || string(raw) == "null" {
		return nil
	}
	// A valid byte uses at most six bytes in JSON, plus the enclosing quotes.
	c.Invalid = raw[0] != '"' || len(raw) > 6*maxContextBytes+2 ||
		json.Unmarshal(raw, &c.Value) != nil || !validContextToken(c.Value, raw)
	return nil
}

func validContextToken(value string, token []byte) bool {
	if !ValidContext(value) {
		return false
	}
	if !strings.ContainsRune(value, utf8.RuneError) {
		return true
	}
	// encoding/json silently replaces invalid bytes and unpaired surrogates.
	raw := string(token)
	explicit := strings.Count(raw, "\uFFFD")
	for {
		_, escape, ok := strings.Cut(raw, `\`)
		if !ok {
			break
		}
		if len(escape) >= 5 && strings.EqualFold(escape[:5], "ufffd") {
			explicit++
		}
		raw = escape[1:] // skip the escaped character, including a literal backslash
	}
	return explicit == strings.Count(value, "\uFFFD")
}
