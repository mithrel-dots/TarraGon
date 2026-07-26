package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/mithrel-dots/tarragon/internal/wire"
)

// Older daemons may interleave unrelated updates with a control reply.
func readControlReply(conn io.Reader, want string, reply any) error {
	scanner := wire.NewScanner(conn)
	for {
		var header struct {
			Type  string `json:"type"`
			Error string `json:"error"`
		}
		if err := wire.ReadMsg(scanner, &header); err != nil {
			return err
		}
		switch header.Type {
		case want:
			return json.Unmarshal(scanner.Bytes(), reply)
		case "error":
			return fmt.Errorf("daemon error: %s", header.Error)
		}
	}
}
