// Package filelock serializes operations on directory entries across processes.
package filelock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// Dir takes an exclusive advisory lock. Close the directory to release it.
// Unlike a lock on a config file or a socket, it survives entry replacement.
func Dir(path string) (*os.File, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	for {
		err = unix.Flock(int(dir.Fd()), unix.LOCK_EX)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		dir.Close()
		return nil, fmt.Errorf("lock directory %s: %w", path, err)
	}
	return dir, nil
}
