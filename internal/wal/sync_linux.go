//go:build linux

package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncFile flushes appended data to disk. fdatasync skips file
// metadata such as the modification time, which an append-only
// log never reads back, so it costs less than a full fsync while
// giving the same durability for the data itself.
func syncFile(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
