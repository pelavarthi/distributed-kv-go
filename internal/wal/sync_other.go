//go:build !linux

package wal

import "os"

// syncFile flushes appended data to disk. Only Linux gets
// fdatasync; everywhere else a full fsync is the portable call
// with the same durability guarantee.
func syncFile(f *os.File) error {
	return f.Sync()
}
