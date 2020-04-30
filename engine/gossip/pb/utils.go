package pb

import (
	"crypto/rand"
	"encoding/binary"
)

// NewMessageID generates new message id.
func NewMessageID() (id uint64, err error) {
	var bins [8]byte

	if _, err = rand.Read(bins[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bins[:]), nil
}
