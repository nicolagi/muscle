package storage

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func RandomKey() Key {
	return RandomPointer().Key()
}

func RandomValue() []byte {
	size := rand.Intn(500)
	value := make([]byte, size)
	rand.Read(value)
	return value
}

func RandomPair() (Key, Value) {
	return RandomKey(), RandomValue()
}

func TestRandomKey(t *testing.T) {
	a := RandomPointer()
	b := RandomPointer()
	assert.Equal(t, a.Len(), uint8(32))
	assert.Equal(t, b.Len(), uint8(32))
	assert.NotEqual(t, a, b)
}
