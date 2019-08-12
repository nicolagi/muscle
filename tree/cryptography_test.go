package tree

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	crypto, err := newCryptography(key)
	if err != nil {
		t.Fatal(err)
	}
	cleartext := []byte("any string would do")
	ciphertext, err := crypto.encrypt(cleartext)
	assert.Nil(t, err)
	assert.NotEqual(t, cleartext, ciphertext)
	assert.Equal(t, cleartext, crypto.decrypt(ciphertext))
}
