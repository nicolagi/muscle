package block

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"
)

func TestBlockCipherRoundTrip(t *testing.T) {
	for _, size := range []int{16, 24, 32} {
		t.Run(fmt.Sprintf("decrypt is inverse to encrypt %d", size), func(t *testing.T) {
			key := make([]byte, size)
			rand.Read(key)
			cipher, err := newBlockCipher(key)
			if err != nil {
				t.Fatal(err)
			}
			f := func(cleartext []byte) bool {
				ciphertext, err := cipher.encrypt(cleartext)
				if err != nil {
					t.Log(err)
					return false
				}
				return bytes.Equal(cipher.decrypt(ciphertext), cleartext)
			}
			if err := quick.Check(f, nil); err != nil {
				t.Error(err)
			}
		})
	}
}
