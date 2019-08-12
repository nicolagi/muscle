package tree

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

type cryptography struct {
	block cipher.Block
}

func newCryptography(key []byte) (*cryptography, error) {
	block, err := aes.NewCipher(key)
	return &cryptography{block}, err
}

func (c *cryptography) encrypt(in []byte) (out []byte, err error) {
	iv := make([]byte, c.block.BlockSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("could not read random bytes for nonce: %v", err)
	}
	return append(iv, c.xor(in, iv)...), nil
}

func (c *cryptography) decrypt(in []byte) (out []byte) {
	iv := in[:c.block.BlockSize()]
	in = in[c.block.BlockSize():]
	return c.xor(in, iv)
}

func (c *cryptography) xor(in, iv []byte) (out []byte) {
	ctr := cipher.NewCTR(c.block, iv)
	out = make([]byte, len(in))
	ctr.XORKeyStream(out, in)
	return
}
