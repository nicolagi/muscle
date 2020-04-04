package block

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

type blockCipher struct {
	cipher.Block
}

func newBlockCipher(key []byte) (blockCipher, error) {
	block, err := aes.NewCipher(key)
	return blockCipher{block}, err
}

func (c *blockCipher) encrypt(cleartext []byte) (ciphertext []byte, err error) {
	iv := make([]byte, c.BlockSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("could not read random bytes for nonce: %v", err)
	}
	return append(iv, c.xor(cleartext, iv)...), nil
}

func (c *blockCipher) decrypt(ciphertext []byte) (cleartext []byte) {
	iv := ciphertext[:c.BlockSize()]
	ciphertext = ciphertext[c.BlockSize():]
	return c.xor(ciphertext, iv)
}

func (c *blockCipher) xor(in, iv []byte) (out []byte) {
	ctr := cipher.NewCTR(c.Block, iv)
	out = make([]byte, len(in))
	ctr.XORKeyStream(out, in)
	return
}
