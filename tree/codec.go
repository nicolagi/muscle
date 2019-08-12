package tree

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
)

// Codec defines how we serialize and deserialize our types.
type Codec interface {
	encodeNode(src *Node) (data []byte, err error)
	encodeRevision(rev *Revision) (data []byte, err error)

	decodeNode(data []byte, node *Node) (err error)
	decodeRevision(data []byte, rev *Revision) (err error)
}

type multiCodec struct {
	mu            sync.Mutex
	codecs        map[byte]Codec
	latestVersion byte
}

var (
	errNoCodec = errors.New("no codec found")
)

func newMultiCodec() *multiCodec {
	return &multiCodec{
		codecs: make(map[byte]Codec),
	}
}

func (mc *multiCodec) register(version byte, c Codec) {
	mc.mu.Lock()
	mc.codecs[version] = c
	if version > mc.latestVersion {
		mc.latestVersion = version
	}
	mc.mu.Unlock()
}

// Encodes node with most recent codec.
func (mc *multiCodec) encodeNode(node *Node) (data []byte, err error) {
	return mc.codecFor(mc.latestVersion).encodeNode(node)
}

// Encodes revision with most recent codec.
func (mc *multiCodec) encodeRevision(rev *Revision) (data []byte, err error) {
	return mc.codecFor(mc.latestVersion).encodeRevision(rev)
}

// Decodes node with the correct codec, based on version.
func (mc *multiCodec) decodeNode(data []byte, node *Node) (err error) {
	c := mc.codecFor(data[0])
	if c == nil {
		// Information leak if this happens, but I need the
		// information if this happens.  Could happen
		// accidentally running an old binary that doesn't
		// have the codecs used in earlier phases of
		// development.
		parent := "(nil)"
		if node.parent != nil {
			parent = node.parent.Path()
		}
		readable := bytes.NewBuffer(nil)
		for b := data[1:]; len(b) > 0; {
			r, size := utf8.DecodeRune(b)
			if r != utf8.RuneError {
				readable.WriteRune(r)
			}
			b = b[size:]
		}
		log.WithFields(log.Fields{
			"key":    node.pointer.Hex(),
			"codec":  data[0],
			"parent": parent,
			"data":   fmt.Sprintf("%x", data),
		}).Error("no codec found")
		return errNoCodec
	}
	return c.decodeNode(data[1:], node)
}

// Decodes revision with the correct codec, based on version.
func (mc *multiCodec) decodeRevision(data []byte, rev *Revision) (err error) {
	return mc.codecFor(data[0]).decodeRevision(data[1:], rev)
}

func (mc *multiCodec) codecFor(version byte) Codec {
	mc.mu.Lock()
	c := mc.codecs[version]
	mc.mu.Unlock()
	return c
}

func newStandardCodec() *multiCodec {
	codec := newMultiCodec()
	codec.register(13, &codecV13{})
	return codec
}
