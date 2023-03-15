package packedconn

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Data order
// | Start | Len        | Description |
// | ----- | ---------- | ------------|
// | 0     | 1          | uint8 indicating count of layers of object |
// | 1     | 3 * layers | uint8 len indicator, followed by uint16 offset indicator |
// | dyn.  | dyn        | all layers, starting with highest layer first

type Connections struct {
	data []byte
}

const layerPos = 0

func NewWithMaxLayer(maxLayer uint8) (Connections, error) {
	if maxLayer+1 > math.MaxUint8 {
		return Connections{}, fmt.Errorf("max supported layer is %d",
			math.MaxUint8-1)
	}
	c := Connections{
		// TODO: low initial size and grow dynamically
		data: make([]byte, 100000),
	}

	c.initLayers(maxLayer)

	return c, nil
}

func (c *Connections) ReplaceLayer(layer uint8, conns []uint64) {
	if layer != 0 {
		panic("only changing layer zero supported atm")
	}

	// create a temporary buffer that is guaranteed to fit everything. The
	// over-allocation does not matter, this buffer won't stick around, so the
	// only real downside is the overhead on GC. If this because noticeable this
	// buffer would be suitable to use pooling.
	buf := make([]byte, len(conns)*binary.MaxVarintLen64)

	sort.Slice(conns, func(a, b int) bool { return conns[a] < conns[b] })
	last := uint64(0)
	offset := 0
	for _, raw := range conns {
		delta := raw - last
		last = raw
		offset += binary.PutUvarint(buf[offset:], delta)
	}

	buf = buf[:offset]

	c.replaceLayer(layer, buf, uint8(len(conns)))
}

func (c Connections) LenAtLayer(layer uint8) int {
	if layer >= c.layers() {
		panic(fmt.Sprintf("only has %d layers", c.layers()))
	}

	return int(c.layerLength(layer))
}

func (c Connections) GetLayer(layer uint8) []uint64 {
	conns := make([]uint64, c.layerLength(layer))

	offset := c.layerOffset(layer)
	end := c.layerEndOffset(layer)
	last := uint64(0)
	i := 0
	for offset < end {
		val, n := binary.Uvarint(c.data[offset:])
		offset += uint16(n)

		// TODO: allocate exact size, don't rely on dynamic growing
		conns[i] = last + val
		last += val
		i++
	}

	return conns
}

func (c *Connections) growLayerBy(layer uint8, delta uint16) {
	if layer != 0 {
		panic("only layer 0 can be resized right now")
	}

	// TODO: check cap and grow backing array if required
	c.data = c.data[:len(c.data)+int(delta)]
}

func (c *Connections) initLayers(maxLayer uint8) {
	layers := maxLayer + 1
	c.data[layerPos] = layers

	// TODO: ensure correct minimum capacity
	c.data = c.data[:c.initialLayerOffset()]

	layer := maxLayer
	for {
		c.setLayerLength(layer, 0)
		c.setLayerOffset(layer, c.initialLayerOffset())

		if layer == 0 {
			break
		}
		layer--
	}
}

// number of layers, e.g. if the maxLayer is 7, the number of layers is 8, as 0
// is a valid layer
func (c *Connections) layers() uint8 {
	return c.data[layerPos]
}

func (c *Connections) layerLengthPos(layer uint8) int {
	return 1 + int(layer*3)
}

func (c *Connections) layerLength(layer uint8) uint8 {
	return c.data[c.layerLengthPos(layer)]
}

func (c *Connections) setLayerLength(layer, length uint8) {
	c.data[c.layerLengthPos(layer)] = length
}

func (c *Connections) layerOffsetPos(layer uint8) int {
	return c.layerLengthPos(layer) + 1
}

func (c *Connections) layerOffset(layer uint8) uint16 {
	return binary.LittleEndian.Uint16(c.data[c.layerOffsetPos(layer):])
}

func (c *Connections) layerEndOffset(layer uint8) uint16 {
	if layer == 0 {
		return uint16(len(c.data))
	}

	return c.layerOffset(layer - 1)
}

func (c *Connections) layerSize(layer uint8) uint16 {
	return c.layerEndOffset(layer) - c.layerOffset(layer)
}

func (c *Connections) setLayerOffset(layer uint8, offset uint16) {
	binary.LittleEndian.PutUint16(c.data[c.layerOffsetPos(layer):], offset)
}

func (c *Connections) initialLayerOffset() uint16 {
	// 1 byte for the uint8 indicating len
	return uint16(1 + c.layers()*3)
}

func (c *Connections) replaceLayer(layer uint8, contents []byte,
	length uint8,
) {
	// resize
	oldLayerSize := c.layerSize(layer)
	newLayerSize := uint16(len(contents))

	if oldLayerSize > newLayerSize {
		// TODO
		panic("shrinking not supported yet")
	} else if newLayerSize > oldLayerSize {
		c.growLayerBy(layer, newLayerSize-oldLayerSize)
	}
	copy(c.data[c.layerOffset(layer):], contents)
	c.setLayerLength(layer, length)
}