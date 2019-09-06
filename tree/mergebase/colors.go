package mergebase

import (
	"fmt"
	"hash/adler32"
)

var colors = newLabelColors()

type labelColors struct {
	available [64]string
	assigned  map[string]int
}

func newLabelColors() *labelColors {
	var colors labelColors
	colors.assigned = make(map[string]int)
	for r := uint8(0); r < 4; r++ {
		for g := uint8(0); g < 4; g++ {
			for b := uint8(0); b < 4; b++ {
				i := 16*r + 4*g + b
				colors.available[i] = fmt.Sprintf("#%x", []byte{r * 85, g * 85, b * 85})
			}
		}
	}
	return &colors
}

func (colors *labelColors) colorFor(label string) string {
	if i, ok := colors.assigned[label]; ok {
		return colors.available[i]
	}
	sum := adler32.Checksum([]byte(label))
	colors.assigned[label] = int(sum % 64)
	return colors.colorFor(label)
}
