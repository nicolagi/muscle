// +build debug

package debug

func Assert(condition bool) {
	if !condition {
		panic("assertion failure")
	}
}
