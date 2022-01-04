package conduit

import (
	"testing"
)

func TestProcessing(t *testing.T) {
	describe("Processing", t)

	// ch := make(chan conduit.Transformable, 3)
	generateFiles("ingress", 3, "test-file")
	// data := conduit.Extract(, ch)
}
