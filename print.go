package wormdb

import "fmt"

func (w *WormDB) Print() {
	for i, tt := range w.Tree {
		if len(tt.Tree) > 0 {
			printTree(tt, fmt.Sprintf("[%c]", i))
		}
	}
}
func printTree(t searchTree, indent string) {
	if t.Start > 0 {
		fmt.Println(indent, t.Start)
	}
	if len(t.Tree) < 256 {
		for i, tt := range t.Tree {
			printTree(tt, fmt.Sprintf("%s[%c]", indent, t.Index[i]))
		}
	} else {
		for i, tt := range t.Tree {
			printTree(tt, fmt.Sprintf("%s[%c]", indent, i))
		}
	}
}
