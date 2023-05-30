package wormdb

type searchTree struct {
	Index []uint8
	Tree  []searchTree
	Start uint32
}

func (st *searchTree) make(val uint8) *searchTree {
	if len(st.Tree) < 128 {
		for i, j := range st.Index {
			if j == val {
				return &st.Tree[i]
			}
		}
		st.Index = append(st.Index, val)
		st.Tree = append(st.Tree, searchTree{})
		return &st.Tree[len(st.Tree)-1]
	}
	if len(st.Tree) < 256 {
		tree := make([]searchTree, 256)
		for _, i := range st.Index {
			tree[i] = st.Tree[i]
		}
		st.Tree = tree
		st.Index = nil
	}
	return &st.Tree[val]
}
func (st *searchTree) get(val uint8) *searchTree {
	if len(st.Tree) < 256 {
		bi, bj := 0, uint8(0)
		for i, j := range st.Index {
			if j == val {
				return &st.Tree[i]
			}
			if bj < val && j > bj {
				bi, bj = i, j
			}
		}
		return &st.Tree[bi]
	}
	return &st.Tree[val]
}
