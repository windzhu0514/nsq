package nsqd

// 正在投递中的消息队列 最小堆（经过排序的完全二叉树 所有父节点小于2个子节点）
// 模仿标准库的heap实现，heap里包含interface{}，减低了性能
type inFlightPqueue []*Message

func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)

	// 长度和容量相同时 容量扩容为2倍
	if n+1 > c {
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}

	// 切片长度加1 因为不用append 需要手动扩大长度
	*pq = (*pq)[0 : n+1]
	x.index = n
	(*pq)[n] = x
	pq.up(n)
}

func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	pq.Swap(0, n-1)          // 把优先级最小的根节点和最后一个节点（优先级不一定是最大的）互换
	pq.down(0, n-1)          // 下沉第一个节点
	if n < (c/2) && c > 25 { // 缩容
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	// 取出最后一个
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1] // 长度减1
	return x
}

func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	if n-1 != i {
		pq.Swap(i, n-1) // 交换到最后
		pq.down(i, n-1) // 尝试下沉交换过来的节点（所有交换都发生在i的子树里面）
		pq.up(i)        // 尝试上浮交换过来的节点（交换过来的节点可能是其他子树上的 会比父节点更小）
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// PeekAndShift 判断优先级队列第一个节点的消息是否已经超时 <=max 说明已经超时
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	if x.pri > max {
		return nil, x.pri - max
	}
	pq.Pop()

	return x, 0
}

// 优先级小的往上浮 找到j应该在的位置
func (pq *inFlightPqueue) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		// 当j=-1 或者j=0时 i==j
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		j = i
	}
}

func (pq *inFlightPqueue) down(i, n int) {
	for {
		// 判断左孩子和右孩子哪个更小
		j1 := 2*i + 1          // 左孩子
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1                                                     // left child
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri { // 如果左孩子大于等于右孩子
			j = j2 // = 2*i + 2  // right child
		}

		// 对比最小的孩子和要下沉的节点的优先级
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
