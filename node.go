package bolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// 文件概述：
// 对node所存元素和node间关系的相关操作，节点内所存元素的增删，加载和落盘，访问孩子兄弟元素，拆分与合并的详细逻辑

// node和page的对应关系：文件系统中一组连续的物理page，加载到内存中成为一个逻辑page，进而转化为一个node

//------------------------------------------------------ node -----------------------------------------------------------//

// node 表示内存中一个反序列化后的 page
type node struct {
	bucket *Bucket //指针：指向所在的bucket
	isLeaf bool    //标志位：是否为叶子节点

	// 用于调整，维持B+树
	unbalanced bool   // 标志位：是否需要进行合并
	spilled    bool   // 标志位：是否需要进行拆分和落盘
	key        []byte // 所含第一个元素的key
	pgid       pgid   // 对应的page id
	parent     *node  // 父节点指针
	children   nodes  // 子节点们(只包含加载到内存中到部分孩子)
	inodes     inodes // 所存元素的元信息，对于分支节点是key+pgid的数组，对于叶子节点是kv数组
}

//inode 表示node所含的内部元素
type inode struct {
	// 指向的元素不一定加载到了内存

	flags uint32 // 用于leaf node，区分是正常value还是subbucket
	pgid  pgid
	key   []byte // 分支节点使用：指向的分支/叶子节点的page id
	value []byte // 叶子节点使用：叶子节点所存储的数据
}
type inodes []inode

//root 返回node的root节点
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

//minKeys 返回 node inodes 的最小数量
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1 // 叶子节点，只有一个节点
	}
	return 2 // 非叶节点，最少有它本身和它的一个叶子节点
}

//size 返回序列化后node的大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}
	return sz
}

//sizeLessThan 判断序列化后node的大小，如果超过v，返回false，否则返回true
func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
}

//pageElementSize 根据node类型返回page大小
func (n *node) pageElementSize() int {
	// node只有两种类型：leaf和brach
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

//childAt 返回给定索引处的子节点
func (n *node) childAt(index int) *node {
	// 叶子节点，不可能挂子节点
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	// 创建一个子节点
	return n.bucket.node(n.inodes[index].pgid, n)
}

//childIndex 返回给定节点的索引
func (n *node) childIndex(child *node) int {
	// 在inodes里面匹配child的key相同的节点的下标
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

//numChildren 返回节点的子节点数量
func (n *node) numChildren() int {
	return len(n.inodes)
}

//nextSibling 返回具有相同父亲节点的下一个节点
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

//prevSibling 返回具有相同父节点的前一个节点
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

//put 新增元素
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// 找到插入点下标
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// 如果key是新增而非替换，则需要为待插入节点腾出空间（index后面元素整体右移，腾出一个位置）
	exact := len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey)
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	// 给插入/替换的元素赋值
	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

//del 在node中删除一个key
func (n *node) del(key []byte) {
	// 寻找key的下表
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// 如果节点找不到，直接返回
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// 删除node的inodes中index位置的元素
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// 将节点标记为 需要重新平衡
	n.unbalanced = true
}

//read 读取逻辑page转化为逻辑node
func (n *node) read(p *page) {
	// 初始化元信息
	n.pgid = p.id
	n.isLeaf = (p.flags & leafPageFlag) != 0
	n.inodes = make(inodes, int(p.count))

	// 加载所包含元素 inodes
	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// 用第一个元素的key作为该node的key，以便父节点以此作为索引，进行查找和路由
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

//write 将 node 写入 1个或多个 page
func (n *node) write(p *page) {
	// 初始化page
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	// 防止溢出
	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))
	if p.count == 0 {
		return
	}

	// 遍历，写入
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// Write the page element.
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// key+value 长度太长，重新分配指针
		klen, vlen := len(item.key), len(item.value)
		if len(b) < klen+vlen {
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:] // 扩容
		}

		// 写入b
		copy(b[0:], item.key)
		b = b[klen:]
		copy(b[0:], item.value)
		b = b[vlen:]
	}

	// DEBUG ONLY: n.dump()
}

//split node 切分成数个指定大小的node（大小不同）
func (n *node) split(pageSize int) []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		a, b := node.splitTwo(pageSize)
		// a添加到列表中，b留下来继续切
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		node = b
	}

	return nodes
}

//splitTwo node 切分成两个
func (n *node) splitTwo(pageSize int) (*node, *node) {
	// node足够小，不必再切
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// 确定阈值
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// 根据阈值确定切分的index
	splitIndex, _ := n.splitIndex(threshold)

	// 如果被拆分的node没有父节点，则需要新建一个
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// 创建一个空节点next，next和node是兄弟节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// 将node节点的部分元素拆分到next
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// 更新分裂计数
	n.bucket.tx.stats.Split++

	return n, next
}

//splitIndex 根据阈值给定切分的index
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// minimum number of keys or 增加其他key将超过阈值
		if i >= minKeysPerPage && sz+elsize > threshold {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	}

	return
}

// spill 溢出操作，拆分节点
func (n *node) spill() error {
	var tx = n.bucket.tx
	// 溢出？
	if n.spilled {
		return nil
	}

	// 不能使用range，因为n.children的切片会一直变化
	sort.Sort(n.children) // key sort
	for i := 0; i < len(n.children); i++ {
		// 递归给各个child进行spill
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// 不再需要children，避免向上递归调用spill形成回路死循环
	n.children = nil

	// 按照 pagesize 将node 分裂出若干个新node，新node们和当前node恭喜同一个父node，返回的nodes中包含当前node
	var nodes = n.split(tx.db.pageSize)
	// 释放这些node占用的page，因为随后要分配新的page
	// transaction commit 是只会向磁盘写入当前transacation分配的脏页
	for _, node := range nodes {
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		//分配page
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// node 写入 page
		if p.id >= tx.meta.pgid { // 分配的page id 大于当前使用的最大page id
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		node.pgid = p.id
		node.write(p)
		node.spilled = true //标志该node已经spill过了

		// Insert into parent inodes.
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			// ？？？
			// key属性下沉，向父节点更新或者添加key和pointer，以指向分裂产生的新node，将父node的key设为第一个子node的第一个key
			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// 更新spill 计数
		tx.stats.Spill++
	}

	// 从根节点处递归处理完所有子节点的spill的过程后，若是根节点需要分裂，则它分裂后将产生新的根
	// 对于新产生的根，我们也需要将其spill，并且同样对其孩子spill
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill() //防止spill过程又有新的parent节点产生
	}

	return nil
}

//rebalance 重新平衡操作，合并节点
func (n *node) rebalance() {
	//已重新平衡过
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// 总平衡次数+1
	n.bucket.tx.stats.Rebalance++

	// 该节点的容量超过一个页的25% || inode的个数少于最少key数 则需要重新平衡
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// n是根节点 特殊处理
	if n.parent == nil {
		// n 非叶子节点 && n只有一个子节点
		if !n.isLeaf && len(n.inodes) == 1 {
			// 将 n的孩子节点child 向上移 顶替掉n的位置
			// 具体操作如下：

			// 将n的子节点child的inodes 拷贝到 根节点上
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// 将子节点的child的孩子节点的父亲 更新为 根节点
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// 移除 child
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// 如果n没有任何子节点，则删除n
	// 1.先将父节点上 n的key和指向n的pointer删除
	// 2.然后在删除n
	// 3.由于父节点出现了删除，对父节点重新进行平衡操作
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	// 确保 n 至少有一个兄弟节点
	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// 找到兄弟节点
	var target *node
	var useNextSibling = n.parent.childIndex(n) == 0
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// 根据n和target的位置关系，决定究竟上谁合并到谁：位置靠右的节点合并到位置靠左的节点
	if useNextSibling {
		// 对target 的子节点进行 re parent 操作，新parent：n
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// target的inodes 移动到 n的inodes
		// 删除释放 target
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// 对n的inodes 进行re parent 操作，新parent：target
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// n的inodes 移动到 target的inodes
		// 删除释放n
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// 进行了删除释放操作，所以要对父节点进行重新平衡操作
	n.parent.rebalance()
}

//removeChild 移除特定的child，不影响inodes
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

//dereference 利用mmap进行解引用，可以避免指向旧数据
func (n *node) dereference() {
	if n.key != nil {
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	n.bucket.tx.stats.NodeDeref++
}

// free 将节点的page 加入到 freeList中
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

//------------------------------------------------------ nodes -----------------------------------------------------------//

type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1
}
