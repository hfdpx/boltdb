package bolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// key的最大长度，单位：字节
	MaxKeySize = 32768

	// value的最大长度，单位：字节
	MaxValueSize = (1 << 31) - 2
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// 默认的bucket中节点的填充百分比阈值，当node中key的个数或者size超过整个node容量的某个百分比阈值之后，节点必须分裂为两个节点，这是为了防止B+树中插入kv对时引发频繁的再平衡操作
const DefaultFillPercent = 0.5

// bucket：db中一组kv对的集合
type Bucket struct {
	*bucket                        // 头部
	tx          *Tx                // 当前bucket所属的事务
	buckets     map[string]*Bucket // 子bucket cache
	page        *page              // inline page reference：当bucket很小的时候，可以存储在此内联page中
	rootNode    *node              // bucket 根节点
	nodes       map[pgid]*node     // node cache
	FillPercent float64            // node分裂阈值：当确定大多数写入操作是中尾部添加时，增大此阈值是有帮助的
}

// bucket 头部
type bucket struct {
	root     pgid   // bucket 根节点 pageID
	sequence uint64 // 序列号，自增
}

// 返回一个与事务关联的新bucket
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// 返回bucket的tx
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// 返回bucket的根节点：之所以是返回pageID，因为执行的node不一定加载到了内存，当你需要访问此node的时候，会按需将page转化为node
func (b *Bucket) Root() pgid {
	return b.root
}

// 判断该bucket事务是否是读写事务
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// 创建一个该bucket上的游标，此游标仅事务存活期间有效
func (b *Bucket) Cursor() *Cursor {
	// Update transaction statistics.
	b.tx.stats.CursorCount++

	// Allocate and return a cursor.
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// 根据name查找bucket，bucket不存在则返回nil，同样：该bucket也仅仅在事务生存期间有效
func (b *Bucket) Bucket(name []byte) *Bucket {
	// 先看看 buckets缓存记录有没有，有则直接返回
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// 寻找bucket
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// 没找到 || 找到的不是bucket，返回nil
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// 找到数据后将其转化成bucket，并且记录到buckets缓存中
	var child = b.openBucket(v)
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}

	return child
}

// 通过原始数据打开一个bucket
func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	// If unaligned load/stores are broken on this arch and value is
	// unaligned simply clone to an aligned byte array.
	// 判断是否对齐？
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0

	// value没有对齐则clone一个一摸一样的
	if unaligned {
		value = cloneBytes(value)
	}

	// If this is a writable transaction then we need to copy the bucket entry.
	// Read-only transactions can point directly at the mmap entry.
	// 在读写tx中，将value深度拷贝到新bucket头部
	// 在只读tx中，将新bucket头指向value即可
	if b.tx.writable && !unaligned {
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// Save a reference to the inline page if the bucket is inline.
	// 如果bucket是内联的，则保存对内联页面的引用：即将新bucket的page字段指向value中内置page的起始位置
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

// 创建一个指定名称的bucket，同样：仅在事务生存期间有效
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil { // tx 关闭
		return nil, ErrTxClosed
	} else if !b.tx.writable { // 非可写tx
		return nil, ErrTxNotWritable
	} else if len(key) == 0 { // bucket name长度不符合要求
		return nil, ErrBucketNameRequired
	}

	// Move cursor to correct position.
	// 通过游标寻找bucket
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key.
	// 已存在该bucket则返回error
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}

	// Create empty, inline bucket.
	// 创建一个空的bucket
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	// Insert into node.
	// 将新bucket写入node
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	// Since subbuckets are not allowed on inline buckets, we need to
	// dereference the inline page, if it exists. This will cause the bucket
	// to be treated as a regular, non-inline bucket for the rest of the tx.
	// 因为当前bucket已经包含了刚创建的新bucket，所以是非内联bucket，得将内联引用b.page置为nil
	b.page = nil

	// 这样查找一遍再返回，可以缓存该bucket，下次查起来更快
	return b.Bucket(key), nil
}

// 创建一个指定名称的bucket，如果它还不存在的话，同样：仅在事务生存期间有效
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// 删除指定bucket
func (b *Bucket) DeleteBucket(key []byte) error {
	// tx 已关闭或非可写
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 查找该bucket
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if bucket doesn't exist or is not a bucket.
	// 不存在或者不是bucket，返回错误
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	// Recursively delete all child buckets.
	// 递归删除其所有的子bucket
	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove cached copy.
	// 删除该bucket在缓存中的信息
	delete(b.buckets, string(key))

	// Release all bucket pages to freelist.
	// 释放该bucket上的所有page 到 freelist
	child.nodes = nil
	child.rootNode = nil
	child.free()

	// Delete the node if we have a matching key.
	// 在node中删除该key
	c.node().del(key)

	return nil
}

// 查找此bucket中的kv对，同样：返回的值仅中事务生存期间有效
func (b *Bucket) Get(key []byte) []byte {
	// 查找
	k, v, flags := b.Cursor().seek(key)

	// Return nil if this is a bucket.
	// 找到的是bucket，返回nil
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	// 没找到，同样返回nil
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// 添加一个kv对
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil { // tx 关闭
		return ErrTxClosed
	} else if !b.Writable() { // tx 非可写
		return ErrTxNotWritable
	} else if len(key) == 0 { // key非法
		return ErrKeyRequired
	} else if len(key) > MaxKeySize { // key 非法
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize { // value 非法
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	// 查找key
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key with a bucket value.
	// key已存在：即当前欲插入的key与当前bucket中已有一个子bucket的key相同时，拒绝写入，从而保护嵌套的子bucket的引用不会被擦除，防止子bucket变成孤儿
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Insert into node.
	// key-value 插入node
	key = cloneBytes(key) // 深度拷贝key
	c.node().put(key, key, value, 0, 0)

	return nil
}

// 删除一个kv对
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil { // tx关闭
		return ErrTxClosed
	} else if !b.Writable() { // tx 非可写
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	// 查找
	c := b.Cursor()
	_, _, flags := c.seek(key)

	// Return an error if there is already existing bucket value.
	// 已存在，但是一个bucket
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Delete the node if we have a matching key.
	// 删除key
	c.node().del(key)

	return nil
}

// 返回bucket当前序列号
func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

// 更新bucket序列号
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	// ？？？
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence = v
	return nil
}

// 递增bucket序列号并返回
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// 为bucket中的每一个kv对执行一个函数，函数返回错误则停止迭代
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// 返回bucket的状态信息
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1 // bucket数量+1
	if b.root == 0 {
		s.InlineBucketN += 1 // 内联bucket数量+1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 { // 该page为 叶子节点类型的page
			s.KeyN += int(p.count)

			// used totals the used bytes for the page
			// 此page 使用的 总字节数
			used := pageHeaderSize

			if p.count != 0 {
				// If page has any elements, add all element headers.
				// kv结构体头部大小 * kv数量
				used += leafPageElementSize * int(p.count-1)

				// Add all element key, value sizes.
				// The computation takes advantage of the fact that the position
				// of the last element's key/value equals to the total of the sizes
				// of all previous elements' keys and values.
				// It also includes the last element's header.
				lastElement := p.leafPageElement(p.count - 1)                        // 最后一个kv对
				used += int(lastElement.pos + lastElement.ksize + lastElement.vsize) // 最后一个kv对位置=之前所有kv对大小+最后一个kv对头部
			}

			if b.root == 0 { // 内联bucket
				// For inlined bucket just update the inline stats
				s.InlineBucketInuse += used
			} else {
				// For non-inlined bucket update all the leaf stats
				// 非内联bucket 叶子节点类型page 状态 更新（因为该page是叶子节点类型的page）
				s.LeafPageN++
				s.LeafInuse += used
				s.LeafOverflowN += int(p.overflow)

				// Collect stats from sub-buckets.
				// Do that by iterating over all element headers
				// looking for the ones with the bucketLeafFlag.
				// 从子bucket收集状态信息
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 { // 该元素是子bucket
						// For any bucket element, open the element value
						// and recursively call Stats on the contained bucket.
						subStats.Add(b.openBucket(e.value()).Stats()) // 递归迭代
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 { // 该page是 分支节点类型page
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used totals the used bytes for the page
			// Add header and all element headers.
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1)) // page 头部大小+ page中所有元素的头部大小

			// Add size of all keys and values.
			// Again, use the fact that last element's position equals to
			// the total of key, value sizes of all previous elements.
			used += int(lastElement.pos + lastElement.ksize) // 最后一个元素位置+ 最后一个元素key的大小 （分支节点类型的page 不存储value值，只提供索引功能，所以没有加上vsize）
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}

		// Keep track of maximum page depth.
		if depth+1 > s.Depth { // 记录最大深度
			s.Depth = (depth + 1)
		}
	})

	// Alloc stats can be computed from page counts and pageSize.
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize // （bucket的分支节点类型page数量 + bucket的分支节点类型page溢出数量）* page 大小
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// Add the max depth of sub-buckets to get total nested depth.
	// 加上子bucket的深度
	s.Depth += subStats.Depth
	// Add the stats for all sub-buckets
	// 加上子bucket状态
	s.Add(subStats)
	return s
}

// 对 bucket 内的每一个page 执行fn
func (b *Bucket) forEachPage(fn func(*page, int)) {
	// If we have an inline page then just use that.
	// 内联page 直接执行fn
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	// Otherwise traverse the page hierarchy.
	// 对每一个page 执行fn
	b.tx.forEachPage(b.root, 0, fn)
}

// 对 bucket内的每一个page或node 执行 fn
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// If we have an inline page or root node then just use that.
	if b.page != nil { // 内联page or node，直接执行fn
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}
func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// Execute function.
	fn(p, n, depth)

	// Recursively loop over children.
	// 子对象上递归执行
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// 将大小超过阈值的node 分解为多个node，避免引发频繁的再平衡操作
func (b *Bucket) spill() error {
	// 遍历 子bucket
	for name, child := range b.buckets {
		// If the child bucket is small enough and it has no child buckets then
		// write it inline into the parent bucket's page. Otherwise spill it
		// like a normal bucket and make the parent value a pointer to the page.
		var value []byte
		if child.inlineable() { // 该子bucket是内联bucket：那么释放子bucket，将其写入到父bucket
			child.free()
			value = child.write()
		} else {
			// 该子bucket是普通bucket，同样进行spill操作
			if err := child.spill(); err != nil {
				return err
			}

			// Update the child bucket header in this bucket.
			// 更新 子bucket的header
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket // todo ????????? *bucket后面没有使用了啊，为什么要这么写
		}

		// Skip writing the bucket if there are no materialized nodes.
		// 没有具化成node 跳过
		if child.rootNode == nil {
			continue
		}

		// Update parent node.
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name)) // 创建游标，寻找子bucket
		if !bytes.Equal([]byte(name), k) {  // 没有找到该子bucket
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		// 将子bucket的新value值更新到父bucket中
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there's not a materialized root node.
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	// spill所有子bucket后，开始spill自己：从rootNode开始，完成后更新一下rootNode
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// 处理异常
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	// 更新bucket头中的根节点页号
	b.root = b.rootNode.pgid

	return nil
}

// 判断该bucket是否是内联bucket
func (b *Bucket) inlineable() bool {
	// 通常情况下，父bucket中只保存了subbucket的bucket header，每个subbucket至少占据一个page，
	// 若subbucket中的数据很少，这样会造成磁盘空间的浪费，所以可以将该subbucket做成inline bucket，
	// 具体的做法是：将小的sunbucket的值完整的放在父bucket的leaf node上，从而减少占用的page数量）

	var n = b.rootNode

	// Bucket must only contain a single leaf node.
	// n == nil || n是分支节点
	if n == nil || !n.isLeaf {
		return false
	}

	// Bucket is not inlineable if it contains subbuckets or if it goes beyond
	// our threshold for inline bucket size.
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 { // 存在subbucket，非inline
			return false
		} else if size > b.maxInlineBucketSize() { // size 超过了inline bucket阈值，非inline
			return false
		}
	}

	return true
}

// 返回 inline bucket 阈值
func (b *Bucket) maxInlineBucketSize() int {
	return b.tx.db.pageSize / 4
}

// bucket 转 []byte：其实是将bucket写入page中
func (b *Bucket) write() []byte {
	// Allocate the appropriate size.
	var n = b.rootNode                                  // 只要将bucket 根节点写入page中就好了，其他节点可以通过索引找到
	var value = make([]byte, bucketHeaderSize+n.size()) // 分配适当大小(就是bucketheader+根节点大小)

	// Write a bucket header.
	// 向value中写入一个bucket header
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// Convert byte slice to a fake page and write the root node.
	// p是page指针，指向写入n的page
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// 再平衡操作：合并节点
func (b *Bucket) rebalance() {
	// 先对bucket中的node 进行再平衡操作
	for _, n := range b.nodes {
		n.rebalance()
	}
	// 然后对子bucket递归调用再平衡
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// 创建一个node，从指定的page和指定的父节点
func (b *Bucket) node(pgid pgid, parent *node) *node {
	// 处理异常
	_assert(b.nodes != nil, "nodes map expected")

	// Retrieve node if it's already been created.
	// 判断要创建的节点是否已存在
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a node and cache it.
	// 创建一个node
	n := &node{bucket: b, parent: parent}
	if parent == nil { // 如果其parent is nil，则将新建节点n置为bucket的根节点
		b.rootNode = n
	} else { // 在parent中缓存新建的n节点
		parent.children = append(parent.children, n)
	}

	// Use the inline page if this is an inline bucket.
	// 定位到对应page
	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	// Read the page into the node and cache it.
	// node n 读取 page 内容，并且记录在bucket的nodes信息中
	n.read(p)
	b.nodes[pgid] = n

	// Update statistics.
	b.tx.stats.NodeCount++

	return n
}

// 释放bucket中的page
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p) // 释放page
		} else {
			n.free() // 释放node：其实底层是将node 对应的page释放
		}
	})
	b.root = 0
}

// 解引用
func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference() // 根节点 解引用
	}

	for _, child := range b.buckets { // 子bucket，递归解引用
		child.dereference()
	}
}

// 根据pageID 查找对应page和node，node存在则先返回node，不存在则返回page
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// Inline buckets have a fake page embedded in their value so treat them
	// differently. We'll return the rootNode (if available) or the fake page.
	// inline buckeet，rootNode存在则返回rootNode，不存在则返回page
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// Check the node cache for non-inline buckets.
	// 从node cache中查找node，存在则返回
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// Finally lookup the page from the transaction if no node is materialized.
	// 没找到node，返回page
	return b.tx.page(id), nil
}

// bucket statistics
type BucketStats struct {
	// page statistics
	BranchPageN     int // 分支节点类型的page 数
	BranchOverflowN int // 分支节点类型page的 溢出数
	LeafPageN       int // 叶子节点类型page的 数
	LeafOverflowN   int // 叶子节点类型page的 数

	// Tree statistics.
	KeyN  int // kv对的数量
	Depth int // B+树深度

	// Page size 利用率.
	BranchAlloc int // 为 分支节点类型page 分配的字节数
	BranchInuse int // 分支节点类型page 实际使用的字节数
	LeafAlloc   int // 为叶子节点类型page 分配的字节数
	LeafInuse   int // 叶子节点类型page 实际使用的字节数

	// Bucket statistics
	BucketN           int // 总bucket数量：包括根bucket在内
	InlineBucketN     int // 内联bucket数量
	InlineBucketInuse int // 内联bucket 使用的字节数
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// 返回给定切片的副本
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
