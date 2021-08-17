package bolt

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// 事务 ID
type txid uint64

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with
// them. Pages can not be reclaimed by the writer until no more transactions
// are using them. A long running read transaction can cause the database to
// quickly grow.
type Tx struct {
	writable       bool           // 标志位：是否是可写事务
	managed        bool           // 标志位：当前事务是否被db托管，即通过db.Update()或db.View来写或读数据库
	db             *DB            // 指向当前db
	meta           *meta          // 指向元信息
	root           Bucket         // 根bucket
	pages          map[pgid]*page // 当前事务用于读或写读page 对应表
	stats          TxStats        // 操作状态统计
	commitHandlers []func()       // commit时的回调函数
	WriteFlag      int            //复制或者打开数据库文件时，指定文件打开模式
}

//init 事务对象 初始化
func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	// Copy the meta page since it can be changed by the writer.
	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	// Copy over the root bucket.
	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	// Increment the transaction id and add a page cache for writable transactions.
	// 如果事务是读写事务，事务的ID为什么要+1？？？
	if tx.writable {
		tx.pages = make(map[pgid]*page)
		tx.meta.txid += txid(1)
	}
}

// ID returns the transaction id.
func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

// DB returns a reference to the database that created the transaction.
func (tx *Tx) DB() *DB {
	return tx.db
}

// Size 返回当前db size
func (tx *Tx) Size() int64 {
	// 当前事务使用到的最大page ID * page 大小
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

// Writable 判断事务是否支持写操作
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Cursor 返回一个游标
func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

// Stats 返回一个 事务 status 副本
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

// Bucket 返回一个特定名称的bucket，该bucket仅仅在事务生存期间有效
func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

// CreateBucket 创建一个特定名称的bucket，该bucket仅仅在事务生存期间有效
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

// CreateBucketIfNotExists 创建一个特定名称的bucket，条件是如果该bucket不存在，该bucket仅仅在事务生存期间有效
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.root.CreateBucketIfNotExists(name)
}

// DeleteBucket 删除特定名称 bucket
func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.root.DeleteBucket(name)
}

// ForEach 对每个bucket执行指定函数，一个出错，则停止执行剩下的
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.root.ForEach(func(k, v []byte) error {
		if err := fn(k, tx.root.Bucket(k)); err != nil {
			return err
		}
		return nil
	})
}

// OnCommit 添加一个事务commit后的回调函数
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit 提交事务：将所有更改写入磁盘并更新元页。如果发生磁盘写入错误，或者在只读事务上调用commit，则返回错误
func (tx *Tx) Commit() error {
	// 保证没有被db托管
	_assert(!tx.managed, "managed tx commit not allowed")
	// 异常情况禁止commit
	if tx.db == nil { //db 空
		return ErrTxClosed
	} else if !tx.writable { // 事务不可写
		return ErrTxNotWritable
	}

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// 对整个db的根bucket进行重新平衡，涉及节点：当前事务读写访问过的bucket中的有删除操作的节点
	var startTime = time.Now()
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}

	// 对整个db的根bucket进行溢出操作，涉及节点：key数量超过限制的节点，进行溢出操作（分裂节点）
	startTime = time.Now()
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	// 更新根bucket的根节点的page id，因为旋转和分裂后根bucket的根节点可能发生了变化
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid

	// 对于freelist占用的page：释放，重新分配，写入(freelist写入新分配的page)
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.rollback()
		return err
	}
	if err := tx.db.freelist.write(p); err != nil {
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// If the high water mark has moved up then attempt to grow the database.
	// 当page数量增加时，同步增长db大小
	// 因为向文件写数据时，文件系统上该文件节点的元数据可能不会立刻刷新，导致文件的size不会立刻更新，当进程crash时，可能会出现写文件结束但是文件大小没有更新的情况，为了防止这种情况，主动增长文件大小
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// 将事务中所有的脏页写入磁盘
	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// 启用了严格模式
	if tx.db.StrictMode {
		// 开始一致性检查
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// 元数据 写入 disk
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)

	// 关闭事务
	tx.close()

	// 调用 一些 回调函数
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

// Read-only transactions must be rolled back and not committed.
// why？
// 回滚
func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}
func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)                    // 从freelist中删除这个txid对应的所有paid
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist)) // 重新刷新freelist
	}
	tx.close()
}

// 事务 关闭
func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// Grab freelist stats.
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()

		// Remove transaction ref & writer lock.
		// 移除事务引用 & 读写锁解锁
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()

		// 记录状态
		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		// 从db中删除此事务
		tx.db.removeTx(tx)
	}

	// Clear all references.
	// 清除事务上的所有引用
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

// 整个db写入writer，用于向后兼容，不推荐使用
func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

// 整个db写入writer
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// 打开 db 文件
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Generate a meta page. We use the same page data for both meta pages.
	// 生成一个meta page
	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta

	// Write meta 0.
	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 0 copy: %s", err)
	}

	// Write meta 1 with a lower transaction id.
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// Move past the meta pages in the file.
	// ？？？为什么要这么做
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// Copy data pages.
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// 整个 db 拷贝到file
func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	err = tx.Copy(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// 一致性检查
func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}
func (tx *Tx) check(ch chan error) {
	// 检查是否有任何page被 双重释放 （即freelist中是否有相同的pageID）
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// Track every reachable page.
	// 追踪每个可达page
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

	// Recursively check buckets.
	// 递归检查bucket
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// 看看有没有 page不可达并且没有被释放
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	//关闭通道，发出完成信号
	close(ch)
}
func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// Ignore inline buckets.
	if b.root == 0 {
		return
	}

	// 检查 bucket 使用的每一个page（非freelist中的page）
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		// 超出了当前最大pageID
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// 确保每个page 只被引用一次
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// bucket 使用的page里 存在已释放的page 或者 存在非叶子，非分支的page
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// Check each bucket within this bucket.
	// 检查 该bucket下的 每个bucket
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

// 返回一个 指定大小的page
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// 保存 对应关系到cache
	tx.pages[p.id] = p

	// Update statistics.
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// 将脏页写入disk
func (tx *Tx) write() error {
	// 复制 cache表
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	// 清空原有cache表，对新cache表按照di排序
	tx.pages = make(map[pgid]*page)
	sort.Sort(pages)

	// 按照顺序将page写入disk
	for _, p := range pages {
		// 先得到page 实际大小
		size := (int(p.overflow) + 1) * tx.db.pageSize
		offset := int64(p.id) * int64(tx.db.pageSize)

		// 将p转化成 最大限制大小的byte数组，ptr指向这个[]byte
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		for {
			// size 不允许超出上限
			sz := size
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// 截取合适大小的块 写入
			buf := ptr[:sz]
			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			// 更新计数
			tx.stats.Write++

			// 这个page全部写完了 则退出，写下一个page
			size -= sz
			if size == 0 {
				break
			}

			// 指向page中的下一块 & prt指向p的剩余部分
			offset += int64(sz)
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}

	// 判断是否进行文件同步
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 将 小page放入 page池
	for _, p := range pages {
		// Ignore page sizes over 1 page.
		// These are allocated using make() instead of the page pool.
		if int(p.overflow) != 0 {
			continue
		}

		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]

		// See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}

	return nil
}

// 将元数据 写入disk
func (tx *Tx) writeMeta() error {
	// 为元数据创建临时缓冲区：申请一块buf，buf转化成page，p指向这块page
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	// 将元数据写入p page
	tx.meta.write(p)

	// 将 p page 写入file
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// Update statistics.
	tx.stats.Write++

	return nil
}

// 返回指定page的引用，不存在则创建一个再返回
func (tx *Tx) page(id pgid) *page {
	// Check the dirty pages first.
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return tx.db.page(id)
}

// 迭代给定page中的每个page并执行函数。
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	// 子对象递归
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// 返回特定的pageInfo：
func (tx *Tx) Page(id int) (*PageInfo, error) {
	// This is only safe for concurrent use when used by a writable transaction.

	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// 构建 pageInfo
	p := tx.db.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// Determine the type (or if it's free).
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats 事务 操作状态统计
type TxStats struct {
	// page 统计
	PageCount int // page 分配数
	PageAlloc int // 分配的总字节数

	// cursor 统计
	CursorCount int // 创建的游标数量

	// Node 统计
	NodeCount int // node 分配数
	NodeDeref int // node 取消引用数

	// Rebalance 统计
	Rebalance     int           // node 重新平衡数
	RebalanceTime time.Duration // 重新平衡 耗费的总时间

	// Split/Spill 统计
	Split     int           // number of nodes split
	Spill     int           // number of nodes spilled
	SpillTime time.Duration // total time spent spilling

	// Write 统计
	Write     int           // number of writes performed
	WriteTime time.Duration // total time spent writing to disk
}

// 事务状态 add操作
func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// 事务状态 sub操作
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
