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

//------------------------------------------------------ Tx -----------------------------------------------------------//

type txid uint64

//Tx 事务
type Tx struct {
	//Tx 表示数据库上的只读或读写事务。只读事务可用于检索键的值和创建游标。读写事务可以创建和删除存储桶以及创建和删除键。重要提示：处理完事务后，必须提交或回滚事务。
	//   全部对数据的操作必须发生在一个事务之内，bolt的并发读取都在此实现

	writable bool // 标志位：读写事务 or 仅读事务
	managed  bool // 标志位：当前事务是否被db托管，即通过db.Update()或db.View来写或读数据库

	db    *DB            // 指向当前db
	meta  *meta          // 事务初始化时 从db复制过来的meta 信息
	root  Bucket         // 事务的 根bucket
	pages map[pgid]*page // 事务的读或写page

	stats          TxStats  // 事务操作状态统计
	commitHandlers []func() // commit时的回调函数
	WriteFlag      int      // 复制或者打开数据库文件时，指定文件打开模式
}

//init 事务 初始化
func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	// 创建一个空的mate对象，并初始化为tx.meta，再讲db中的mate复制到刚刚创建的mate对象中(对象拷贝而非指针拷贝)
	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	// 为当前事务创建一个bucket，并将其设为根bucket，同时用mate中保存的根bucket头部来初始化事务的根bucket头部
	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	// 如果是读写事务，就将mate中的txid+1，这样当该读写事务commit之后，mate会更新到数据库文件中，数据库的修改版本号就增加了
	if tx.writable {
		tx.pages = make(map[pgid]*page) // 读写事务需要一个page cache
		tx.meta.txid += txid(1)
	}
}

//ID 返回事务ID
func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

//DB 返回一个对db的引用
func (tx *Tx) DB() *DB {
	return tx.db
}

//Size 返回当前db size
func (tx *Tx) Size() int64 {
	// 当前事务使用到的最大page ID * page 大小
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

//Writable 判断是否是 读写事务
func (tx *Tx) Writable() bool {
	return tx.writable
}

//Cursor 返回一个新建的游标
func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

//Stats 返回 事务的操作状态统计 副本
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

//Bucket 返回一个特定名称的bucket，该bucket仅仅在事务生存期间有效
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
	// 具体：
	// 1. 从根bucket开始，对访问过的bucket进行合并和分裂，让进行过插入和删除操作的B+树重新达到平衡状态
	// 2. 更新freeList页
	// 3. 将由当前事务分配的page 写入 disk，需要分配page的地方有：a.节点分裂产生新节点，b.freeList页重新分配
	// 4. 将meta 写入 disk

	// 处理异常情况
	_assert(!tx.managed, "managed tx commit not allowed") // 保证没有被db托管
	if tx.db == nil {
		// db 空
		return ErrTxClosed
	} else if !tx.writable {
		// 事务不可写
		return ErrTxNotWritable
	}

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// 1.1 节点合并：对整个db的根bucket进行重新平衡，涉及节点：当前事务读写访问过的bucket中的有删除操作的节点
	var startTime = time.Now()
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}
	// 1.2 节点分裂：对整个db的根bucket进行溢出操作，涉及节点：key数量超过限制的节点，进行溢出操作（分裂节点）
	startTime = time.Now()
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	tx.meta.root.root = tx.root.root // 更新根bucket的根节点的page id，因为旋转和分裂后根bucket的根节点可能发生了变化
	opgid := tx.meta.pgid

	// 2.1 对于freelist占用的page：释放，重新分配，写入(freelist写入新分配的page)
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

	// 当page数量增加时，同步增长db大小
	if tx.meta.pgid > opgid {
		// 因为向文件写数据时，文件系统上该文件节点的元数据可能不会立刻刷新，导致文件的size不会立刻更新，当进程crash时，可能会出现写文件结束但是文件大小没有更新的情况，为了防止这种情况，主动增长文件大小
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// 3.1 将事务中所有的脏页写入磁盘
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

	// 4.元数据 写入 disk，因为进行读写操作后，mate中的txid已经改变，root，freeList和paid也有可能已经更新了
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime) // 记录整个事务耗时

	// 关闭事务，清空相关字段
	tx.close()
	// 执行事务提交后的回调函数
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

//Rollback 回滚,只读事务不必提交，回滚即可
func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

//rollback
func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	// 如果是读写事务，则从freelist中删除这个txid对应的所有paid，并且重新刷新freelist
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
	}
	// 最后，将事务关闭
	tx.close()
}

//close 关闭事务
func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	// 如果当前事务是读写事务
	if tx.writable {
		//  freelist 状态统计
		var freelistFreeN = tx.db.freelist.freeCount()
		var freelistPendingN = tx.db.freelist.pendingCount()
		var freelistAlloc = tx.db.freelist.size()

		// 移除db当前读写事务引用 & 读写锁解锁
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
		// 如果当前事务是仅读事务，
		tx.db.removeTx(tx) // 在db上移除此事务
	}

	// 清除事务上的所有引用
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

// Copy 整个db写入writer，用于向后兼容，不推荐使用
func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo 整个db写入writer
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// 打开 db 文件
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

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

	// Write meta 1
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// 偏移 两个page 位置
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// 复制数据页
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// CopyFile 整个 db 拷贝到file
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

// Check 一致性检查
func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

//check
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

	// 追踪每个可达page
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

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

//checkBucket 检查bucket
func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// 忽略 inline bucket.
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

	// 检查 该bucket下的 每个bucket
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

//allocate 分配 指定数量的 page
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// 将该page 归属于该
	tx.pages[p.id] = p

	// 更新 统计记录
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

//write 将脏页写入disk
func (tx *Tx) write() error {
	// 将tx中的脏页引用复制一份，并且释放原来的引用，tx对象并不是线程安全的，而接下来的写文件操作会比较耗时，此时应该避免tx.pages被修改
	// 复制 事务的 page cache表
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

//writeMeta 将元数据 写入disk
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

//page 返回指定page的引用，不存在则创建一个再返回
func (tx *Tx) page(id pgid) *page {
	// 先在事务使用的 pages中查找，实在找不到再从db映射的page中查找
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}
	return tx.db.page(id)
}

//forEachPage 迭代给定page中的每个page并执行函数。
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	// 查找 page
	p := tx.page(pgid)

	// 执行函数
	fn(p, depth)

	// 子对象递归
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

//Page 返回特定的pageInfo(人为可读的page结构)
func (tx *Tx) Page(id int) (*PageInfo, error) {
	// This is only safe for concurrent use when used by a writable transaction.

	// 处理异常
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

	// 确定 pageInfo type
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

//------------------------------------------------------ TxStats -----------------------------------------------------------//

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
