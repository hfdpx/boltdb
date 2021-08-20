package bolt

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

//------------------------------------------------------ 常量值 -----------------------------------------------------------//
// 最大 mmp 步长
const maxMmapStep = 1 << 30 // 1GB

// 数据文件的格式版本
const version = 2

// 一个文件标记值，表示该文件是db file
const magic uint32 = 0xED0CDAED

// IgnoreNoSync 指定将更改同步到文件时是否忽略数据库的NoSync字段。这是必需的，因为某些操作系统（如OpenBSD）没有统一缓冲区缓存（UBC），必须使用msync（2）系统调用同步写入。
const IgnoreNoSync = runtime.GOOS == "openbsd"

// DB 默认值
const (
	DefaultMaxBatchSize  int = 1000                  // 默认批处理最大size
	DefaultMaxBatchDelay     = 10 * time.Millisecond // 默认批处理最大延时
	DefaultAllocSize         = 16 * 1024 * 1024
)

// DB 默认page大小（操作系统page size）
var defaultPageSize = os.Getpagesize()

// Options 表示打开数据库时可以设置的选项
type Options struct {
	//Timeout 等待获取文件锁的时间量，等于0则无限期等待
	Timeout time.Duration

	//NoGrowSync 非增长同步模式，是否启用
	NoGrowSync bool

	//ReadOnly 只读模式，是否启用
	ReadOnly bool

	// 在内存映射文件之前设置DB.MmapFlags标志。
	MmapFlags int

	//InitialMmapSize 数据库初始mmap大小
	InitialMmapSize int
}

//DefaultOptions db默认配置
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

type Info struct {
	Data     uintptr
	PageSize int
}

//------------------------------------------------------ DB -----------------------------------------------------------//

//DB 保存到磁盘上文件的存储桶的集合
type DB struct {
	// 严格模式，启用后会在每次commit之后check，严重影响性能，应仅用于调试模式
	StrictMode bool

	// 非同步模式，启用后会在每次commit时跳过fsync函数的调用，这在批量加载数据时非常有用
	NoSync bool

	// 非增长同步模式，启用后会在数据库增长时跳过对 truncate 的调用
	NoGrowSync bool

	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int

	// 批处理的最大大小，<=0则禁用批处理
	MaxBatchSize int

	// 批处理开始前的最大延时，<=0则禁用批处理
	MaxBatchDelay time.Duration

	//AllocSize 数据库需要创建新页面时分配的空间量。这样做是为了在增长数据文件时分摊truncate（）和fsync（）的成本。
	AllocSize int

	path     string            // db file 存储路径
	file     *os.File          // 指向db file，文件描述符fd
	lockfile *os.File          // windows only
	dataref  []byte            // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte // db data
	datasz   int               // db data size
	filesz   int               // current on disk file size

	meta0    *meta // 元数据 0
	meta1    *meta // 元数据 1
	pageSize int   // page size
	opened   bool  // db 是否是 打开状态

	rwtx     *Tx       // 当前读写事务，唯一！
	txs      []*Tx     // 所有已打开的只读事务
	freelist *freelist // 空闲page 列表
	stats    Stats     // db 统计信息
	pagePool sync.Pool // page 缓冲池

	batchMu  sync.Mutex   // 批处理 锁
	batch    *batch       // 批处理
	rwlock   sync.Mutex   // 读写锁
	metalock sync.Mutex   // meta page 锁
	mmaplock sync.RWMutex // mmap 锁
	statlock sync.RWMutex // stats 锁

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// 只读模式
	readOnly bool
}

// Path 返回当前打开的数据库文件路径
func (db *DB) Path() string {
	return db.path
}

// GoString 返回数据库的字符串表示
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String 返回数据库的字符串表示
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open 通过给定的路径，创建和打开一个数据库【如果文件不存在则自动创建，传入nil参数将使用默认值】
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	// 数据库标记为已打开
	var db = &DB{opened: true}

	// 初始化一些配置
	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize
	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// 打开数据文件，文件不存在则直接创建
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// 锁定数据库文件，如果数据库是仅读模式，则是共享锁，否则是独占锁，保证其他boltdb进程无法获取
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// 用于测试钩子函数
	db.ops.writeAt = db.file.WriteAt

	// 如果 db 不存在 ，初始化db
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// 读取第一个mata page，用于确定page size
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			// 如果无法读取第一个mata page，则用当前os 系统 page size
			if err := m.validate(); err != nil {
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// 初始化 page pool
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// mmap db file
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// 读入 free list
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	return db, nil
}

//mmap 打开 底层内存映射文件 并 初始化元page
func (db *DB) mmap(minsz int) error {
	// 锁住，确保独占
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	// 获取db file 基础信息(FileInfo 结构)
	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// 保证 size 下限
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}

	// 得到真正的mmap size
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// 在 unmaping之前 把所有的mmap内容 解引用，拷贝到内存里
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// unmap , 取消原有的db file 映射，如果有的话
	if err := db.munmap(); err != nil {
		return err
	}

	// 将磁盘上的 db file 映射到内存中，这样在内存中修改了db file 都会同步到磁盘，减少了内存的拷贝次数（文件的read和write会产生频繁的内存拷贝）
	if err := mmap(db, size); err != nil {
		return err
	}

	// 保存对 meta page 的引用：读取db file 的第0页和第1页来初始化db.mata0和db.mate1
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// 验证mate page 信息是否正确
	//(如果两个元页面都未通过验证，我们只会返回一个错误，因为 meta0 验证失败意味着它没有正确保存——但我们可以使用 meta1 进行恢复。反之亦然。)
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap 取消映射 db file
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

//mmapSize 根据传入的mmap size大小返回一个合适的mmap size 大小。最小大小为 32KB，并加倍直至达到 1GB。如果新的 mmap 大小大于允许的最大值，则返回错误。
func (db *DB) mmapSize(size int) (int, error) {
	// 当 32KB <= size <= 1GB 时，直接翻倍返回
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// 不允许超过最大值
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// size >= 1GB，每次增加到1GB的倍数，比如1.2GB 就增长到 2GB
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// 确保 mmap 大小是页面大小的倍数。这应该总是正确的，因为我们以 MB 为单位递增。
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// 保证不超过最大size 上线
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

//init 数据库初始化
func (db *DB) init() error {
	// 前面四个page：｜ meta ｜ meta ｜ freelist ｜ leafpage ｜
	// 此时只包含一个空的叶子节点，该叶子节点即为root bucket的root node

	// 将页面大小设置为操作系统的页面大小
	db.pageSize = os.Getpagesize()

	// 申请四个 page 空间
	buf := make([]byte, db.pageSize*4)

	// 在pageID 0和1 上创建两个元信息页
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// 初始化元信息页
		m := p.meta()
		m.magic = magic     // 魔数
		m.version = version //版本号
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2           // 指向freelist的pageID 2
		m.root = bucket{root: 3} //指向bucket的pageID 3
		m.pgid = 4               //指向正式数据的pageID 4
		m.txid = txid(i)         // 事务序列号
		m.checksum = m.sum64()   // check sum
	}

	// 在pageID 2 写入一个空的 freelist页
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// 在pageID 4 写入一个空的 叶子节点页
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// 将buf中的数据写入文件，同时将磁盘缓冲页立即写入磁盘
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

//Close 关闭db，释放所有db资源：要求所有tx都已被关闭
func (db *DB) Close() error {

	// 锁住所有需要竞争的资源
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	// db 标记为 关闭
	if !db.opened {
		return nil
	}
	db.opened = false

	db.freelist = nil

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	if db.file != nil {
		// 只读文件无需 unlock
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// 关闭 fd(文件描述符)
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

//Begin 开始一个新的事务。
func (db *DB) Begin(writable bool) (*Tx, error) {
	// 可以同时使用多个只读事务，但一次只能使用一个写事务。
	// 启动多个写事务将导致调用阻塞并序列化，直到当前写事务完成。
	// 事务不应相互依赖。在同一个goroutine中打开读事务和写事务可能会导致写入程序死锁，因为数据库在增长时需要周期性地重新映射自身，而在读事务打开时不能这样做。
	// 如果需要长时间运行的读取事务（例如，快照事务），则可能需要将DB.InitialMmapSize设置为足够大的值，以避免潜在的写入事务阻塞。
	// 重要提示：完成后必须关闭只读事务，否则数据库将无法回收旧页。

	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

//beginTx 开始一个 只读事务
func (db *DB) beginTx() (*Tx, error) {
	// 锁住 mata page
	db.metalock.Lock()

	// 在mmap上获取只读锁。
	// 当mmap被重新映射时，它将获得一个写锁.
	db.mmaplock.RLock()

	// mmap被重新映射的两种情况：
	// 1.db file文件在第一次创建或打开时
	// 2.在写入db后且db file需要增大时，需要重新mmap将新的文件映射入进程的地址空间
	// 具体情况分析：当db在不同线程中进行读写时，可能存在其中一个线程的读写事务写入了大量的数据，在commit时，由于已映射区的空闲page不够，
	//    会重新进行mmap，此时若有未关闭的只读事务，由于此事务占用着mmap的读锁，那么mmap操作会阻塞在争用mmap写锁的地方
	//    也就是说，如果存在着耗时的只读事务，同时写事务需要remmap时，写操作会被读操作阻塞
	// 所以：在使用boltdb时，应尽量避免耗时的读操作，同时在写操作时，应避免频繁的remmap

	// db 不是打开状态，return并释放锁
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建一个与db关联的只读事务
	t := &Tx{}
	t.init(db)
	// 将创建好的事务 加入到 db的仅读事务列表 中
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// 仅释放meta page 锁!
	db.metalock.Unlock()

	// 更新db 统计数据
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

//beginRWTx 开始一个 读写事务
func (db *DB) beginRWTx() (*Tx, error) {
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// 读写锁，锁住
	db.rwlock.Lock()

	// 在获得写锁之后，锁住meta page，来修改meta
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建一个与db关联的读写事务
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// boltdb同时只能有一个读写事务，但是可以同时有多个只读事务！

	// 在所有打开的只读事务中，找到最小的txid，释放该事务用到的page
	// todo ???????????? 为什么要释放这个只读事务？
	var minid txid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTx 移除事务
func (db *DB) removeTx(tx *Tx) {
	// 释放 mmap 上 的读锁
	db.mmaplock.RUnlock()

	// 锁住 meta page
	db.metalock.Lock()

	// 移除此事务
	for i, t := range db.txs {
		if t == tx {
			// 将最后一个事务放到此事务的位置上，然后txs缩短一个长度，就算是删除了此事务
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Merge statistics.
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

//Update 在读写管理事务的上下文中执行函数。如果函数未返回错误，则提交事务。如果返回错误，则回滚整个事务。函数返回或提交返回的任何错误都将从Update（）方法返回。尝试在函数内手动提交或回滚将导致死机。
func (db *DB) Update(fn func(*Tx) error) error {
	// 开启一个读写事务
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// 确保事务在紧急状态下回滚
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为托管发送，以便内部函数无法手动提交。
	t.managed = true

	// 如果函数返回错误，则回滚并返回错误。
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	// 提交此事务
	return t.Commit()
}

//View 在只读事务的上下文中执行函数，函数返回的任何错误都将从View（）方法返回。尝试在函数内手动回滚将导致死机
func (db *DB) View(fn func(*Tx) error) error {
	// 开启一个只读事务
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// 确保事务在紧急情况下回滚
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为托管发送，以便内部功能无法手动回滚。
	t.managed = true

	// 执行 fn，错误则回滚
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	// 不需要调用Commit(),而是通过调用Rollback()来结束Transaction
	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

//Batch 批处理
func (db *DB) Batch(fn func(*Tx) error) error {
	// 批处理作为批处理的一部分调用fn。它的行为类似于更新，
	// 只是：1.并发批处理调用可以组合到单个Bolt事务中。
	//      2.传递给Batch的函数可能会被多次调用，无论它是否返回错误。这意味着批处理函数的副作用必须是幂等的，
	//        并且只有在调用者中看到成功的返回后才会永久生效。最大批量大小和延迟可分别使用DB.MaxBatchSize和DB.MaxBatchDelay进行调整。批处理只有在有多个goroutine调用它时才有用。

	errCh := make(chan error, 1)

	// 获取 批处理锁
	db.batchMu.Lock()

	// 没有批处理任务 或者 批处理中任务数量已满，开启一个新的批处理
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})

	// 只有当批处理中任务数量达到一定要求，才执行批处理
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}

	// 释放批处理锁
	db.batchMu.Unlock()

	// 批处理产生错误，应该单独重新运行
	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

//Sync 针对数据库文件句柄执行fdatasync
//(在正常操作下，这是不必要的，但是，如果您使用NoSync，那么它允许您强制数据库文件与磁盘同步)
func (db *DB) Sync() error { return fdatasync(db) }

//Stats 检索数据库的持续性能统计信息。仅在事务关闭时更新。
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

//Info 返回db的Info信息
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

//page 返回db中的特定page
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

//pageInBuffer 从给定的[]byte 查找 指定page
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta 返回db mate page
func (db *DB) meta() *meta {

	// 返回具有最高txid的meta，这不会导致验证失败。否则，当数据库实际上处于一致状态时，我们可能会导致错误。
	// metaA是txid较高的一种。
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// 这永远不应该达到，因为meta1和meta0都在mmap（）上进行了验证，并且我们在每次写入时都执行fsync（）。
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate 分配给定数量的page
func (db *DB) allocate(count int) (*page, error) {
	// 临时缓冲区
	var buf []byte

	// 如果需要的page数量只有一个，则从pagePool缓冲池中分配，以减少分配内存带来的时间开销
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	// buf转化为page，提供指针
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// 从freelist查看有没有可用的页号，如果有则分配给刚刚申请到的页缓存,并返回
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// 将新申请的第一个page的ID 设置为文件内容结尾处的page ID
	p.id = db.rwtx.meta.pgid

	// 如果需要的page数量大于已经映射到内存的文件总page数，则触发remmap
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}
	// 分析：如果打开db文件时，设定的初始文件映射长度足够长，
	//      可以减少读写事务需要remmap的概率，从而降低读写事务被阻塞的概率(因为读写事务在remmap时，需要等待所有已经打开的只读事务的结束)，提高读写并发

	// 更新db已使用的最大page ID
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow db大小增长到给定sz
func (db *DB) grow(sz int) error {
	// 给定sz比目前size小，忽略
	if sz <= db.filesz {
		return nil
	}

	// 如果数据小于alloc大小，则只分配所需的数据。一旦超过分配大小，则分块分配。
	// todo ????????????
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284

	// 同步增长模式 && db可写模式
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			// 更改文件大小
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		// 安全起见，需要在更改文件大小后进行fsync
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

//IsReadOnly db是否只读模式
func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

//------------------------------------------------------ 批处理 -----------------------------------------------------------//

//call 函数封装
type call struct {
	fn  func(*Tx) error
	err chan<- error
}

//batch 批处理封装
type batch struct {
	db    *DB
	timer *time.Timer // 定时器
	start sync.Once   // 只执行一次操作的对象
	calls []call
}

// trigger 触发批处理
func (b *batch) trigger() {
	b.start.Do(b.run)
}

//run 执行批处理中的事务，并将结果传回DB.batch
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// 确保没有新工作添加到此批次，但不要中断其他批次。
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// 从批处理中取出失败的事务。在这里缩短b.calls是安全的，因为db.batch不再指向我们，而且我们仍然保持互斥。
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// 告诉提交者重新单独运行，继续批处理的其余部分
			c.err <- trySolo
			continue retry
		}

		// 将成功或内部错误传递给所有调用者
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

//trySolo 是一个特殊的sentinel错误值，用于发出重新运行事务功能的信号
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

//------------------------------------------------------ Stats -----------------------------------------------------------//

// Stats db 统计信息
type Stats struct {
	// Freelist 统计信息
	FreePageN     int // freelist上 free page 数量
	PendingPageN  int // freelist上 pending page 数量
	FreeAlloc     int // free pages 中 分配的总字节数
	FreelistInuse int // freelsit 使用的总字节数

	// Transaction 统计信息
	TxN     int // 仅读事务 总数
	OpenTxN int // 当前打开的仅读事务 数量

	TxStats TxStats // 事务操作状态 统计
}

// Sub 计算并返回两组数据库统计数据之间的差异
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

//------------------------------------------------------ meta -----------------------------------------------------------//

// 元信息page加载到内存后的数据结构
type meta struct {
	magic    uint32 // 魔数
	version  uint32 // 数据文件版本号
	pageSize uint32 // 该db的page大小，通过syscall.Getpagesize()获取，通常是4k

	flags uint32

	root     bucket // 各个bucket的根组成的树
	freelist pgid   // 空闲列表存储的起始页ID

	pgid pgid // 当前用到的最大page ID，也即用到的page数量
	txid txid // 事务序列号

	checksum uint64 // 校验和，用于校验元信息页是否完整
}

// validate 检查 mate page 信息是否正确
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// write 元数据 写入 page(此page就成为了元page）
func (m *meta) write(p *page) {
	// 保证没有超过 高水位线(当前使用的最大pageID)
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// 通过事务ID来确定Page ID，0或者1
	p.id = pgid(m.txid % 2)
	// 将page 设置为 mate page
	p.flags |= metaPageFlag

	// 计算 校验和
	m.checksum = m.sum64()

	// p的mate 指向 m
	m.copy(p.meta())
}

//sum64 生成校验和
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

//_assert 如果给定的条件为false，则断言将在给定的格式化消息中死机。
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
