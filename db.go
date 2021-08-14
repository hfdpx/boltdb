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

// 最大 mmp 步长
const maxMmapStep = 1 << 30 // 1GB

// 数据文件的格式版本
const version = 2

// 一个文件标记值，表示该文件是db file
const magic uint32 = 0xED0CDAED

// 指定将更改同步到文件时是否忽略数据库的NoSync字段。这是必需的，因为某些操作系统（如OpenBSD）没有统一缓冲区缓存（UBC），必须使用msync（2）系统调用同步写入。
const IgnoreNoSync = runtime.GOOS == "openbsd"

// DB 默认值
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 16 * 1024 * 1024
)

// DB 默认page大小（操作系统page size）
var defaultPageSize = os.Getpagesize()

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
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

	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	AllocSize int

	path     string
	file     *os.File
	lockfile *os.File // windows only
	dataref  []byte   // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int
	filesz   int // current on disk file size
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool
	rwtx     *Tx
	txs      []*Tx
	freelist *freelist
	stats    Stats

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

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
	var db = &DB{opened: true}

	// Set default options if no options are provided.
	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// Set default values for later DB operations.
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// Lock file so that other processes using Bolt in read-write mode cannot
	// use the database  at the same time. This would cause corruption since
	// the two processes would write meta pages and free pages separately.
	// The database file is locked exclusively (only one process can grab the lock)
	// if !options.ReadOnly.
	// The database file is locked using the shared lock (more than one process may
	// hold a lock at the same time) otherwise (options.ReadOnly is set).
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// Default values for test hooks
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// Read the first meta page to determine the page size.
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// Initialize page pool.
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// Memory map the data file.
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// Read in the freelist.
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	// Mark the database as opened and return.
	return db, nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Dereference all mmap references before unmapping.
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// 数据库初始化
// 前面四个page：｜ meta ｜ meta ｜ freelist ｜ leafpage ｜
// 此时只包含一个空的叶子节点，该叶子节点即为root bucket的root node
func (db *DB) init() error {
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

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
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
		// No need to unlock read-only file.
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// Close the file descriptor.
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin starts a new transaction.
// Multiple read-only transactions can be used concurrently but only one
// write transaction can be used at a time. Starting multiple write transactions
// will cause the calls to block and be serialized until the current write
// transaction finishes.
//
// Transactions should not be dependent on one another. Opening a read
// transaction and a write transaction in the same goroutine can cause the
// writer to deadlock because the database periodically needs to re-mmap itself
// as it grows and it cannot do that while a read transaction is open.
//
// If a long running read transaction (for example, a snapshot transaction) is
// needed, you might want to set DB.InitialMmapSize to a large enough value
// to avoid potential blocking of write transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished or
// else the database will not reclaim old pages.
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) beginTx() (*Tx, error) {
	// Lock the meta pages while we initialize the transaction. We obtain
	// the meta lock before the mmap lock because that's the order that the
	// write transaction will obtain them.
	db.metalock.Lock()

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	db.mmaplock.RLock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{}
	t.init(db)

	// Keep track of transaction until it closes.
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Update the transaction stats.
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// Free any pages associated with closed read-only transactions.
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

// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Use the meta lock to restrict access to the DB object.
	db.metalock.Lock()

	// Remove the transaction.
	for i, t := range db.txs {
		if t == tx {
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

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually commit.
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
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
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
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

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (db *DB) Sync() error { return fdatasync(db) }

// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// This is for internal access to the raw data bytes from the C cursor, use
// carefully, or not at all.
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
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

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int
}

// DefaultOptions represent the options used if nil options are passed into Open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

// Stats represents statistics about the database.
type Stats struct {
	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions

	TxStats TxStats // global, ongoing stats.
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
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

type Info struct {
	Data     uintptr
	PageSize int
}

// 元信息page加载到内存后的数据结构
type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32 // 该db的page大小，通过syscall.Getpagesize()获取，通常是4k
	flags    uint32
	root     bucket // 各个bucket的根组成的树
	freelist pgid   // 空闲列表存储的起始页ID
	pgid     pgid   // 当前用到的最大page ID，也即用到的page数量
	txid     txid   // 事务版本号
	checksum uint64 // 校验和，用于校验元信息页是否完整
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
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

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// Calculate the checksum.
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert will panic with a given formatted message if the given condition is false.
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
