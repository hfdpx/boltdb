package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// 空闲列表页
// db文件中一组连续的页，用于保存中db使用过程中由于修改操作而释放的页的id列表
type freelist struct {
	ids     []pgid          // 可以分配的空闲页列表 ids
	pending map[txid][]pgid // 按照事务id分别记录了中对应事务期间新增的空闲页列表
	cache   map[pgid]bool   // 快速查找所有空闲和待处理页面 ID。
}

// pending需要单独记录的原因：
// 1.写事务回滚时，对应事务待释放的空闲页列表要从pending项中删除
// 2.某个写事务(txid=7)已经提交，但可能仍然有一些读事务(如txid<7)仍然中使用其刚释放的页，因此不能立即用于分配

// 返回 一个空的初始化好的 freelist
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// 返回 freelist 序列化后大小
func (f *freelist) size() int {
	n := f.count()
	// 如果溢出了，那么按照规定，真正的存储计数存储中数据部分的第一个元素，所以n需要+1
	if n >= 0xFFFF {
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n) // 头部+元素大小*元素数量
}

// 返回 freelist 的page数量
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// 返回 freelist 中free page 数量
func (f *freelist) free_count() int {
	return len(f.ids)
}

// 返回 freelist 中 pending page 数量
func (f *freelist) pending_count() int {
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}

// 合并 ids 和 pending 并排序
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}

// 空闲列表分配
// 如果可以找到连续的n个空闲页，则返回起始页id，否则返回0
// 分配规则：分配单位是页，分配策略是首次适应：即从排序好的空闲列表ids中，找到第一段等于指定长度的连续空闲页，然后返回起始页id
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	// 遍历寻找连续空闲页，并且判断是否等于n
	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// 非连续，重置initial
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// 找到了n个连续的page
		if (id-initial)+1 == pgid(n) {
			// 当正好分配到ids中的前n个page时，仅仅简单的往前调整 f.ids 切片即可
			// 虽然这样会造成空间浪费，但是中对f.ids 进行appned/free时，会重新进行空间分配，而这些被浪费的空间会被GC
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				// 把n个连续page后面的page都往前挪n个位置
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// 从f.cache中删除这n个page的id
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			//返回起始 page id
			return initial
		}

		previd = id
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// Free page and all its overflow pages.
	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// Verify that page is not already free.
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		// Add to the freelist and cache.
		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending[txid] = ids
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	// Remove page ids from cache.
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	// Remove pages from pending list.
	delete(f.pending, txid)
}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// ：空闲列表从page中加载
// 将空闲列表页面初始化为freelist
// 在数据库重启时，会从仅有的两个元信息页中恢复一个合法的元信息，然后根据元信息中的freelist字段，找到存储freelist页的起始位置，然后恢复到内存中
func (f *freelist) read(p *page) {

	// 如果p.count溢出了，那么其真正的值存储在数据部分的第一个元素中
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	//将空闲列表从page拷贝到内存中的freelist结构体中
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// 保证ids有序
		sort.Sort(pgids(f.ids))
	}

	// 重新构建freelist.cache这个map
	f.reindex()
}

// ：空闲列表转化为page
// freelist通过wirte函数，在事务提交时，将自己写入给定的页，进行持久化，在写入，将pending和ids合并后写入
// todo 待理解！
// 合并写入的原因如下：
// 1.write用于写事务提交时调用，写事务是串行的，因此pending中对应的写事务都已提交，所以可以合并
// 2.写入文件是为了应对崩溃后重启，而重启时没有任何读操作，自然不用担心还有读事务还在使用刚释放的页
// 注意：本步骤只是将freelist转化为内存中的页结构，需要额外的操作才能将对应的页持久化到文件(比如tx.write)
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// 设置 页 类型
	p.flags |= freelistPageFlag

	//p.count 是uint16类型，范围是[0,64k-1]，0xFFFF=64k
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		// 通过capyall将 pending和ids 合并并且排序
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		//如果空闲页id超出了64k，则将其放在数据部分的第一个位置上
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {
	f.read(p)

	// Build a cache of only pending pages.
	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	// Once the available list is rebuilt then rebuild the free cache so that
	// it includes the available and pending free pages.
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
