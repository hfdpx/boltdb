package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

//freelist 空闲page列表
type freelist struct {
	// 空闲页列表，db文件中一组连续的页，用于保存中db使用过程中由于修改操作而释放的页的id列表
	ids []pgid // 可以分配的空闲页列表id

	pending map[txid][]pgid // 按照事务id 分别记录了中 对应事务期间新增的空闲页列表
	// pending需要单独记录的原因：
	// 1.某个写事务(txid=7)已经提交，但可能仍然有一些读事务(如txid<7)仍然中使用其刚释放的页，因此不能立即用于分配

	cache map[pgid]bool // (pending释放标记，用于验证页面是否空闲)(快速查找所有空闲和待处理页面ID)
}

//newFreelist 返回一个空的初始化好的 freelist
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

//size 返回freelist size
func (f *freelist) size() int {
	n := f.count()
	// 如果溢出了，那么按照规定，真正的存储计数存储中数据部分的第一个元素，所以n需要+1
	if n >= 0xFFFF {
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n) // 头部+元素大小*元素数量
}

//count 返回 freelist 的page数量
func (f *freelist) count() int {
	return f.freeCount() + f.pendingCount()
}

//freeCount 返回 freelist 中free page 数量
func (f *freelist) freeCount() int {
	return len(f.ids)
}

//pendingCount 返回 freelist 中 pending page 数量
func (f *freelist) pendingCount() int {
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}

//copyall 合并 ids 和 pending 并排序
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pendingCount())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}

//allocate 分配n个空闲的page
func (f *freelist) allocate(n int) pgid {
	// 如果可以找到连续的n个空闲页，则返回起始页id，否则返回0
	// 分配规则：分配单位是页，分配策略是首次适应：即从排序好的空闲列表ids中，找到第一段等于指定长度的连续空闲页，然后返回起始页id

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

// free 释放page空间到pending缓存上面
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// 释放从[p.id, p.id+p.overflow]的的page(标记释放而已)
	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// 验证页面是否空闲
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		ids = append(ids, id)
		f.cache[id] = true // 打上释放标记
	}
	f.pending[txid] = ids
}

//release 将事务ID小于txid的事务的page 从f.pending 移动到 f.ids
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// rollback 回滚，将特定事务期间的 pages ID 删除
func (f *freelist) rollback(txid txid) {
	// 从f.pending中移除 事务的一些page
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	// 在f.pending中删除对应事务的page id 列表
	delete(f.pending, txid)
}

// freed 判断给定 page 是否在 空闲列表 中。
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

//read 从page中加载freeList
func (f *freelist) read(p *page) {
	// 在数据库重启时，会从仅有的两个元信息页中恢复一个合法的元信息，然后根据元信息中的freelist字段，找到存储freelist页的起始位置，然后恢复到内存中

	// 如果p.count溢出了，那么其真正的值存储在数据部分的第一个元素中
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	// 构建 f.ids
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// 保证f.ids有序
		sort.Sort(pgids(f.ids))
	}

	// 构建 f.cache
	f.reindex()
}

// reindex 根据f.ids和f.pending 来重建 f.cache
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

//write freeList 写入page
func (f *freelist) write(p *page) error {
	// freelist通过wirte函数，在事务提交时，将自己写入给定的页，进行持久化，在写入时，将pending和ids合并后写入
	// todo 待理解！
	// 合并写入的原因如下：
	// 1.write用于写事务提交时调用，写事务是串行的，因此pending中对应的写事务都已提交，所以可以合并
	// 2.写入文件是为了应对崩溃后重启，而重启时没有任何读操作，自然不用担心还有读事务还在使用刚释放的页
	// 注意：本步骤只是将freelist转化为内存中的页结构，需要额外的操作才能将对应的页持久化到文件(比如tx.write)

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
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:]) //如果空闲页id超出了64k，则将其放在数据部分的第一个位置上
	}

	return nil
}

// reload 从page读取freeList并且过滤掉f.pending
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
