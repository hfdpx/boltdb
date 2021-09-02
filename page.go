package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

//------------------------------------------------------ 常量 -----------------------------------------------------------//

// page头部大小
const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

// 每个page中最少的key数量
const minKeysPerPage = 2

// 分支页元素头部 大小
const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))

// 叶子页元素头部 大小
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

const (
	bucketLeafFlag = 0x01
)

// page类型
const (
	branchPageFlag   = 0x01 // 分支节点page: 数据page，存放B+树中的中间节点
	leafPageFlag     = 0x02 // 叶子节点page: 数据page，存放B+树中的叶子节点
	metaPageFlag     = 0x04 // 元信息page:   全局仅有两个元信息page，保存在文件，是实现事务的关键
	freelistPageFlag = 0x10 // 空闲列表page: 存放空闲页ID列表，在文件中表现为一段一段的连续的page
)

//------------------------------------------------------ page -----------------------------------------------------------//

type pgid uint64
type page struct {
	id       pgid   // page id
	flags    uint16 // page 类型
	count    uint16 // 对应节点包含的元素个数，比如说包含的KV对
	overflow uint32 // 对应节点元素溢出到其他page的page数量，即使用 overflow+1 个page来保存对应节点的信息
	//以上四个字段构成了 page 定长header
	ptr uintptr //数据指针，指向数据对应的byte数组，当 overflow>0 时会跨越多个连续的物理page，不过多个物理page在内存中也只会用一个page结构体来表示
}

// PageInfo 人类可读的page结构
type PageInfo struct {
	ID            int    // ID
	Type          string // 类型: branch/leaf/meta/freelist/unkonw
	Count         int    // 元素数量
	OverflowCount int    // 溢出page数量
}

//为了避免载入内存和写入文件系统时的序列化和反序列化操作，使用了大量的unsafe包中的指针操作

// 返回一个表示page类型的字符串
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// 返回一个指向page数据部分的指针
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// 通过index获取叶子节点
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// 获取叶子节点列表
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// 通过index获取中间节点
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// 获取中间节点列表
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// 将页面的 n 个字节作为 16进制 输出写入到stderr
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	_, _ = fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge 合并a和b，并排序
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}

//------------------------------------------------------ page 元素 -----------------------------------------------------------//

//branchPageElement 分支页元素 头部
type branchPageElement struct {
	// 只存储key，不存储value本身

	pos   uint32 // key偏移量
	ksize uint32 // key字节数
	pgid  pgid   // 子节点的pageID(相当于value，只是没有存储value本身)
}

// key 返回key
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// 叶子页元素 header
type leafPageElement struct {
	// 对应：某个bucket的B+树中的叶子节点，存储是某个bucket中的一条用户数据
	// 对应：顶层bucket树中的叶子节点，存储的是该db中的某个subbucket

	// &leafPageElement + pos == &key
	// &leafPageElement + pos + ksize == &value

	flags uint32 // 标志位：指明是普通kv还是subbucket
	pos   uint32 // 偏移位：kv header与对应的 kv 的距离
	ksize uint32 // 统计位：key字节数
	vsize uint32 // 统计位：value字节数
}

// key 返回key
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value 返回value
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}
