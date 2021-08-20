package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeysPerPage = 2

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

// 四种类型的page
const (
	branchPageFlag   = 0x01 //中间节点page: 数据page，存放B+树中的中间节点
	leafPageFlag     = 0x02 //叶子节点page: 数据page，存放B+树中的叶子节点
	metaPageFlag     = 0x04 //元信息page: 全局仅有两个元信息page，保存在文件，是实现事务的关键
	freelistPageFlag = 0x10 //空闲列表page: 存放空闲页ID列表，在文件中表现为一段一段的连续的page

)

// page和节点（node）的对应关系：
// 1.page是文件存储的基本单位，node是 B+ tree 的基本构成节点
// 2.一个数据node对应一到多个连续的数据page
// 3.连续的数据page序列化加载到内存中就成为一个数据node
// 总结：在文件系统上线性组织的数据page，通过页内指针，在逻辑上组成来一棵二维的B+tree
//      ,该树的树根保证在元信息page中，而文件中所有其他没有用到的页的ID列表，保存在空闲列表page中

const (
	bucketLeafFlag = 0x01
)

type pgid uint64
type page struct {
	id       pgid   //页id
	flags    uint16 //页类型
	count    uint16 //对应节点包含的元素个数，比如说包含的KV对
	overflow uint32 //对应节点元素溢出到其他page的page数量，即使用 overflow+1 个page来保存对应节点的信息
	//以上四个字段构成了 page 定长header
	ptr uintptr //数据指针，指向数据对应的byte数组，当 overflow>0 时会跨越多个连续的物理page，不过多个物理page在内存中也只会用一个page结构体来表示
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

// 中间节点页header
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// 叶子节点页header
// 对应：某个bucket的B+树中的叶子节点，存储是某个bucket中的一条用户数据
// 对应：顶层bucket树中的叶子节点，存储的是该db中的某个subbucket
type leafPageElement struct {
	flags uint32 //标志位：指明是普通kv还是subbucket
	pos   uint32 //偏移位：kv header与对应的 kv 的距离
	ksize uint32 //统计位：key字节数
	vsize uint32 //统计位：value字节数
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
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

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
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
