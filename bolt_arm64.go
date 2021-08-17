// +build arm64

package bolt

// maxMapSize 表示 Bol 支持的最大 mmap 大小
const maxMapSize = 0xFFFFFFFFFFFF // 256TB

// maxAllocSize 创建数组指针时允许使用的最大大小
const maxAllocSize = 0x7FFFFFFF

// Are unaligned load/stores broken on this arch?
var brokenUnaligned = false
