package bolt

import "errors"

// 在数据库上Open()或调用方法时，可能发生的错误
var (
	// ErrDatabaseNotOpen db没有打开
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrDatabaseOpen db已打开
	ErrDatabaseOpen = errors.New("database already open")

	// ErrInvalid 无效db：db文件上的两个mate信息都无效
	ErrInvalid = errors.New("invalid database")

	// ErrVersionMismatch db和dbfile 版本不匹配
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrChecksum 校验和不匹配
	ErrChecksum = errors.New("checksum error")

	// ErrTimeout 获取db独占锁超时
	ErrTimeout = errors.New("timeout")
)

// 开始或提交事务时，可能发生的错误
var (
	// ErrTxNotWritable 该事务非读写事务
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed 事务已关闭
	ErrTxClosed = errors.New("tx closed")

	// ErrDatabaseReadOnly 数据库是只读模式
	ErrDatabaseReadOnly = errors.New("database is in read-only mode")
)

// These errors can occur when putting or deleting a value or a bucket.
var (
	// ErrBucketNotFound 没有找到对应的bucket
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketExists bucket已存在
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNameRequired bucket name 不符合要求
	ErrBucketNameRequired = errors.New("bucket name required")

	// ErrKeyRequired bucket key name 不符合要求
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge bucket key 太大
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge bucket value 太大
	ErrValueTooLarge = errors.New("value too large")

	// ErrIncompatibleValue 非法操作
	ErrIncompatibleValue = errors.New("incompatible value")
)
