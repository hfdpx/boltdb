/*
Package bolt implements a low-level key/value store in pure Go. It supports
fully serializable transactions, ACID semantics, and lock-free MVCC with
multiple readers and a single writer. Bolt can be used for projects that
want a simple data store without the need to add large dependencies such as
Postgres or MySQL.

Package bolt在pure Go中实现了一个低级keyvalue存储。
它支持完全可序列化的事务、ACID语义和具有多个读卡器和一个编写器的无锁MVCC。
Bolt可用于需要简单数据存储而无需添加大型依赖项（如Postgres或MySQL）的项目。

Bolt is a single-level, zero-copy, B+tree data store. This means that Bolt is
optimized for fast read access and does not require recovery in the event of a
system crash. Transactions which have not finished committing will simply be
rolled back in the event of a crash.

Bolt是一个单级、零拷贝、B+树数据存储。这意味着Bolt针对快速读取访问进行了优化，
并且在系统崩溃时不需要恢复。未完成提交的事务将在崩溃时回滚。


The design of Bolt is based on Howard Chu's LMDB database project.

Bolt currently works on Windows, Mac OS X, and Linux.


Basics

There are only a few types in Bolt: DB, Bucket, Tx, and Cursor. The DB is
a collection of buckets and is represented by a single file on disk. A bucket is
a collection of unique keys that are associated with values.

Bolt中只有几种类型：DB、Bucket、Tx和Cursor。DB是存储桶的集合，由磁盘上的单个文件表示。bucket是与值关联的唯一键的集合。

Transactions provide either read-only or read-write access to the database.
Read-only transactions can retrieve key/value pairs and can use Cursors to
iterate over the dataset sequentially. Read-write transactions can create and
delete buckets and can insert and remove keys. Only one read-write transaction
is allowed at a time.

事务提供对数据库的只读或读写访问。只读事务可以检索键值对，并可以使用游标顺序遍历数据集。读写事务可以创建和删除存储桶，还可以插入和删除键。一次只允许一个读写事务



Caveats

The database uses a read-only, memory-mapped data file to ensure that
applications cannot corrupt the database, however, this means that keys and
values returned from Bolt cannot be changed. Writing to a read-only byte slice
will cause Go to panic.

数据库使用只读的内存映射数据文件来确保应用程序不会损坏数据库，但是，这意味着无法更改从Bolt返回的键和值。写入只读字节片将导致Go死机。


Keys and values retrieved from the database are only valid for the life of
the transaction. When used outside the transaction, these byte slices can
point to different data or can point to invalid memory which will cause a panic.

从数据库检索的键和值仅在事务的生命周期内有效。当在事务外部使用时，这些字节片可能指向不同的数据，或者指向会导致死机的无效内存。

*/
package bolt
