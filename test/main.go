package main

import (
	"bolt"
	"fmt"
	"log"
	"strconv"
)

func main() {
	// 创建数据库，数据库文件为test.db，所有人可读写执行，采用默认的数据库配置
	db, err := bolt.Open("test.db", 0777, nil)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}

	key := "testKey"
	value := "testValue"
	bucketName := "testBucket"
	// 执行读写事务，采用Update
	err = db.Update(func(tx *bolt.Tx) error {
		// 在此读写事务中，如果该bucket不存在，则先创建bucket

		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))

		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		for i := 0; i < 50; i++ {
			newKey := key + "-" + strconv.Itoa(i)
			newValue := value + "-" + strconv.Itoa(i)
			//fmt.Println(newKey,newValue)
			err = b.Put([]byte(newKey), []byte(newValue))
			if err != nil {
				return fmt.Errorf("insert kv: %s", err)
			}
		}

		fmt.Printf("bucket 头部:%+v\n", b)

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	//err = db.View(func(tx *bolt.Tx) error {
	//	b := tx.Bucket([]byte(bucketName))
	//	v := b.Get([]byte(key))
	//	fmt.Printf("The answer is: %s\n", v)
	//	return nil
	//})
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}

}
