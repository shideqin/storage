package storage

import (
	"mss.git.kkyoo.com/shideqin/storage/aws/s3v2"
	"mss.git.kkyoo.com/shideqin/storage/aws/s3v4"
	"mss.git.kkyoo.com/shideqin/storage/storagebase"
)

//GetClient 获得存储客户端
func GetClient(service, host, accessKeyID, accessKeySecret string) storagebase.IClient {
	var client storagebase.IClient
	if service == "aws" {
		client = s3v4.New(host, accessKeyID, accessKeySecret)
	} else {
		client = s3v2.New(host, accessKeyID, accessKeySecret)
	}
	return client
}
