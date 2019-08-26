package client

import (
	"bytes"
	"github.com/shideqin/storage/aliyun/oss"
	"github.com/shideqin/storage/base"
	"github.com/shideqin/storage/ceph/s3"
)

// Client OSS客户端结构
type IClient interface {
	//bucket
	GetService() (*base.ServiceResult, error)
	CreateBucket(bucket string, options map[string]string) (map[string]string, error)
	DeleteBucket(bucket string) (map[string]string, error)
	ListPart(bucket string, options map[string]string) (*base.ListPartsResult, error)
	DeleteAllPart(bucket, prefix string, options map[string]string) (map[string]int, error)
	GetACL(bucket string) (map[string]string, error)
	SetACL(bucket string, options map[string]string) (map[string]string, error)

	//multi
	UploadLargeFile(filePath, bucket, object string, options map[string]string) (map[string]string, error)
	CopyLargeFile(bucket, object, source string, options map[string]string) (map[string]string, error)
	InitUpload(bucket, object string, options map[string]string) (*base.InitUploadResult, error)
	UploadPart(body *bytes.Reader, bucket, object string, partNumber int, uploadID string) (map[string]string, error)
	CopyPart(partRange, bucket, object, source string, partNumber int, uploadID string) (map[string]string, error)
	CompleteUpload(body []byte, bucket, object, uploadID string, objectSize int) (map[string]string, error)

	//object
	UploadFile(filePath, bucket, object string, options map[string]string) (map[string]string, error)
	Put(body []byte, bucket, object string, options map[string]string) (map[string]string, error)
	Copy(bucket, object, source string, options map[string]string) (map[string]string, error)
	Delete(bucket, object string) (map[string]string, error)
	Head(bucket, object string) (map[string]string, error)
	Get(bucket, object, localfile string, options map[string]string) (map[string]string, error)
	Cat(bucket, object string, param ...string) (map[string]string, error)
	CopyFromHlsFile(bucket, prefix, source string, options map[string]string) (map[string]int, error)
	UploadFromDir(localdir, bucket, prefix string, options map[string]string) (map[string]int, error)
	ListObject(bucket string, options map[string]string) (*base.ListObjectResult, error)
	CopyAllObject(bucket, prefix, source string, options map[string]string) (map[string]int, error)
	DeleteAllObject(bucket, prefix string, options map[string]string) (map[string]int, error)
	DownloadAllObject(bucket, prefix, localdir string, options map[string]string) (map[string]int, error)
}

//GetStorageClient 获得存储客户端
func GetStorageClient(cmd, host, accessKeyID, accessKeySecret string) IClient {
	var client IClient
	if cmd == "s3" {
		client = s3.New(host, accessKeyID, accessKeySecret)
	} else {
		client = oss.New(host, accessKeyID, accessKeySecret)
	}
	return client
}
