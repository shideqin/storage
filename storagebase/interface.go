package storagebase

import (
	"io"
)

// IClient Client 客户端结构
type IClient interface {
	GetService() (*ServiceResult, error)
	CreateBucket(bucket string, options map[string]string) (map[string]interface{}, error)
	DeleteBucket(bucket string) (map[string]interface{}, error)
	ListPart(bucket string, options map[string]string) (*ListPartsResult, error)
	DeleteAllPart(bucket, prefix string, options map[string]string, percentChan chan int) (map[string]int, error)
	GetACL(bucket string) (*AclResult, error)
	SetACL(bucket string, options map[string]string) (map[string]interface{}, error)

	UploadLargeFile(filePath, bucket, object string, options map[string]string, percentChan chan int) (map[string]interface{}, error)
	CopyLargeFile(bucket, object, source string, options map[string]string, percentChan chan int, exitChan <-chan bool) (map[string]interface{}, error)
	InitUpload(bucket, object string, options map[string]string) (*InitUploadResult, error)
	UploadPart(body io.Reader, bodySize int, bucket, object string, partNumber int, uploadID string) (map[string]interface{}, error)
	CancelPart(bucket, object string, uploadID string) (map[string]interface{}, error)
	CopyPart(partRange, bucket, object, source string, partNumber int, uploadID string, exitChan <-chan bool) (map[string]string, error)
	CompleteUpload(body []byte, bucket, object, uploadID string, objectSize int) (map[string]interface{}, error)

	SyncLargeFile(toClient IClient, bucket, object, source string, options map[string]string, percentChan chan int) (map[string]interface{}, error)
	SyncAllObject(toClient IClient, bucket, prefix, source string, options map[string]string, percentChan chan int) (map[string]int, error)

	UploadFile(filePath, bucket, object string, options map[string]string) (map[string]interface{}, error)
	Put(body io.Reader, bodySize int, bucket, object string, options map[string]string) (map[string]interface{}, error)
	Copy(bucket, object, source string, options map[string]string) (map[string]interface{}, error)
	Delete(bucket, object string) (map[string]interface{}, error)
	Head(bucket, object string) (map[string]interface{}, error)
	Get(bucket, object, localFile string, options map[string]string, percentChan chan int) (map[string]string, error)
	Cat(bucket, object string, param ...string) (map[string]interface{}, error)
	UploadFromDir(localDir, bucket, prefix string, options map[string]string, percentChan chan int) (map[string]int, error)
	ListObject(bucket string, options map[string]string) (*ListObjectResult, error)
	CopyAllObject(bucket, prefix, source string, options map[string]string, percentChan chan int) (map[string]int, error)
	DeleteAllObject(bucket, prefix string, options map[string]string, percentChan chan int) (map[string]int, error)
	MoveAllObject(bucket, prefix, source string, options map[string]string, percentChan chan int) (map[string]int, error)
	DownloadAllObject(bucket, prefix, localDir string, options map[string]string, percentChan chan int) (map[string]int, error)
}
