package storagebase

// ServiceResult 获取bucket列表结果
type ServiceResult struct {
	Owner struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	} `xml:"Owner"`
	Buckets struct {
		Bucket []struct {
			Name         string `xml:"Name"`
			CreationDate string `xml:"CreationDate"`
		} `xml:"Bucket"`
	} `xml:"Buckets"`
}

// AclResult 获取bucket Acl列表结果
type AclResult struct {
	Owner struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	} `xml:"Owner"`
	AccessControlList struct {
		Grant struct {
			Permission string `xml:"Permission"`
		} `xml:"Grant"`
	} `xml:"AccessControlList"`
}

// ListPartsResult 获取分块列表结果
type ListPartsResult struct {
	NextKeyMarker      string `xml:"NextKeyMarker"`
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	IsTruncated        string `xml:"IsTruncated"`
	Upload             []struct {
		Key       string `xml:"Key"`
		UploadID  string `xml:"UploadId"`
		Initiated string `xml:"Initiated"`
	} `xml:"Upload"`
}

// InitUploadResult 初始化上传结果
type InitUploadResult struct {
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

// CompleteUploadResult 完成上传结果
type CompleteUploadResult struct {
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	ETag     string `xml:"ETag"`
}

// CopyPartResult 复制分片结果
type CopyPartResult struct {
	ETag string `xml:"ETag"`
}

// CopyObjectResult COPY结果
type CopyObjectResult struct {
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
}

// ListObjectResult 列表结果
type ListObjectResult struct {
	Name           string               `xml:"Name"`
	Prefix         string               `xml:"Prefix"`
	Marker         string               `xml:"Marker"`
	MaxKeys        string               `xml:"MaxKeys"`
	Delimiter      string               `xml:"Delimiter"`
	IsTruncated    string               `xml:"IsTruncated"`
	CommonPrefixes []ListObjectPrefixes `xml:"CommonPrefixes"`
	Contents       []ListObjectContents `xml:"Contents"`
}

// ListObjectPrefixes 列表前缀
type ListObjectPrefixes struct {
	Prefix string `xml:"Prefix"`
}

// ListObjectContents 列表内容
type ListObjectContents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Type         string `xml:"Type"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}
