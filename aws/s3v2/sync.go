package s3v2

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shideqin/storage/storagebase"
)

// SyncLargeFile 分块同步文件
func (c *Client) SyncLargeFile(toClient storagebase.IClient, bucket, object, source string, options map[string]string, percentChan chan int) (map[string]interface{}, error) {
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourceObject := strings.Join(tmpSourceInfo[2:], "/")
	sourceHead, headErr := c.Head(sourceBucket, sourceObject)
	if headErr != nil {
		return nil, headErr
	}
	var partSize = c.partMaxSize
	if options["part_size"] != "" {
		n, err := strconv.Atoi(options["part_size"])
		if err == nil && n <= c.partMaxSize && n >= c.partMinSize {
			partSize = n
		}
	}
	var threadNum = c.threadMaxNum
	if options["thread_num"] != "" {
		n, err := strconv.Atoi(options["thread_num"])
		if err == nil && n <= c.threadMaxNum && n >= c.threadMinNum {
			threadNum = n
		}
	}

	if object == "" {
		object = path.Base(sourceObject)
	}
	if strings.TrimSuffix(object, "/") == path.Dir(object) {
		object = path.Dir(object) + "/" + path.Base(sourceObject)
	}
	var objectSize int
	if l, ok := sourceHead["Content-Length"]; ok {
		objectSize, _ = strconv.Atoi(l.(string))
	}
	if !(objectSize > 0) {
		return nil, fmt.Errorf(" SyncLargeFile Object: %s Content-Length cant not zero", object)
	}
	var total = (objectSize + partSize - 1) / partSize
	if total < threadNum {
		threadNum = total
	}

	//初化化上传
	initUpload, initErr := toClient.InitUpload(bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
	if initErr != nil {
		return nil, initErr
	}
	var syncPartList = make([]string, total)
	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var partErr error
	var partExit bool
	//sync分片
	var wg sync.WaitGroup
	for partNum := 0; partNum < total; partNum++ {
		if partExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int) {
			defer func() {
				if partErr != nil {
					partExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			//part范围,如：0-1023
			tmpStart := partNum * partSize
			tmpEnd := (partNum+1)*partSize - 1
			if tmpEnd > objectSize {
				tmpEnd = tmpStart + objectSize%partSize - 1
			}
			partRange := fmt.Sprintf("bytes=%d-%d", tmpStart, tmpEnd)
			partBody := map[string]interface{}{}
			for i := 0; i < c.maxRetryNum; i++ {
				cat, syncErr := c.Cat(sourceBucket, sourceObject, partRange)
				if syncErr != nil {
					partErr = syncErr
					continue
				}
				partErr = nil
				partBody = cat
				break
			}
			if partErr != nil {
				return
			}
			if _, ok := partBody["Body"]; !ok {
				return
			}
			body := partBody["Body"].(*bytes.Buffer).Bytes()
			for i := 0; i < c.maxRetryNum; i++ {
				partReader := bytes.NewReader(body)
				partReaderSize := int(partReader.Size())
				uploadPart, syncErr := toClient.UploadPart(partReader, partReaderSize, bucket, object, partNum+1, initUpload.UploadID)
				if syncErr != nil {
					partErr = syncErr
					continue
				}
				if _, ok := uploadPart["Etag"]; !ok {
					continue
				}
				partErr = nil
				syncPartList[partNum] = uploadPart["Etag"].(string)
				break
			}
			if partErr != nil {
				return
			}
			percentChan <- total
		}(partNum)
	}
	wg.Wait()
	if partErr != nil {
		return nil, partErr
	}
	//sync完成
	completeSyncInfo := "<CompleteMultipartUpload>"
	for partNum, Etag := range syncPartList {
		completeSyncInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, Etag)
	}
	completeSyncInfo += "</CompleteMultipartUpload>"
	return toClient.CompleteUpload([]byte(completeSyncInfo), bucket, object, initUpload.UploadID, objectSize)
}

// SyncAllObject 同步目录
func (c *Client) SyncAllObject(toClient storagebase.IClient, bucket, prefix, source string, options map[string]string, percentChan chan int) (map[string]int, error) {
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	marker := ""
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourcePrefix := strings.Join(tmpSourceInfo[2:], "/")
	total := 0
	var threadNum = c.threadMaxNum
	if options["thread_num"] != "" {
		n, err := strconv.Atoi(options["thread_num"])
		if err == nil && n <= c.threadMaxNum && n >= c.threadMinNum {
			threadNum = n
		}
	}
	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
	var fileErr error
	var fileExit bool
	var wg sync.WaitGroup
LIST:
	sourceList, listErr := c.ListObject(sourceBucket, map[string]string{"prefix": sourcePrefix, "marker": marker, "max-keys": "1000"})
	if listErr != nil {
		return nil, listErr
	}
	sourceObjectNum := len(sourceList.Contents)
	total += sourceObjectNum
	for fileNum := 0; fileNum < sourceObjectNum; fileNum++ {
		if fileExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo ListObjectContents) {
			defer func() {
				if fileErr != nil {
					fileExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			//支持自定义前缀
			object := prefix
			if options["full_path"] == "true" {
				object += objectInfo.Key
			} else {
				object += path.Base(objectInfo.Key)
			}
			isSkipped := false
			var sourceHead, _ = c.Head(sourceBucket, objectInfo.Key)
			var disposition string
			if d, ok := sourceHead["Content-Disposition"]; ok {
				disposition = d.(string)
			}
			var sourceHeadSize int64
			if l, ok := sourceHead["Content-Length"]; ok {
				sourceHeadSize, _ = strconv.ParseInt(l.(string), 10, 64)
			}
			if options["replace"] != "true" {
				var objectHead, _ = c.Head(bucket, object)
				var objectHeadSize int64
				if l, ok := objectHead["Content-Length"]; ok {
					objectHeadSize, _ = strconv.ParseInt(l.(string), 10, 64)
				}
				if sourceHeadSize == objectHeadSize {
					var objectTime, sourceTime time.Time
					if m, ok := objectHead["Last-Modified"]; ok {
						objectTime, _ = time.Parse(c.dateTimeGMT, m.(string))
					}
					if m, ok := sourceHead["Last-Modified"]; ok {
						sourceTime, _ = time.Parse(c.dateTimeGMT, m.(string))
					}
					if objectTime.Unix() >= sourceTime.Unix() {
						isSkipped = true
						atomic.AddInt64(&tmpSkip, 1)
					}
				}
			}
			if !isSkipped {
				sourceBody := map[string]interface{}{}
				for i := 0; i < c.maxRetryNum; i++ {
					cat, syncErr := c.Cat(sourceBucket, objectInfo.Key)
					if syncErr != nil {
						fileErr = syncErr
						continue
					}
					fileErr = nil
					sourceBody = cat
					break
				}
				if fileErr != nil {
					return
				}
				if _, ok := sourceBody["Body"]; !ok {
					return
				}
				body := sourceBody["Body"].(*bytes.Buffer).Bytes()
				for i := 0; i < c.maxRetryNum; i++ {
					partReader := bytes.NewReader(body)
					partReaderSize := int(partReader.Size())
					_, syncErr := toClient.Put(partReader, partReaderSize, bucket, object, map[string]string{"disposition": disposition, "acl": options["acl"]})
					if syncErr != nil {
						fileErr = syncErr
						continue
					}
					fileErr = nil
					break
				}
				if fileErr != nil {
					return
				}
				atomic.AddInt64(&tmpSize, sourceHeadSize)
				atomic.AddInt64(&tmpFinish, 1)
			}
			percentChan <- total
		}(sourceList.Contents[fileNum])
	}
	wg.Wait()
	if fileErr == nil && sourceList.IsTruncated == "true" {
		marker = sourceList.Contents[sourceObjectNum-1].Key
		goto LIST
	}
	if fileErr != nil {
		return nil, fileErr
	}
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	size := int(atomic.LoadInt64(&tmpSize))
	return map[string]int{"Total": total, "Skip": skip, "Finish": finish, "Size": size}, nil
}
