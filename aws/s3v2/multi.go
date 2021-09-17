package s3v2

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shideqin/storage/storagebase"
	"github.com/shideqin/storage/storageutil"
)

// InitUploadResult 初始化上传结果
type InitUploadResult = storagebase.InitUploadResult

// CopyPartResult 复制分片结果
type CopyPartResult = storagebase.CopyPartResult

// CompleteUploadResult 完成上传结果
type CompleteUploadResult = storagebase.CompleteUploadResult

// UploadLargeFile 分块上传文件
func (c *Client) UploadLargeFile(filePath, bucket, object string, options map[string]string, percentChan chan int) (map[string]interface{}, error) {
	//open本地文件
	fd, openErr := os.Open(filePath)
	if fd != nil {
		defer fd.Close()
	}
	if openErr != nil {
		return nil, fmt.Errorf(" UploadLargeFile Open localFile: %s Error: %v", filePath, openErr)
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
		object = path.Base(filePath)
	}
	if strings.TrimSuffix(object, "/") == path.Dir(object) {
		object = path.Dir(object) + "/" + path.Base(filePath)
	}
	fileStat, _ := fd.Stat()
	fileSize := int(fileStat.Size())
	var total = (fileSize + partSize - 1) / partSize
	if total < threadNum {
		threadNum = total
	}
	//初化化上传
	initUpload, initErr := c.InitUpload(bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
	if initErr != nil {
		return nil, initErr
	}
	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var uploadPartList = make([]string, total)
	var uploadExit bool
	var partErr error
	var wg sync.WaitGroup
	for partNum := 0; partNum < total; partNum++ {
		if uploadExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int, fd *os.File) {
			defer func() {
				if partErr != nil {
					uploadExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			offset := partNum * partSize
			num := partSize
			if fileSize-offset < num {
				num = fileSize - offset
			}
			for i := 0; i < c.maxRetryNum; i++ {
				partReader := io.NewSectionReader(fd, int64(offset), int64(num))
				partReaderSize := int(partReader.Size())
				uploadPart, upErr := c.UploadPart(partReader, partReaderSize, bucket, object, partNum+1, initUpload.UploadID)
				if upErr != nil {
					partErr = upErr
					continue
				}
				partErr = nil
				uploadPartList[partNum] = uploadPart["Etag"].(string)
				break
			}
			if partErr != nil {
				return
			}
			//进度条
			if percentChan != nil {
				percentChan <- total
			}
		}(partNum, fd)
	}
	wg.Wait()
	if partErr != nil {
		return nil, partErr
	}
	//上传完成
	completeUploadInfo := "<CompleteMultipartUpload>"
	for partNum, Etag := range uploadPartList {
		completeUploadInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, Etag)
	}
	completeUploadInfo += "</CompleteMultipartUpload>"
	return c.CompleteUpload([]byte(completeUploadInfo), bucket, object, initUpload.UploadID, fileSize)
}

// CopyLargeFile 分块复制文件
func (c *Client) CopyLargeFile(bucket, object, source string, options map[string]string, percentChan chan int, exitChan <-chan bool) (map[string]interface{}, error) {
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
	var total = (objectSize + partSize - 1) / partSize
	if total < threadNum {
		threadNum = total
	}

	//初化化上传
	initUpload, initErr := c.InitUpload(bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
	if initErr != nil {
		return nil, initErr
	}
	var copyPartList = make([]string, total)

	//取消处理
	var copyExitChan = make(chan bool)
	var copyCancel bool
	go func() {
		for {
			exit, ok := <-exitChan
			if !ok {
				close(copyExitChan)
				return
			}
			if exit {
				copyCancel = true
				copyExitChan <- true
			}
		}
	}()

	//copy分片
	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var partErr error
	var copyExit bool
	var wg sync.WaitGroup
	for partNum := 0; partNum < total; partNum++ {
		if copyExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int) {
			defer func() {
				wg.Done()
				if partErr != nil {
					copyExit = true
				}
			}()
			//part范围,如：0-1023
			tmpStart := partNum * partSize
			tmpEnd := (partNum+1)*partSize - 1
			if tmpEnd > objectSize {
				tmpEnd = tmpStart + objectSize%partSize - 1
			}
			partRange := fmt.Sprintf("bytes=%d-%d", tmpStart, tmpEnd)
			for i := 0; i < c.maxRetryNum; i++ {
				if copyCancel {
					partErr = errors.New("canceled")
					break
				}
				copyPart, copyErr := c.CopyPart(partRange, bucket, object, source, partNum+1, initUpload.UploadID, copyExitChan)
				if copyErr != nil {
					partErr = copyErr
					continue
				}
				partErr = nil
				copyPartList[partNum] = copyPart["Etag"]
				break
			}
			if partErr != nil {
				return
			}
			//进度条
			if percentChan != nil {
				percentChan <- total
			}
			<-queueMaxSize
		}(partNum)
	}
	wg.Wait()
	if partErr != nil {
		return nil, partErr
	}
	//copy完成
	completeCopyInfo := "<CompleteMultipartUpload>"
	for partNum, Etag := range copyPartList {
		completeCopyInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, Etag)
	}
	completeCopyInfo += "</CompleteMultipartUpload>"
	return c.CompleteUpload([]byte(completeCopyInfo), bucket, object, initUpload.UploadID, objectSize)
}

func (c *Client) InitUpload(bucket, object string, options map[string]string) (*InitUploadResult, error) {
	subObject := "?uploads"
	addr := fmt.Sprintf("http://%s.%s/%s%s", bucket, c.host, object, subObject)
	method := "POST"
	contentType := mime.TypeByExtension(path.Ext(object))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	if options["acl"] != "" {
		headers["x-amz-acl"] = options["acl"]
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF, headers, bucket, object+subObject)
	if options["disposition"] != "" {
		headers["Content-Disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" InitUpload Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" InitUpload Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" InitUpload Object: %s Error: respond body is nil", object)
	}
	var initUpload = &InitUploadResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), initUpload); err != nil {
		return nil, fmt.Errorf(" InitUpload Object: %s Error: %v", object, err)
	}
	return initUpload, nil
}

func (c *Client) UploadPart(body io.Reader, bodySize int, bucket, object string, partNumber int, uploadID string) (map[string]interface{}, error) {
	subObject := fmt.Sprintf("?partNumber=%d&uploadId=%s", partNumber, uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s%s", bucket, c.host, object, subObject)
	method := "PUT"
	contentType := mime.TypeByExtension(path.Ext(object))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	LF := "\n"
	object += subObject
	headers["Authorization"] = c.sign(method+LF, headers, bucket, object)
	headers["Content-Length"] = fmt.Sprintf("%d", bodySize)
	resp, err := storageutil.CURL(addr, method, headers, body)
	if err != nil {
		return nil, fmt.Errorf(" UploadPart Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" UploadPart Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	return resp, nil
}

func (c *Client) CancelPart(bucket, object string, uploadID string) (map[string]interface{}, error) {
	subObject := fmt.Sprintf("?uploadId=%s", uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s%s", bucket, c.host, object, subObject)
	method := "DELETE"
	contentType := mime.TypeByExtension(path.Ext(object))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	LF := "\n"
	object += subObject
	headers["Authorization"] = c.sign(method+LF, headers, bucket, object)
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" CancelPart Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 204 {
		return nil, fmt.Errorf(" CancelPart Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	return resp, nil
}

func (c *Client) CopyPart(partRange, bucket, object, source string, partNumber int, uploadID string, copyExitChan <-chan bool) (map[string]string, error) {
	subObject := fmt.Sprintf("?partNumber=%d&uploadId=%s", partNumber, uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s%s", bucket, c.host, object, subObject)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date":                    date,
		"x-amz-copy-source":       source,
		"x-amz-copy-source-range": partRange,
	}
	LF := "\n"
	object += subObject
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	resp, err := storageutil.CURL2Reader(addr, method, headers, strings.NewReader(""), copyExitChan)
	if err != nil {
		return nil, fmt.Errorf(" CopyPart Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" CopyPart Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" CopyPart Object: %s Error: respond body is nil", object)
	}
	var copyPart = &CopyPartResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), copyPart); err != nil {
		return nil, fmt.Errorf(" CopyPart Object: %s Error: %v", object, err)
	}
	return map[string]string{"Etag": copyPart.ETag}, nil
}

func (c *Client) CompleteUpload(body []byte, bucket, object, uploadID string, objectSize int) (map[string]interface{}, error) {
	subObject := fmt.Sprintf("?uploadId=%s", uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s%s", bucket, c.host, object, subObject)
	method := "POST"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(len(body))
	contentMd5 := storageutil.Base64Encode(storageutil.Md5Byte(body))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Md5":  contentMd5,
		"Content-Type": contentType,
		"Date":         date,
	}

	headers["Authorization"] = c.sign(method, headers, bucket, object+subObject)
	headers["Content-Length"] = contentLength
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf(" CompleteUpload Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" CompleteUpload Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" CompleteUpload Object: %s Error: respond body is nil", object)
	}
	var completeUpload = &CompleteUploadResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), completeUpload); err != nil {
		return nil, fmt.Errorf(" CompleteUpload Object: %s Error: %v", object, err)
	}
	return map[string]interface{}{
		"Location": fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object),
		"Bucket":   completeUpload.Bucket,
		"Key":      completeUpload.Key,
		"ETag":     completeUpload.ETag,
		"Size":     objectSize,
	}, nil
}
