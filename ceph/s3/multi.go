package s3

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/shideqin/storage/base"
	"mime"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InitUploadResult 初始化上传结果
type InitUploadResult = base.InitUploadResult

// CopyPartResult 复制分片结果
type CopyPartResult = base.CopyPartResult

// CompleteUploadResult 完成上传结果
type CompleteUploadResult = base.CompleteUploadResult

// UploadLargeFile 分块上传文件
func (c *Client) UploadLargeFile(filePath, bucket, object string, options map[string]string) (map[string]string, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	//open本地文件
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

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
	if strings.TrimRight(object, "/") == path.Dir(object) {
		object = strings.TrimRight(object, "/") + "/" + path.Base(filePath)
	}
	fileStat, _ := fd.Stat()
	fileSize := int(fileStat.Size())
	var total = (fileSize + partSize - 1) / partSize
	if total < threadNum {
		threadNum = total
	}
	//初化化上传
	initUpload, err := c.InitUpload(bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
	if err != nil {
		return nil, err
	}
	var uploadPartList = make([]string, total)
	var queueMaxSize = make(chan bool, threadNum)
	var uploadPercent = make(chan bool)
	var uploadDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-uploadPercent
			if !ok {
				close(uploadDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%% ", float64(finishNum)/float64(total)*100)
		}
	}()

	partNum := 0
	for {
		if partNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int, fd *os.File) {
			defer wg.Done()
			offset := partNum * partSize
			num := partSize
			if fileSize-offset < num {
				num = fileSize - offset
			}
			isUploadSuccess := false
			buf := make([]byte, num)
			for i := 0; i < c.maxRetryNum; i++ {
				_, err := fd.ReadAt(buf, int64(offset))
				if err != nil {
					continue
				}
				body := bytes.NewReader(buf)
				uploadPart, err := c.UploadPart(body, bucket, object, partNum+1, initUpload.UploadID)
				if err != nil {
					continue
				}
				uploadPartList[partNum] = uploadPart["Etag"]
				isUploadSuccess = true
				break
			}
			if !isUploadSuccess {
				fmt.Printf("\nUpload Part Fail PartNum:%d\n", partNum+1)
				os.Exit(0)
			}
			uploadPercent <- true
			<-queueMaxSize
		}(partNum, fd)
		partNum++
	}
	wg.Wait()
	close(uploadPercent)
	<-uploadDone
	//上传完成
	completeUploadInfo := "<CompleteMultipartUpload>"
	for partNum, Etag := range uploadPartList {
		completeUploadInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, Etag)
	}
	completeUploadInfo += "</CompleteMultipartUpload>"
	return c.CompleteUpload([]byte(completeUploadInfo), bucket, object, initUpload.UploadID, fileSize)
}

// CopyLargeFile 分块复制文件
func (c *Client) CopyLargeFile(bucket, object, source string, options map[string]string) (map[string]string, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourceObject := strings.Join(tmpSourceInfo[2:], "/")
	sourceHead, err := c.Head(sourceBucket, sourceObject)
	if err != nil {
		return nil, err
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
	if strings.TrimRight(object, "/") == path.Dir(object) {
		object = strings.TrimRight(object, "/") + "/" + path.Base(sourceObject)
	}
	objectSize, err := strconv.Atoi(sourceHead["Content-Length"])
	if err != nil {
		return nil, err
	}

	var total = (objectSize + partSize - 1) / partSize
	if total < threadNum {
		threadNum = total
	}

	//初化化上传
	initUpload, err := c.InitUpload(bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
	if err != nil {
		return nil, err
	}
	var copyPartList = make([]string, total)
	var queueMaxSize = make(chan bool, threadNum)
	var copyPercent = make(chan bool)
	var copyDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-copyPercent
			if !ok {
				close(copyDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%% ", float64(finishNum)/float64(total)*100)
		}
	}()

	partNum := 0
	//copy分片
	for {
		if partNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int) {
			defer wg.Done()
			//part范围,如：0-1023
			tmpStart := partNum * partSize
			tmpEnd := (partNum+1)*partSize - 1
			if tmpEnd > objectSize {
				tmpEnd = tmpStart + objectSize%partSize - 1
			}
			partRange := fmt.Sprintf("bytes=%d-%d", tmpStart, tmpEnd)
			isCopySuccess := false
			for i := 0; i < c.maxRetryNum; i++ {
				copyPart, err := c.CopyPart(partRange, bucket, object, source, partNum+1, initUpload.UploadID)
				if err != nil {
					continue
				}
				copyPartList[partNum] = copyPart["Etag"]
				isCopySuccess = true
				break
			}
			if !isCopySuccess {
				fmt.Printf("\nUpload Part Fail,PartNum: %d\n", partNum+1)
				os.Exit(0)
			}
			copyPercent <- true
			<-queueMaxSize
		}(partNum)
		partNum++
	}
	wg.Wait()
	close(copyPercent)
	<-copyDone
	//copy完成
	completeCopyInfo := "<CompleteMultipartUpload>"
	for partNum, Etag := range copyPartList {
		completeCopyInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, Etag)
	}
	completeCopyInfo += "</CompleteMultipartUpload>"
	return c.CompleteUpload([]byte(completeCopyInfo), bucket, object, initUpload.UploadID, objectSize)
}

func (c *Client) InitUpload(bucket, object string, options map[string]string) (*InitUploadResult, error) {
	subobject := "?uploads"
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object+subobject)
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
	headers["Authorization"] = c.sign(method+LF, headers, bucket, object+subobject)
	if options["disposition"] != "" {
		headers["Content-Disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"])
	}
	var initUpload = &InitUploadResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), initUpload); err != nil {
		return nil, err
	}
	return initUpload, nil
}

func (c *Client) UploadPart(body *bytes.Reader, bucket, object string, partNumber int, uploadID string) (map[string]string, error) {
	subobject := fmt.Sprintf("?partNumber=%d&uploadId=%s", partNumber, uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object+subobject)
	method := "PUT"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(int(body.Size()))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	LF := "\n"
	object += subobject
	headers["Authorization"] = c.sign(method+LF, headers, bucket, object)
	headers["Content-Length"] = contentLength
	res, err := c.curl2Reader(addr, method, headers, body)
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"])
	}
	return res, nil
}

func (c *Client) CancelPart(bucket, object string, uploadID string) (map[string]string, error) {
	subobject := fmt.Sprintf("?uploadId=%s", uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object+subobject)
	method := "DELETE"
	contentType := mime.TypeByExtension(path.Ext(object))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	LF := "\n"
	object += subobject
	headers["Authorization"] = c.sign(method+LF, headers, bucket, object)
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "204" {
		return nil, errors.New("StatusCode:" + res["StatusCode"])
	}
	return res, nil
}

func (c *Client) CopyPart(partRange, bucket, object, source string, partNumber int, uploadID string) (map[string]string, error) {
	subobject := fmt.Sprintf("?partNumber=%d&uploadId=%s", partNumber, uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object+subobject)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date":                    date,
		"x-amz-copy-source":       source,
		"x-amz-copy-source-range": partRange,
	}
	LF := "\n"
	object += subobject
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"])
	}
	var copyPart = &CopyPartResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), copyPart); err != nil {
		return nil, err
	}
	return map[string]string{"Etag": copyPart.ETag}, nil
}

func (c *Client) CompleteUpload(body []byte, bucket, object, uploadID string, objectSize int) (map[string]string, error) {
	subobject := fmt.Sprintf("?uploadId=%s", uploadID)
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object+subobject)
	method := "POST"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(len(body))
	contentMd5 := c.base64(c.md5Byte(body))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Md5":  contentMd5,
		"Content-Type": contentType,
		"Date":         date,
	}

	headers["Authorization"] = c.sign(method, headers, bucket, object+subobject)
	headers["Content-Length"] = contentLength
	res, err := c.curl(addr, method, headers, body)
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"])
	}
	var completeUpload = &CompleteUploadResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), completeUpload); err != nil {
		return nil, err
	}
	//解决腾讯COS返回bucket不完整bug
	if completeUpload.Location == "" {
		completeUpload.Location = fmt.Sprintf("http://%s.%s/%s", completeUpload.Bucket, c.host, completeUpload.Key)
	} else {
		completeUpload.Bucket = strings.Split(completeUpload.Location, ".")[0]
	}
	return map[string]string{
		"Location": fmt.Sprintf("http://%s", completeUpload.Location),
		"Bucket":   completeUpload.Bucket,
		"Key":      completeUpload.Key,
		"ETag":     completeUpload.ETag,
		"Size":     strconv.Itoa(objectSize),
	}, nil
}
