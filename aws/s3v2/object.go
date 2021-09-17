package s3v2

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shideqin/storage/storagebase"
	"github.com/shideqin/storage/storageutil"
)

// CopyObjectResult COPY结果
type CopyObjectResult = storagebase.CopyObjectResult

// ListObjectResult 列表结果
type ListObjectResult = storagebase.ListObjectResult

// ListObjectPrefixes 列表前缀
type ListObjectPrefixes = storagebase.ListObjectPrefixes

// ListObjectContents 列表内容
type ListObjectContents = storagebase.ListObjectContents

// UploadFile 上传文件根据路径
func (c *Client) UploadFile(filePath, bucket, object string, options map[string]string) (map[string]interface{}, error) {
	fd, err := os.Open(filePath)
	if fd != nil {
		defer fd.Close()
	}
	if err != nil {
		return nil, fmt.Errorf(" UploadFile Open localFile: %s Error: %v", filePath, err)
	}
	stat, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf(" UploadFile Stat localFile: %s Error: %v", filePath, err)
	}
	bodySize := int(stat.Size())
	if object == "" {
		object = path.Base(filePath)
	}
	if strings.TrimSuffix(object, "/") == path.Dir(object) {
		object = path.Dir(object) + "/" + path.Base(filePath)
	}
	return c.Put(fd, bodySize, bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
}

// Put 上传文件根据内容
func (c *Client) Put(body io.Reader, bodySize int, bucket, object string, options map[string]string) (map[string]interface{}, error) {
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object)
	method := "PUT"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentMd5 := storageutil.Base64Encode(storageutil.Md5ByteReader(body))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Content-Md5":  contentMd5,
		"Content-Type": contentType,
		"Date":         date,
	}
	if options["acl"] != "" {
		headers["x-amz-acl"] = options["acl"]
	}
	headers["Authorization"] = c.sign(method, headers, bucket, object)
	headers["Content-Length"] = fmt.Sprintf("%d", bodySize)
	if options["disposition"] != "" {
		headers["Content-Disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	resp, err := storageutil.CURL(addr, method, headers, body)
	if err != nil {
		return nil, fmt.Errorf(" Put Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" Put Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	return map[string]interface{}{
		"X-Amz-Request-Id": reqID,
		"StatusCode":       status,
		"Location":         fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object),
		"Size":             bodySize,
		"Bucket":           bucket,
		"ETag":             resp["Etag"].(string),
		"Key":              object,
	}, nil
}

// Copy 复制文件
func (c *Client) Copy(bucket, object, source string, options map[string]string) (map[string]interface{}, error) {
	//source head
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourceObject := strings.Join(tmpSourceInfo[2:], "/")
	sourceHead, err := c.Head(sourceBucket, sourceObject)
	if err != nil {
		return nil, err
	}
	if object == "" {
		object = path.Base(sourceObject)
	}
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date":              date,
		"x-amz-copy-source": source,
	}
	if options["acl"] != "" {
		headers["x-amz-acl"] = options["acl"]
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	if options["disposition"] != "" {
		headers["response-content-disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" Copy Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" Copy Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" Copy Object: %s Error: respond body is nil", object)
	}
	var CopyObject = &CopyObjectResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), CopyObject); err != nil {
		return nil, fmt.Errorf(" Copy Object: %s Error: %v", object, err)
	}
	var contentLength int
	if l, ok := sourceHead["Content-Length"]; ok {
		contentLength, _ = strconv.Atoi(l.(string))
	}
	return map[string]interface{}{
		"X-Amz-Request-Id": reqID,
		"StatusCode":       status,
		"Location":         fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object),
		"Size":             contentLength,
		"Bucket":           bucket,
		"ETag":             CopyObject.ETag,
		"Key":              object,
	}, nil
}

// Delete 删除文件
func (c *Client) Delete(bucket, object string) (map[string]interface{}, error) {
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object)
	method := "DELETE"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" Delete Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 && status != 204 {
		return nil, fmt.Errorf(" Delete Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	return resp, nil
}

// Head 查看文件信息
func (c *Client) Head(bucket, object string) (map[string]interface{}, error) {
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object)
	method := "HEAD"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	resp, err := storageutil.Header(addr, method, headers)
	if err != nil {
		return nil, fmt.Errorf(" Head Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" Head Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	return resp, nil
}

// Get 下载文件到本地
func (c *Client) Get(bucket, object, localFile string, options map[string]string, percentChan chan int) (map[string]string, error) {
	objectHead, headErr := c.Head(bucket, object)
	if headErr != nil {
		return nil, headErr
	}
	var objectSize int
	if l, ok := objectHead["Content-Length"]; ok {
		objectSize, _ = strconv.Atoi(l.(string))
	}
	//当没指定文件名时，默认使用object的文件名
	if strings.TrimSuffix(localFile, "/") == path.Dir(localFile) {
		localFile = path.Dir(localFile) + "/" + path.Base(object)
	}
	var partSize = c.partMinSize
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

	//创建local文件
	var localDir = path.Dir(localFile)
	err := os.MkdirAll(localDir, 0755)
	if err != nil {
		return nil, fmt.Errorf(" Get MkdirAll LocalDir: %s Error: %v", localDir, err)
	}
	fd, oErr := os.OpenFile(localFile, os.O_CREATE|os.O_WRONLY, 0755)
	if fd != nil {
		defer fd.Close()
	}
	if oErr != nil {
		return nil, fmt.Errorf(" Get OpenFile localFile: %s Error: %v", localFile, oErr)
	}

	var total = (objectSize + partSize - 1) / partSize
	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var partErr error
	var partExit bool
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
			for i := 0; i < c.maxRetryNum; i++ {
				cat, cErr := c.Cat(bucket, object, partRange)
				if cErr != nil {
					partErr = cErr
					continue
				}
				_, _ = fd.Seek(int64(tmpStart), 0)
				_, cErr = io.Copy(fd, cat["Body"].(*bytes.Buffer))
				if cErr != nil {
					partErr = cErr
					continue
				}
				partErr = nil
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
	return map[string]string{"Object": object, "Localfile": localFile}, nil
}

// Cat 读取文件内容
func (c *Client) Cat(bucket, object string, param ...string) (map[string]interface{}, error) {
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	//分片请求
	partRange := ""
	if len(param) > 0 {
		partRange = param[0]
	}
	if partRange != "" {
		headers["Range"] = partRange
	}
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" Cat Object: %s Error: %v", object, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 && status != 206 {
		return nil, fmt.Errorf(" Cat Object: %s StatusCode: %d X-Amz-Request-Id: %s", object, status, reqID)
	}
	return resp, nil
}

// UploadFromDir 上传目录
func (c *Client) UploadFromDir(localDir, bucket, prefix string, options map[string]string, percentChan chan int) (map[string]int, error) {
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	suffix := ""
	if options["suffix"] != "" {
		suffix = options["suffix"]
	}
	localDir = strings.TrimSuffix(localDir, "/") + "/"
	fileList := storageutil.WalkDir(localDir, suffix)
	total := len(fileList)
	var threadNum = c.threadMaxNum
	if options["thread_num"] != "" {
		n, err := strconv.Atoi(options["thread_num"])
		if err == nil && n <= c.threadMaxNum && n >= c.threadMinNum {
			threadNum = n
		}
	}
	if total < threadNum {
		threadNum = total
	}

	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var fileErr error
	var fileExit bool
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
	var wg sync.WaitGroup
	for fileNum := 0; fileNum < total; fileNum++ {
		if fileExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(fileName string) {
			defer func() {
				if fileErr != nil {
					fileExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			object := prefix + fileName
			isSkipped := false
			localFileStat, err := os.Stat(localDir + fileName)
			if err != nil {
				fileErr = fmt.Errorf(" UploadFromDir Stat localFile: %s%s Error: %v", localDir, fileName, err)
				return
			}
			localFileSize := localFileStat.Size()
			localFileTime := localFileStat.ModTime()
			if options["replace"] != "true" {
				var objectHead, _ = c.Head(bucket, object)
				var objectHeadSize int64
				if l, ok := objectHead["Content-Length"]; ok {
					objectHeadSize, _ = strconv.ParseInt(l.(string), 10, 64)
				}
				if localFileSize == objectHeadSize {
					var objectTime time.Time
					if m, ok := objectHead["Last-Modified"]; ok {
						objectTime, _ = time.Parse(c.dateTimeGMT, m.(string))
					}
					if objectTime.Unix() >= localFileTime.Unix() {
						isSkipped = true
						atomic.AddInt64(&tmpSkip, 1)
					}
				}
			}
			if !isSkipped {
				fd, oErr := os.Open(localDir + fileName)
				if fd != nil {
					defer fd.Close()
				}
				if oErr != nil {
					fileErr = fmt.Errorf(" UploadFromDir Open localFile: %s%s Error: %v", localDir, fileName, oErr)
					return
				}
				stat, sErr := fd.Stat()
				if sErr != nil {
					fileErr = fmt.Errorf(" UploadFromDir Stat localFile: %s%s Error: %v", localDir, fileName, sErr)
					return
				}
				bodySize := int(stat.Size())
				for i := 0; i < c.maxRetryNum; i++ {
					_, fileErr = c.Put(fd, bodySize, bucket, object, map[string]string{"disposition": fileName, "acl": options["acl"]})
					if fileErr != nil {
						continue
					}
					break
				}
				if fileErr != nil {
					return
				}
				atomic.AddInt64(&tmpSize, localFileSize)
				atomic.AddInt64(&tmpFinish, 1)
			}
			percentChan <- total
		}(fileList[fileNum])
	}
	wg.Wait()
	if fileErr != nil {
		return nil, fileErr
	}
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	size := int(atomic.LoadInt64(&tmpSize))
	return map[string]int{"Total": total, "Skip": skip, "Finish": finish, "Size": size}, nil
}

// ListObject 查看列表
func (c *Client) ListObject(bucket string, options map[string]string) (*ListObjectResult, error) {
	param := ""
	if options["delimiter"] != "" {
		param += "&delimiter=" + options["delimiter"]
	}
	if options["marker"] != "" {
		param += "&marker=" + options["marker"]
	}
	if options["max-keys"] != "" {
		param += "&max-keys=" + options["max-keys"]
	}
	if options["prefix"] != "" {
		param += "&prefix=" + options["prefix"]
	}
	addr := fmt.Sprintf("http://%s.%s/?%s", bucket, c.host, strings.TrimPrefix(param, "&"))
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/", "")
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" ListObject Bucket: %s Error: %v", bucket, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" ListObject Bucket: %s StatusCode: %d X-Amz-Request-Id: %s", bucket, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" ListObject Bucket: %s Error: respond body is nil", bucket)
	}
	var listObject = &ListObjectResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), listObject); err != nil {
		return nil, fmt.Errorf(" ListObject Bucket: %s Error: %v", bucket, err)
	}
	return listObject, nil
}

// CopyAllObject 复制目录
func (c *Client) CopyAllObject(bucket, prefix, source string, options map[string]string, percentChan chan int) (map[string]int, error) {
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
	var copyExit bool
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
	var wg sync.WaitGroup
LIST:
	sourceList, err := c.ListObject(sourceBucket, map[string]string{"prefix": sourcePrefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	sourceObjectNum := len(sourceList.Contents)
	total += sourceObjectNum
	var fileErr error
	for fileNum := 0; fileNum < sourceObjectNum; fileNum++ {
		if copyExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo ListObjectContents) {
			defer func() {
				if fileErr != nil {
					copyExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			//根据后缀过滤处理
			isSkipped := false
			if options["suffix"] != "" {
				suffixList := strings.Split(options["suffix"], ",")
				for _, tmpSuffix := range suffixList {
					if tmpSuffix != "" {
						if strings.HasSuffix(strings.ToLower(objectInfo.Key), tmpSuffix) {
							isSkipped = true
							break
						}
					}
				}
			}
			//支持自定义前缀
			object := prefix
			if options["full_path"] == "true" {
				object += objectInfo.Key
			} else {
				object += path.Base(objectInfo.Key)
			}
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
					}
				}
			}
			if isSkipped {
				atomic.AddInt64(&tmpSkip, 1)
			} else {
				tmpSourceObject := "/" + sourceBucket + "/" + objectInfo.Key
				_, fileErr = c.CopyLargeFile(bucket, object, tmpSourceObject, map[string]string{"disposition": disposition, "acl": options["acl"]}, nil, nil)
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

// DeleteAllObject 删除目录
func (c *Client) DeleteAllObject(bucket, prefix string, options map[string]string, percentChan chan int) (map[string]int, error) {
	bodyList := make([]string, 0)
	bodyListNum := make([]int, 0)
	marker := ""
	total := 0
	var tmpFinish int64
LIST:
	list, err := c.ListObject(bucket, map[string]string{"prefix": prefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	total += len(list.Contents)
	if total <= 0 {
		return map[string]int{"total": 0, "finish": 0}, nil
	}
	body := "<Delete>"
	body += "<Quiet>true</Quiet>"
	for _, v := range list.Contents {
		body += "<Object><Key>" + v.Key + "</Key></Object>"
		marker = v.Key
	}
	body += "</Delete>"
	bodyList = append(bodyList, body)
	bodyListNum = append(bodyListNum, len(list.Contents))
	if list.IsTruncated == "true" {
		goto LIST
	}
	var threadNum = c.threadMaxNum
	if options["thread_num"] != "" {
		n, err := strconv.Atoi(options["thread_num"])
		if err == nil && n <= c.threadMaxNum && n >= c.threadMinNum {
			threadNum = n
		}
	}
	var bodyNum = len(bodyList)
	if bodyNum < threadNum {
		threadNum = bodyNum
	}
	var queueMaxSize = make(chan bool, threadNum)
	defer close(queueMaxSize)
	var fileErr error
	var fileExit bool
	var wg sync.WaitGroup
	for fileNum := 0; fileNum < bodyNum; fileNum++ {
		if fileExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(fileNum int, body string) {
			defer func() {
				if fileErr != nil {
					fileExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			object := "?delete"
			addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object)
			method := "POST"
			date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
			contentLength := strconv.Itoa(len(body))
			contentMd5 := storageutil.Base64Encode(storageutil.Md5Byte([]byte(body)))
			headers := map[string]string{
				"Content-Md5": contentMd5 + "\n",
				"Date":        date,
			}
			headers["Authorization"] = c.sign(method, headers, bucket, object)
			headers["Content-Length"] = contentLength
			headers["Content-Md5"] = strings.TrimSuffix(headers["Content-Md5"], "\n")
			resp, cErr := storageutil.CURL(addr, method, headers, bytes.NewBufferString(body))
			if cErr != nil {
				fileErr = fmt.Errorf(" DeleteAllObject Prefix: %s Error: %v", prefix, cErr)
				return
			}
			status := resp["StatusCode"].(int)
			reqID := resp["X-Amz-Request-Id"].(string)
			if status != 200 {
				fileErr = fmt.Errorf(" DeleteAllObject Prefix: %s StatusCode: %d X-Amz-Request-Id: %s", prefix, status, reqID)
				return
			}
			atomic.AddInt64(&tmpFinish, int64(bodyListNum[fileNum]))
			percentChan <- total
		}(fileNum, bodyList[fileNum])
	}
	wg.Wait()
	if fileErr != nil {
		return nil, fileErr
	}
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"Total": total, "Finish": finish}, nil
}

// MoveAllObject 移动目录
func (c *Client) MoveAllObject(bucket, prefix, source string, options map[string]string, percentChan chan int) (map[string]int, error) {
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
	var copyExit bool
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
	var wg sync.WaitGroup
LIST:
	sourceList, err := c.ListObject(sourceBucket, map[string]string{"prefix": sourcePrefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	sourceObjectNum := len(sourceList.Contents)
	total += sourceObjectNum
	var fileErr error
	for fileNum := 0; fileNum < sourceObjectNum; fileNum++ {
		if copyExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo ListObjectContents) {
			defer func() {
				if fileErr != nil {
					copyExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			//根据后缀过滤处理
			isSkipped := false
			if options["suffix"] != "" {
				suffixList := strings.Split(options["suffix"], ",")
				for _, tmpSuffix := range suffixList {
					if tmpSuffix != "" {
						if strings.HasSuffix(strings.ToLower(objectInfo.Key), tmpSuffix) {
							isSkipped = true
							break
						}
					}
				}
			}
			//支持自定义前缀
			object := prefix
			if options["full_path"] == "true" {
				object += objectInfo.Key
			} else {
				object += path.Base(objectInfo.Key)
			}
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
					}
				}
			}
			if isSkipped {
				atomic.AddInt64(&tmpSkip, 1)
			} else {
				tmpSourceObject := "/" + sourceBucket + "/" + objectInfo.Key
				_, fileErr = c.CopyLargeFile(bucket, object, tmpSourceObject, map[string]string{"disposition": disposition, "acl": options["acl"]}, nil, nil)
				if fileErr != nil {
					return
				}
				//删除源文件
				for i := 0; i < c.maxRetryNum; i++ {
					_, fileErr = c.Delete(sourceBucket, objectInfo.Key)
					if fileErr != nil {
						continue
					}
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

// DownloadAllObject 下载目录
func (c *Client) DownloadAllObject(bucket, prefix, localDir string, options map[string]string, percentChan chan int) (map[string]int, error) {
	marker := ""
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
	var tmpSkip int64
	var tmpFinish int64
	var wg sync.WaitGroup
LIST:
	list, err := c.ListObject(bucket, map[string]string{"prefix": prefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	objectNum := len(list.Contents)
	total += objectNum
	var fileErr error
	var fileExit bool
	for fileNum := 0; fileNum < objectNum; fileNum++ {
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
			localFile := strings.TrimSuffix(localDir, "/") + "/" + objectInfo.Key
			isSkipped := false
			if options["replace"] != "true" {
				var objectHead, _ = c.Head(bucket, objectInfo.Key)
				var objectHeadSize int64
				if l, ok := objectHead["Content-Length"]; ok {
					objectHeadSize, _ = strconv.ParseInt(l.(string), 10, 64)
				}
				fileStat, sErr := os.Stat(localFile)
				if sErr == nil && objectHeadSize == fileStat.Size() {
					var objectTime time.Time
					if m, ok := objectHead["Last-Modified"]; ok {
						objectTime, _ = time.Parse(c.dateTimeGMT, m.(string))
					}
					if objectTime.Unix() >= fileStat.ModTime().Unix() {
						isSkipped = true
						atomic.AddInt64(&tmpSkip, 1)
					}
				}
			}
			if !isSkipped {
				var getPercent = make(chan int)
				defer close(getPercent)
				go func() {
					for {
						_, ok := <-getPercent
						if !ok {
							break
						}
					}
				}()
				_, fileErr = c.Get(bucket, objectInfo.Key, localFile, map[string]string{
					"thread_num": options["thread_num"],
					"part_size":  options["part_size"],
				}, getPercent)
				if fileErr != nil {
					return
				}
				atomic.AddInt64(&tmpFinish, 1)
			}
			percentChan <- total
		}(list.Contents[fileNum])
	}
	wg.Wait()
	if fileErr == nil && list.IsTruncated == "true" {
		marker = list.Contents[objectNum-1].Key
		goto LIST
	}
	if fileErr != nil {
		return nil, fileErr
	}
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"Total": total, "Skip": skip, "Finish": finish}, nil
}
