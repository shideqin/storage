package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"lib/storage/base"
	"mime"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CopyObjectResult COPY结果
type CopyObjectResult = base.CopyObjectResult

// ListObjectResult 列表结果
type ListObjectResult = base.ListObjectResult

// ListObjectPrefixes 列表前缀
type ListObjectPrefixes = base.ListObjectPrefixes

// ListObjectContents 列表内容
type ListObjectContents = base.ListObjectContents

// UploadFile 上传文件根据路径
func (c *Client) UploadFile(filePath, bucket, object string, options map[string]string) (map[string]string, error) {
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	body, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}
	if object == "" {
		object = path.Base(filePath)
	}
	if strings.TrimRight(object, "/") == path.Dir(object) {
		object = strings.TrimRight(object, "/") + "/" + path.Base(filePath)
	}
	return c.Put(body, bucket, object, map[string]string{"disposition": options["disposition"], "acl": options["acl"]})
}

// Put 上传文件根据内容
func (c *Client) Put(body []byte, bucket, object string, options map[string]string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s/%s/%s", c.host, bucket, object)
	method := "PUT"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(len(body))
	contentMd5 := c.base64(c.md5Byte(body))
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
	headers["Content-Length"] = contentLength
	if options["disposition"] != "" {
		headers["Content-Disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	res, err := c.curl(addr, method, headers, body)
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
	}
	return map[string]string{
		"X-Amz-Request-Id": res["X-Amz-Request-Id"],
		"StatusCode":       res["StatusCode"],
		"Location":         fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object),
		"Size":             contentLength,
		"Bucket":           bucket,
		"ETag":             res["Etag"],
		"Key":              object,
	}, nil
}

// Copy 复制文件
func (c *Client) Copy(bucket, object, source string, options map[string]string) (map[string]string, error) {
	//source head
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourceObject := strings.Join(tmpSourceInfo[2:], "/")
	sourceHead, err := c.Head(sourceBucket, sourceObject)
	if err != nil {
		return nil, err
	}
	addr := fmt.Sprintf("http://%s/%s/%s", c.host, bucket, object)
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
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
	}
	var CopyObject = &CopyObjectResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), CopyObject); err != nil {
		return nil, err
	}
	return map[string]string{
		"X-Amz-Request-Id": res["X-Amz-Request-Id"],
		"StatusCode":       res["StatusCode"],
		"Location":         fmt.Sprintf("http://%s.%s/%s", bucket, c.host, object),
		"Size":             sourceHead["Content-Length"],
		"Bucket":           bucket,
		"ETag":             CopyObject.ETag,
		"Key":              object,
	}, nil
}

// Delete 删除文件
func (c *Client) Delete(bucket, object string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s/%s/%s", c.host, bucket, object)
	method := "DELETE"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" && res["StatusCode"] != "204" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
	}
	return res, nil
}

// Head 查看文件信息
func (c *Client) Head(bucket, object string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s/%s/%s", c.host, bucket, object)
	method := "HEAD"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket, object)
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
	}
	return res, nil
}

// Get 下载文件到本地
func (c *Client) Get(bucket, object, localfile string, options map[string]string) (map[string]string, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	objectHead, err := c.Head(bucket, object)
	if err != nil {
		return nil, err
	}
	objectSize, err := strconv.Atoi(objectHead["Content-Length"])
	if err != nil {
		return nil, err
	}
	//当没指定文件名时，默认使用object的文件名
	if strings.TrimRight(localfile, "/") == path.Dir(localfile) {
		localfile = strings.TrimRight(localfile, "/") + "/" + path.Base(object)
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

	var total = (objectSize + partSize - 1) / partSize
	var queueMaxSize = make(chan bool, threadNum)
	var writePercent = make(chan bool)
	var writeDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-writePercent
			if !ok {
				close(writeDone)
				break
			}
			finishNum++
			if options["percent"] == "true" {
				fmt.Printf("\r%.0f%% ", float64(finishNum)/float64(total)*100)
			}
		}
	}()

	//创建local文件
	localdir := path.Dir(localfile)
	err = os.MkdirAll(localdir, 0755)
	if err != nil {
		fmt.Printf("\nCreate Dir Fail LocalDir: %s %s\n", localdir, err)
		os.Exit(0)
	}
	file, _ := os.OpenFile(localfile, os.O_CREATE|os.O_WRONLY, 0755)
	defer file.Close()

	partNum := 0
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
			isWriteSuccess := false
			for i := 0; i < c.maxRetryNum; i++ {
				tmp, err := c.Cat(bucket, object, partRange)
				if err != nil {
					continue
				}
				if tmp["StatusCode"] != "200" && tmp["StatusCode"] != "206" {
					continue
				}
				_, err = file.WriteAt([]byte(tmp["Body"]), int64(tmpStart))
				if err != nil {
					continue
				}
				isWriteSuccess = true
				break
			}
			if !isWriteSuccess {
				fmt.Printf("\nWrite Part Fail PartNum: %d\n", partNum)
				os.Exit(0)
			}
			writePercent <- true
			<-queueMaxSize
		}(partNum)
		partNum++
	}
	wg.Wait()
	close(writePercent)
	<-writeDone
	return map[string]string{"object": object, "localfile": localfile}, nil
}

// Cat 读取文件内容
func (c *Client) Cat(bucket, object string, param ...string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s/%s/%s", c.host, bucket, object)
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
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" && res["StatusCode"] != "206" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
	}
	return res, nil
}

// CopyFromHlsFile 复制hls视频文件相关文件
func (c *Client) CopyFromHlsFile(bucket, prefix, source string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	if prefix != "" {
		prefix = strings.TrimRight(prefix, "/") + "/"
	}
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourceObject := strings.Join(tmpSourceInfo[2:], "/")
	sourcePrefix := path.Dir(sourceObject)
	m3u8Content, err := c.Cat(sourceBucket, sourceObject, "")
	if err != nil {
		return nil, err
	}
	//copy ts
	objectList := c.parseM3u8Content(sourceBucket, sourcePrefix, m3u8Content["Body"])
	total := len(objectList)
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
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

	fileNum := 0
	for {
		if fileNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo map[string]string) {
			defer wg.Done()
			object := prefix + objectInfo["object"]
			isSkipped := false
			sourceHead, err := c.Head(objectInfo["sourceBucket"], objectInfo["sourceObject"])
			var sourceHeadSize int64
			if err == nil && sourceHead["StatusCode"] == "200" {
				sourceHeadSize, _ = strconv.ParseInt(sourceHead["Content-Length"], 10, 64)
				if options["replace"] != "true" {
					objectHead, err := c.Head(bucket, object)
					if err == nil && objectHead["StatusCode"] == "200" {
						objectHeadSize, _ := strconv.ParseInt(objectHead["Content-Length"], 10, 64)
						if sourceHeadSize == objectHeadSize {
							sourceTime, _ := time.Parse(c.dateTimeGMT, sourceHead["Last-Modified"])
							objectTime, _ := time.Parse(c.dateTimeGMT, objectHead["Last-Modified"])
							if sourceTime.Unix() <= objectTime.Unix() {
								isSkipped = true
								atomic.AddInt64(&tmpSkip, 1)
							}
						}
					}
				}
			}
			if !isSkipped {
				sourceObject := "/" + objectInfo["sourceBucket"] + "/" + objectInfo["sourceObject"]
				isCopySuccess := false
				for i := 0; i < c.maxRetryNum; i++ {
					res, err := c.Copy(bucket, object, sourceObject, map[string]string{"disposition": sourceHead["Content-Disposition"], "acl": options["acl"]})
					if err != nil {
						continue
					}
					isCopySuccess = true
					if res["StatusCode"] == "200" {
						atomic.AddInt64(&tmpSize, sourceHeadSize)
						atomic.AddInt64(&tmpFinish, 1)
					}
					break
				}
				if !isCopySuccess {
					fmt.Printf("\nCopy File Fail object: %s\n", sourceObject)
					os.Exit(0)
				}
			}
			copyPercent <- true
			<-queueMaxSize
		}(objectList[fileNum])
		fileNum++
	}
	wg.Wait()
	close(copyPercent)
	<-copyDone
	finish := int(atomic.LoadInt64(&tmpFinish))
	//upload m3u8
	if finish > 0 {
		newM3u8Content := c.updateM3u8Content(m3u8Content["Body"])
		newObject := path.Base(sourceObject)
		res, err := c.Put([]byte(newM3u8Content), bucket, prefix+newObject, map[string]string{"disposition": newObject, "acl": options["acl"]})
		if err != nil {
			return nil, err
		}
		if res["StatusCode"] != "200" {
			return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
		}
		m3u8Size, _ := strconv.ParseInt(res["Size"], 10, 64)
		atomic.AddInt64(&tmpSize, m3u8Size)
	}
	skip := int(atomic.LoadInt64(&tmpSkip))
	size := int(atomic.LoadInt64(&tmpSize))
	return map[string]int{"total": total, "skip": skip, "finish": finish, "size": size}, nil
}

// UploadFromDir 上传目录
func (c *Client) UploadFromDir(localdir, bucket, prefix string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	if prefix != "" {
		prefix = strings.TrimRight(prefix, "/") + "/"
	}
	suffix := ""
	if options["suffix"] != "" {
		suffix = options["suffix"]
	}
	localdir = strings.TrimRight(localdir, "/") + "/"
	fileList := c.walkDir(localdir, suffix)
	total := len(fileList)
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
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
	var filePercent = make(chan bool)
	var fileDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-filePercent
			if !ok {
				close(fileDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%% ", float64(finishNum)/float64(total)*100)
		}
	}()

	fileNum := 0
	for {
		if fileNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(fileName string) {
			defer wg.Done()
			object := prefix + fileName
			isSkipped := false
			localFileStat, err := os.Stat(localdir + fileName)
			if err != nil {
				fmt.Printf("UploadFromDir Stat Fail FileName: %s %s\n", fileName, err)
				os.Exit(0)
			}
			localFileSize := localFileStat.Size()
			localFileTime := localFileStat.ModTime()
			if options["replace"] != "true" {
				objectHead, err := c.Head(bucket, object)
				if err == nil && objectHead["StatusCode"] == "200" {
					objectHeadSize, _ := strconv.ParseInt(objectHead["Content-Length"], 10, 64)
					if localFileSize == objectHeadSize {
						objectTime, _ := time.Parse(c.dateTimeGMT, objectHead["Last-Modified"])
						if objectTime.Unix() >= localFileTime.Unix() {
							isSkipped = true
							atomic.AddInt64(&tmpSkip, 1)
						}
					}
				}
			}
			if !isSkipped {
				fd, err := os.Open(localdir + fileName)
				if err != nil {
					fmt.Printf("UploadFromDir Open Fail FileName: %s %s\n", fileName, err)
					os.Exit(0)
				}
				defer fd.Close()
				body, err := ioutil.ReadAll(fd)
				if err != nil {
					fmt.Printf("UploadFromDir ReadAll Fail FileName: %s %s\n", fileName, err)
					os.Exit(0)
				}
				isUploadSuccess := false
				for i := 0; i < c.maxRetryNum; i++ {
					res, err := c.Put(body, bucket, object, map[string]string{"disposition": fileName, "acl": options["acl"]})
					if err != nil {
						continue
					}
					isUploadSuccess = true
					if res["StatusCode"] == "200" {
						atomic.AddInt64(&tmpSize, localFileSize)
						atomic.AddInt64(&tmpFinish, 1)
					}
					break
				}
				if !isUploadSuccess {
					fmt.Printf("\nUploadFromDir Fail FileName: %s\n", fileName)
					os.Exit(0)
				}
			}
			filePercent <- true
			<-queueMaxSize
		}(fileList[fileNum])
		fileNum++
	}
	wg.Wait()
	close(filePercent)
	<-fileDone
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	size := int(atomic.LoadInt64(&tmpSize))
	return map[string]int{"total": total, "skip": skip, "finish": finish, "size": size}, nil
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
	addr := fmt.Sprintf("http://%s/%s/?%s", c.host, bucket, strings.TrimLeft(param, "&"))
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/", "")
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Amz-Request-Id:" + res["X-Amz-Request-Id"])
	}
	var listObject = &ListObjectResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), listObject); err != nil {
		return nil, err
	}
	return listObject, nil
}

// CopyAllObject 复制目录
func (c *Client) CopyAllObject(bucket, prefix, source string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	if prefix != "" {
		prefix = strings.TrimRight(prefix, "/") + "/"
	}
	marker := ""
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourcePrefix := strings.Join(tmpSourceInfo[2:], "/")
	total := 0
	var tmpSize int64
	var tmpSkip int64
	var tmpFinish int64
	var threadNum = c.threadMaxNum
	if options["thread_num"] != "" {
		n, err := strconv.Atoi(options["thread_num"])
		if err == nil && n <= c.threadMaxNum && n >= c.threadMinNum {
			threadNum = n
		}
	}
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
LIST:
	sourceList, err := c.ListObject(sourceBucket, map[string]string{"prefix": sourcePrefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	sourceObjectNum := len(sourceList.Contents)
	total += sourceObjectNum
	fileNum := 0
	for {
		if fileNum >= sourceObjectNum {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo ListObjectContents) {
			defer wg.Done()
			//支持自定义前缀
			object := prefix
			if options["fullpath"] == "true" {
				object += objectInfo.Key
			} else {
				object += path.Base(objectInfo.Key)
			}
			isSkipped := false
			sourceHead, err := c.Head(sourceBucket, objectInfo.Key)
			var sourceHeadSize int64
			if err == nil && sourceHead["StatusCode"] == "200" {
				sourceHeadSize, _ = strconv.ParseInt(sourceHead["Content-Length"], 10, 64)
				if options["replace"] != "true" {
					objectHead, err := c.Head(bucket, object)
					if err == nil && objectHead["StatusCode"] == "200" {
						objectHeadSize, _ := strconv.ParseInt(objectHead["Content-Length"], 10, 64)
						if sourceHeadSize == objectHeadSize {
							objectTime, _ := time.Parse(c.dateTimeGMT, objectHead["Last-Modified"])
							sourceTime, _ := time.Parse(c.dateTimeGMT, sourceHead["Last-Modified"])
							if objectTime.Unix() >= sourceTime.Unix() {
								isSkipped = true
								atomic.AddInt64(&tmpSkip, 1)
							}
						}
					}
				}
			}

			if !isSkipped {
				sourceObject := "/" + sourceBucket + "/" + objectInfo.Key
				isCopySuccess := false
				for i := 0; i < c.maxRetryNum; i++ {
					res, err := c.Copy(bucket, object, sourceObject, map[string]string{"disposition": sourceHead["Content-Disposition"], "acl": options["acl"]})
					if err != nil {
						continue
					}
					isCopySuccess = true
					if res["StatusCode"] == "200" {
						atomic.AddInt64(&tmpSize, sourceHeadSize)
						atomic.AddInt64(&tmpFinish, 1)
					}
					break
				}
				if !isCopySuccess {
					fmt.Printf("\nCopy File Fail object: %s\n", sourceObject)
					os.Exit(0)
				}
			}
			copyPercent <- true
			<-queueMaxSize
		}(sourceList.Contents[fileNum])
		fileNum++
	}
	wg.Wait()
	if sourceList.IsTruncated == "true" {
		marker = sourceList.Contents[sourceObjectNum-1].Key
		goto LIST
	}
	close(copyPercent)
	<-copyDone
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	size := int(atomic.LoadInt64(&tmpSize))
	return map[string]int{"total": total, "skip": skip, "finish": finish, "size": size}, nil
}

// DeleteAllObject 删除目录
func (c *Client) DeleteAllObject(bucket, prefix string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
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
	var deletePercent = make(chan bool)
	var deleteDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-deletePercent
			if !ok {
				close(deleteDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%% ", float64(finishNum)/float64(bodyNum)*100)
		}
	}()

	fileNum := 0
	for {
		if fileNum >= bodyNum {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(fileNum int, body string) {
			defer wg.Done()
			object := "?delete"
			addr := fmt.Sprintf("http://%s/%s/%s", c.host, bucket, object)
			method := "POST"
			date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
			contentLength := strconv.Itoa(len(body))
			contentMd5 := c.base64(c.md5Byte([]byte(body)))
			headers := map[string]string{
				"Content-Md5": contentMd5 + "\n",
				"Date":        date,
			}
			headers["Authorization"] = c.sign(method, headers, bucket, object)
			headers["Content-Length"] = contentLength
			headers["Content-Md5"] = strings.TrimRight(headers["Content-Md5"], "\n")
			isDeleteSuccess := false
			for i := 0; i < c.maxRetryNum; i++ {
				res, err := c.curl(addr, method, headers, []byte(body))
				if err != nil {
					continue
				}
				isDeleteSuccess = true
				if res["StatusCode"] == "200" {
					atomic.AddInt64(&tmpFinish, int64(bodyListNum[fileNum]))
				}
				break
			}
			if !isDeleteSuccess {
				fmt.Printf("\nDelete File Fail FileNum: %d\n", fileNum)
				os.Exit(0)
			}
			deletePercent <- true
			<-queueMaxSize
		}(fileNum, bodyList[fileNum])
		fileNum++
	}
	wg.Wait()
	close(deletePercent)
	<-deleteDone
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"total": total, "finish": finish}, nil
}

// DownloadAllObject 下载目录
func (c *Client) DownloadAllObject(bucket, prefix, localdir string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	marker := ""
	total := 0
	var tmpSkip int64
	var tmpFinish int64
	var threadNum = c.threadMaxNum
	if options["thread_num"] != "" {
		n, err := strconv.Atoi(options["thread_num"])
		if err == nil && n <= c.threadMaxNum && n >= c.threadMinNum {
			threadNum = n
		}
	}
	var queueMaxSize = make(chan bool, threadNum)
	var downloadPercent = make(chan bool)
	var downloadDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-downloadPercent
			if !ok {
				close(downloadDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%% ", float64(finishNum)/float64(total)*100)
		}
	}()
LIST:
	list, err := c.ListObject(bucket, map[string]string{"prefix": prefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	objectNum := len(list.Contents)
	total += objectNum
	fileNum := 0
	for {
		if fileNum >= objectNum {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo ListObjectContents) {
			defer wg.Done()
			localfile := strings.TrimRight(localdir, "/") + "/" + objectInfo.Key
			isSkipped := false
			if options["replace"] != "true" {
				objectHead, err := c.Head(bucket, objectInfo.Key)
				if err == nil && objectHead["StatusCode"] == "200" {
					objectHeadSize, _ := strconv.ParseInt(objectHead["Content-Length"], 10, 64)
					fileStat, err := os.Stat(localfile)
					if err == nil && objectHeadSize == fileStat.Size() {
						objectTime, _ := time.Parse(c.dateTimeGMT, objectHead["Last-Modified"])
						if objectTime.Unix() >= fileStat.ModTime().Unix() {
							isSkipped = true
							atomic.AddInt64(&tmpSkip, 1)
						}
					}
				}
			}
			if !isSkipped {
				isDownloadSuccess := false
				for i := 0; i < c.maxRetryNum; i++ {
					res, err := c.Get(bucket, objectInfo.Key, localfile, map[string]string{
						"percent":    "false",
						"thread_num": options["thread_num"],
						"part_size":  options["part_size"],
					})
					if err != nil {
						continue
					}
					isDownloadSuccess = true
					if res["localfile"] == localfile {
						atomic.AddInt64(&tmpFinish, 1)
					}
					break
				}
				if !isDownloadSuccess {
					fmt.Printf("\nDownload File Fail object: %s\n", objectInfo.Key)
					os.Exit(0)
				}
			}
			downloadPercent <- true
			<-queueMaxSize
		}(list.Contents[fileNum])
		fileNum++
	}
	wg.Wait()
	if list.IsTruncated == "true" {
		marker = list.Contents[objectNum-1].Key
		goto LIST
	}
	close(downloadPercent)
	<-downloadDone
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"total": total, "skip": skip, "finish": finish}, nil
}
