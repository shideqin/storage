package oss

import (
	"encoding/xml"
	"errors"
	"fmt"
	"storage/base"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ServiceResult 获取bucket列表结果
type ServiceResult = base.ServiceResult

// ListPartsResult 获取分块列表结果
type ListPartsResult = base.ListPartsResult

// GetService 获取bucket列表
func (c *Client) GetService() (*ServiceResult, error) {
	addr := fmt.Sprintf("http://%s/", c.host)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, "", "")
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Oss-Request-Id:" + res["X-Oss-Request-Id"])
	}
	var service = &ServiceResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), service); err != nil {
		return nil, err
	}
	return service, nil
}

// CreateBucket 创建bucket
func (c *Client) CreateBucket(bucket string, options map[string]string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s.%s/", bucket, c.host)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	if options["acl"] != "" {
		headers["x-oss-acl"] = options["acl"]
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/", "")
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Oss-Request-Id:" + res["X-Oss-Request-Id"])
	}
	return map[string]string{
		"X-Oss-Request-Id": res["X-Oss-Request-Id"],
		"StatusCode":       res["StatusCode"],
	}, nil
}

// DeleteBucket 删除bucket
func (c *Client) DeleteBucket(bucket string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s.%s/", bucket, c.host)
	method := "DELETE"
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
	if res["StatusCode"] != "204" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Oss-Request-Id:" + res["X-Oss-Request-Id"])
	}
	return map[string]string{
		"X-Oss-Request-Id": res["X-Oss-Request-Id"],
		"StatusCode":       res["StatusCode"],
	}, nil
}

// ListPart 查看分块列表
func (c *Client) ListPart(bucket string, options map[string]string) (*ListPartsResult, error) {
	param := ""
	if options["delimiter"] != "" {
		param += "&delimiter=" + options["delimiter"]
	}
	if options["key-marker"] != "" {
		param += "&key-marker=" + options["key-marker"]
	}
	if options["max-keys"] != "" {
		param += "&max-keys=" + options["max-keys"]
	}
	if options["prefix"] != "" {
		param += "&prefix=" + options["prefix"]
	}
	subobject := "?uploads"
	addr := fmt.Sprintf("http://%s/%s%s", c.host, bucket+subobject, param)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+subobject, "")
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Oss-Request-Id:" + res["X-Oss-Request-Id"])
	}
	var ListParts = &ListPartsResult{}
	if err := xml.Unmarshal([]byte(res["Body"]), ListParts); err != nil {
		return nil, err
	}
	return ListParts, nil
}

// DeleteAllPart 删除所有分块
func (c *Client) DeleteAllPart(bucket, prefix string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	bodyList := make([]map[string]string, 0)
	marker := ""
	total := 0
	var tmpFinish int64
	var tmpSkip int64
LIST:
	list, err := c.ListPart(bucket, map[string]string{"prefix": prefix, "key-marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	total += len(list.Upload)
	if total <= 0 {
		return map[string]int{"total": 0, "finish": 0}, nil
	}
	expired, _ := strconv.Atoi(options["expired"])
	for _, v := range list.Upload {
		lastModified, err := time.Parse("2006-01-02T15:04:05.000Z", v.Initiated)
		if err == nil && time.Since(lastModified).Seconds() < float64(expired) {
			atomic.AddInt64(&tmpSkip, 1)
			continue
		}
		bodyList = append(bodyList, map[string]string{"Bucket": bucket, "Key": v.Key, "UploadID": v.UploadID})
	}
	if list.IsTruncated == "true" {
		marker = list.NextKeyMarker
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

	partNum := 0
	for {
		if partNum >= bodyNum {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int, body map[string]string) {
			defer wg.Done()

			isDeleteSuccess := false
			for i := 0; i < c.maxRetryNum; i++ {
				res, err := c.CancelPart(body["Bucket"], body["Key"], body["UploadID"])
				if err != nil {
					continue
				}
				isDeleteSuccess = true
				if res["StatusCode"] == "204" {
					atomic.AddInt64(&tmpFinish, 1)
				}
				break
			}
			if !isDeleteSuccess {
				fmt.Printf("\nDelete Part Fail partNum: %d\n", partNum)
				os.Exit(0)
			}
			deletePercent <- true
			<-queueMaxSize
		}(partNum, bodyList[partNum])
		partNum++
	}
	wg.Wait()
	close(deletePercent)
	<-deleteDone
	finish := int(atomic.LoadInt64(&tmpFinish))
	skip := int(atomic.LoadInt64(&tmpSkip))
	return map[string]int{"total": total, "finish": finish, "skip": skip}, nil
}

// GetACL 获取bucket acl
func (c *Client) GetACL(bucket string) (map[string]string, error) {
	subobject := "?acl"
	addr := fmt.Sprintf("http://%s/%s", c.host, bucket+subobject)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+subobject, "")
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Oss-Request-Id:" + res["X-Oss-Request-Id"])
	}
	return map[string]string{
		"X-Oss-Request-Id": res["X-Oss-Request-Id"],
		"StatusCode":       res["StatusCode"],
	}, nil
}

// SetACL 设置bucket acl
func (c *Client) SetACL(bucket string, options map[string]string) (map[string]string, error) {
	subobject := "?acl"
	addr := fmt.Sprintf("http://%s/%s", c.host, bucket+subobject)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	if options["acl"] != "" {
		headers["x-oss-acl"] = options["acl"]
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+subobject, "")
	res, err := c.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"] + " X-Oss-Request-Id:" + res["X-Oss-Request-Id"])
	}
	return map[string]string{
		"X-Oss-Request-Id": res["X-Oss-Request-Id"],
		"StatusCode":       res["StatusCode"],
	}, nil
}
