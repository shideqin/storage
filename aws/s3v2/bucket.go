package s3v2

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shideqin/storage/storagebase"
	"github.com/shideqin/storage/storageutil"
)

// ServiceResult 获取bucket列表结果
type ServiceResult = storagebase.ServiceResult

// AclResult 获取bucket Acl列表结果
type AclResult = storagebase.AclResult

// ListPartsResult 获取分块列表结果
type ListPartsResult = storagebase.ListPartsResult

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
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" GetService Error: %v", err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" GetService StatusCode: %d X-Amz-Request-Id: %s", status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" GetService Error: respond body is nil")
	}
	var service = &ServiceResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), service); err != nil {
		return nil, fmt.Errorf(" GetService Error: %v", err)
	}
	return service, nil
}

// CreateBucket 创建bucket
func (c *Client) CreateBucket(bucket string, options map[string]string) (map[string]interface{}, error) {
	addr := fmt.Sprintf("http://%s.%s/", bucket, c.host)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	if options["acl"] != "" {
		headers["x-amz-acl"] = options["acl"]
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/", "")
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" CreateBucket Bucket: %s Error: %v", bucket, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" CreateBucket Bucket: %s StatusCode: %d X-Amz-Request-Id: %s", bucket, status, reqID)
	}
	return map[string]interface{}{
		"X-Amz-Request-Id": reqID,
		"StatusCode":       status,
	}, nil
}

// DeleteBucket 删除bucket
func (c *Client) DeleteBucket(bucket string) (map[string]interface{}, error) {
	addr := fmt.Sprintf("http://%s.%s/", bucket, c.host)
	method := "DELETE"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/", "")
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" DeleteBucket Bucket: %s Error: %v", bucket, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 204 {
		return nil, fmt.Errorf(" DeleteBucket Bucket: %s StatusCode: %d X-Amz-Request-Id: %s", bucket, status, reqID)
	}
	return map[string]interface{}{
		"X-Amz-Request-Id": reqID,
		"StatusCode":       status,
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
	subObject := "/?uploads"
	addr := fmt.Sprintf("http://%s.%s%s%s", bucket, c.host, subObject, param)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+subObject, "")
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" ListPart Bucket: %s Error: %v", bucket, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" ListPart Bucket: %s StatusCode: %d X-Amz-Request-Id: %s", bucket, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" ListPart Bucket: %s Error: respond body is nil", bucket)
	}
	var ListParts = &ListPartsResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), ListParts); err != nil {
		return nil, fmt.Errorf(" ListPart Bucket: %s Error: %v", bucket, err)
	}
	return ListParts, nil
}

// DeleteAllPart 删除所有分块
func (c *Client) DeleteAllPart(bucket, prefix string, options map[string]string, percentChan chan int) (map[string]int, error) {
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
		return map[string]int{"Total": 0, "Finish": 0}, nil
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
	defer close(queueMaxSize)
	var partErr error
	var partExit bool
	var wg sync.WaitGroup
	for partNum := 0; partNum < bodyNum; partNum++ {
		if partExit {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int, body map[string]string) {
			defer func() {
				if partErr != nil {
					partExit = true
				}
				wg.Done()
				<-queueMaxSize
			}()
			for i := 0; i < c.maxRetryNum; i++ {
				_, partErr = c.CancelPart(body["Bucket"], body["Key"], body["UploadID"])
				if partErr != nil {
					continue
				}
				partErr = nil
				break
			}
			if partErr != nil {
				return
			}
			atomic.AddInt64(&tmpFinish, 1)
			percentChan <- total
		}(partNum, bodyList[partNum])
	}
	wg.Wait()
	if partErr != nil {
		return nil, partErr
	}
	finish := int(atomic.LoadInt64(&tmpFinish))
	skip := int(atomic.LoadInt64(&tmpSkip))
	return map[string]int{"Total": total, "Finish": finish, "Skip": skip}, nil
}

// GetACL 获取bucket acl
func (c *Client) GetACL(bucket string) (*AclResult, error) {
	subObject := "?acl"
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, subObject)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/"+subObject, "")
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" GetACL Bucket: %s Error: %v", bucket, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" GetACL Bucket: %s StatusCode: %d X-Amz-Request-Id: %s", bucket, status, reqID)
	}
	if _, ok := resp["Body"]; !ok {
		return nil, fmt.Errorf(" GetACL Bucket: %s Error: respond body is nil", bucket)
	}
	var acl = &AclResult{}
	if err := xml.Unmarshal(resp["Body"].(*bytes.Buffer).Bytes(), acl); err != nil {
		return nil, fmt.Errorf(" GetACL Bucket: %s Error: %v", bucket, err)
	}
	return acl, nil
}

// SetACL 设置bucket acl
func (c *Client) SetACL(bucket string, options map[string]string) (map[string]interface{}, error) {
	subObject := "?acl"
	addr := fmt.Sprintf("http://%s.%s/%s", bucket, c.host, subObject)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(c.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	if options["acl"] != "" {
		headers["x-amz-acl"] = options["acl"]
	}
	LF := "\n"
	headers["Authorization"] = c.sign(method+LF+LF, headers, bucket+"/"+subObject, "")
	resp, err := storageutil.CURL(addr, method, headers, bytes.NewBufferString(""))
	if err != nil {
		return nil, fmt.Errorf(" SetACL Bucket: %s Error: %v", bucket, err)
	}
	status := resp["StatusCode"].(int)
	reqID := resp["X-Amz-Request-Id"].(string)
	if status != 200 {
		return nil, fmt.Errorf(" SetACL Bucket: %s StatusCode: %d X-Amz-Request-Id: %s", bucket, status, reqID)
	}
	return map[string]interface{}{
		"X-Amz-Request-Id": reqID,
		"StatusCode":       status,
	}, nil
}
