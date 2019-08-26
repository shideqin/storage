package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Client 客户端结构
type Client struct {
	host            string
	accessKeyID     string
	accessKeySecret string

	dateTimeGMT string
	dateTimeCST string

	partMaxSize  int
	partMinSize  int
	maxRetryNum  int
	threadMaxNum int
	threadMinNum int
}

// New 实例化
func New(host, accessKeyID, accessKeySecret string) *Client {
	return &Client{
		host:            host,
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,

		dateTimeGMT: "Mon, 02 Jan 2006 15:04:05 GMT",
		dateTimeCST: "2006-01-02 15:04:05.00000 +0800 CST",

		partMaxSize:  100 * 1024 * 1024,
		partMinSize:  1 * 1024 * 1024,
		maxRetryNum:  5,
		threadMaxNum: 100,
		threadMinNum: 1,
	}
}

func (c *Client) curl2Reader(addr string, method string, headers map[string]string, body io.Reader) (map[string]string, error) {
	// new Transport
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest(method, addr, body)
	for k, v := range headers {
		if req.Header.Get(k) != "" {
			req.Header.Set(k, v)
		} else {
			req.Header.Add(k, v)
		}
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	str, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := map[string]string{
		"StatusCode": strconv.Itoa(res.StatusCode),
		"Body":       fmt.Sprintf("%s", str),
	}
	for k, v := range res.Header {
		result[k] = v[0]
	}
	return result, nil
}

func (c *Client) curl(addr string, method string, headers map[string]string, body []byte) (map[string]string, error) {
	return c.curl2Reader(addr, method, headers, bytes.NewReader(body))
}

func (c *Client) sign(method string, headers map[string]string, bucket, object string) string {
	var keyList []string
	LF := "\n"
	sign := method + LF
	for key := range headers {
		keyList = append(keyList, key)
	}
	sort.Strings(keyList)
	for _, key := range keyList {
		if strings.Contains(key, "x-amz-") {
			sign += key + ":" + headers[key] + LF
		} else {
			sign += headers[key] + LF
		}
	}
	sign += "/"
	if bucket != "" {
		sign += bucket
	}
	if object != "" {
		sign += "/" + object
	}
	return "AWS " + c.accessKeyID + ":" + c.base64([]byte(c.hmac(sign, c.accessKeySecret)))
}

func (c *Client) hmac(sign string, key string) string {
	h := hmac.New(sha1.New, []byte(key))
	_, _ = h.Write([]byte(sign))
	return string(h.Sum(nil))
}

func (c *Client) base64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func (c *Client) md5Byte(data []byte) []byte {
	sum := md5.Sum(data)
	return sum[:]
}

func (c *Client) walkDir(localdir string, suffix string) []string {
	var list = make([]string, 0)
	localdir = strings.TrimRight(localdir, "/") + "/"
	localdir = filepath.Dir(localdir)
	localdir = strings.Replace(localdir, "\\", "/", -1)
	_ = filepath.Walk(localdir, func(fileName string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if fi == nil {
			return nil
		}
		if fi.IsDir() {
			return nil
		}
		fileName = strings.Replace(fileName, "\\", "/", -1)
		fileName = strings.Replace(fileName, localdir, "", 1)
		fileName = strings.TrimLeft(fileName, "/")

		allowed := true
		if suffix != "" {
			suffixList := strings.Split(suffix, ",")
			for _, tmpSuffix := range suffixList {
				if tmpSuffix != "" {
					allowed = false
					if strings.HasSuffix(strings.ToLower(fileName), tmpSuffix) {
						allowed = true
						break
					}
				}
			}
		}
		if allowed {
			list = append(list, fileName)
		}
		return nil
	})
	return list
}

func (c *Client) parseM3u8Content(bucket, prefix, m3u8Content string) []map[string]string {
	regFile := regexp.MustCompile(`.*([\w-_]+.ts)`)
	regPath := regexp.MustCompile(`([\w.]+)/([\w-_]+.ts)`)
	regIsURL := regexp.MustCompile(`(http(s)?:)?//.*([\w-_]+.ts)`)
	regIsUpload := regexp.MustCompile(`/u/[0-9]+/`)
	var list []map[string]string
	var publicBucket = map[string]bool{"live-vod": true, "long-vod": true}
	match := regFile.FindAllString(m3u8Content, -1)
	for _, line := range match {
		row := map[string]string{}
		//是否URL绝对路径
		if regIsURL.MatchString(line) {
			v := regPath.FindStringSubmatch(line)
			tmpURL, _ := url.Parse(line)
			row["sourceBucket"] = "live-vod"
			row["object"] = v[2]
			row["sourceObject"] = tmpURL.Path[1:]
			if regIsUpload.MatchString(line) {
				row["sourceBucket"] = "long-vod"
				row["object"] = fmt.Sprintf("%s-%s", v[1], v[2])
			}
			if _, ok := publicBucket[bucket]; !ok {
				row["sourceBucket"] = bucket
			}
		} else {
			row["sourceBucket"] = bucket
			row["object"] = line
			row["sourceObject"] = fmt.Sprintf("%s/%s", prefix, line)
		}
		list = append(list, row)
	}
	return list
}

func (c *Client) updateM3u8Content(m3u8Content string) string {
	regURL := regexp.MustCompile(`(http(s)?:)?//.*([\w-_]+.ts)`)
	regPath := regexp.MustCompile(`([\w.]+)/([\w-_]+.ts)`)
	regIsUpload := regexp.MustCompile(`/u/[0-9]+/`)
	res := regURL.ReplaceAllStringFunc(m3u8Content,
		func(line string) string {
			v := regPath.FindStringSubmatch(line)
			if regIsUpload.MatchString(line) {
				return fmt.Sprintf("%s-%s", v[1], v[2])
			}
			return v[2]
		})
	return res
}
