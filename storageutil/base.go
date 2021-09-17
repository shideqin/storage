package storageutil

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//http client
var client *http.Client

func init() {
	//http client
	var connectTimeout = 30 * time.Second
	var headerTimeout = 60 * time.Second
	var keepAlive = 60 * time.Second
	client = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				//connectTimeout
				Timeout: connectTimeout,
				//keepAlive
				KeepAlive: keepAlive,
			}).DialContext,
			MaxIdleConnsPerHost: 200,
			//keepAlive
			IdleConnTimeout: keepAlive,
			//headerTimeout
			ResponseHeaderTimeout: headerTimeout,
		},
	}
}

func CURL2Reader(addr, method string, headers map[string]string, body io.Reader, exitChan <-chan bool) (map[string]interface{}, error) {
	//readTimeout
	var readTimeout = 300 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	go func() {
		for {
			exit, ok := <-exitChan
			if !ok {
				return
			}
			if exit {
				cancel()
			}
		}
	}()
	req, err := http.NewRequest(method, addr, body)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		if req.Header.Get(k) != "" {
			req.Header.Set(k, v)
		} else {
			req.Header.Add(k, v)
		}
	}
	cl, _ := strconv.ParseInt(req.Header.Get("Content-Length"), 10, 64)
	if cl > 0 {
		req.ContentLength = cl
	}
	resp, err := client.Do(req.WithContext(ctx))
	if resp != nil {
		defer func() {
			_, _ = io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	if err != nil {
		return nil, err
	}
	buffer := &bytes.Buffer{}
	_, err = io.Copy(buffer, resp.Body)
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{
		"StatusCode": resp.StatusCode,
		"Body":       buffer,
	}
	for k, v := range resp.Header {
		result[k] = v[0]
	}
	return result, nil
}

func CURL(addr, method string, headers map[string]string, body io.Reader) (map[string]interface{}, error) {
	var exitChan = make(chan bool)
	defer close(exitChan)
	return CURL2Reader(addr, method, headers, body, exitChan)
}

//Header http header请求
func Header(addr, method string, headers map[string]string) (map[string]interface{}, error) {
	req, err := http.NewRequest(method, addr, bytes.NewBufferString(""))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		if req.Header.Get(k) != "" {
			req.Header.Set(k, v)
		} else {
			req.Header.Add(k, v)
		}
	}
	resp, err := client.Do(req)
	if resp != nil {
		defer func() {
			resp.Body.Close()
		}()
	}
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	result := map[string]interface{}{
		"StatusCode": resp.StatusCode,
	}
	for k, v := range resp.Header {
		result[k] = v[0]
	}
	return result, nil
}

func Base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func Md5Byte(data []byte) []byte {
	m := md5.Sum(data)
	return m[:]
}

func Md5ByteReader(body io.Reader) []byte {
	rs := body.(io.ReadSeeker)
	m := md5.New()
	_, _ = io.Copy(m, rs)
	_, _ = rs.Seek(0, 0)
	return m.Sum(nil)
}

func WalkDir(localDir, suffix string) []string {
	var list = make([]string, 0)
	localDir = strings.TrimSuffix(localDir, "/") + "/"
	localDir = filepath.Dir(localDir)
	localDir = strings.Replace(localDir, "\\", "/", -1)
	_ = filepath.Walk(localDir, func(fileName string, fi os.FileInfo, err error) error {
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
		fileName = strings.Replace(fileName, localDir, "", 1)
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
