package s3v4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

func (c *Client) sign(method string, headers map[string]string, uri, canonQuery string) string {
	dt, _ := time.Parse(c.iso8601FormatDateTime, headers["x-amz-date"])
	credentialString := c.buildCredentialString(dt)
	signHeaders := c.canonicalSignHeaders(headers)
	signature := c.buildSignature(method, headers, uri, canonQuery, dt)
	return fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.authHeaderPrefix,
		c.accessKeyID,
		credentialString,
		signHeaders,
		signature)
}

// 得出最终的签名结果
func (c *Client) buildSignature(method string, headers map[string]string, uri, canonQuery string, dt time.Time) string {
	signKey := c.deriveSigningKey(dt)
	strToSign := c.stringToSign(method, headers, uri, canonQuery, dt)
	signature := hmacSHA256(signKey, []byte(strToSign))
	return hex.EncodeToString(signature)
}

// 创建待签名字符串
func (c *Client) stringToSign(method string, headers map[string]string, uri, canonQuery string, dt time.Time) string {
	credentialString := c.buildCredentialString(dt)
	canonicalString := c.canonicalRequest(method, headers, uri, canonQuery)
	return strings.Join([]string{
		c.authHeaderPrefix,
		headers["x-amz-date"],
		credentialString,
		hex.EncodeToString(hashSHA256([]byte(canonicalString))),
	}, "\n")
}

// 构建签名凭据
func (c *Client) buildCredentialString(dt time.Time) string {
	hostInfo := strings.Split(c.host, ".")
	credentialString := fmt.Sprintf("%s/%s/%s/%s",
		dt.Format(c.iso8601FormatDate),
		hostInfo[1],
		hostInfo[0],
		c.awsV4Request,
	)
	return credentialString
}

// 构建规范的请求字符串
func (c *Client) canonicalRequest(method string, headers map[string]string, uri, canonQuery string) string {
	signHeader := c.canonicalSignHeaders(headers)
	canonHeader := c.canonicalHeaders(headers)
	return strings.Join([]string{
		method,
		uri,
		canonQuery,
		canonHeader,
		signHeader,
		headers["x-amz-content-sha256"],
	}, "\n")
}

// 规范化header
func (c *Client) canonicalHeaders(headers map[string]string) string {
	// 获取keys并排序
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var canonHeader string
	for _, k := range keys {
		canonHeader += strings.ToLower(k) + ":" + headers[k] + "\n"
	}
	return canonHeader
}

// 这里将加入到签名的header名称拼凑起来
// 目的是为了让服务端知道是基于哪几个header进行签名
func (c *Client) canonicalSignHeaders(headers map[string]string) string {
	var keys []string
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ";")
}

// 将秘钥加入到sign中
func (c *Client) deriveSigningKey(dt time.Time) []byte {
	hostInfo := strings.Split(c.host, ".")
	kDate := hmacSHA256([]byte("AWS4"+c.accessKeySecret), []byte(dt.Format(c.iso8601FormatDate)))
	kRegion := hmacSHA256(kDate, []byte(hostInfo[1]))
	kService := hmacSHA256(kRegion, []byte(hostInfo[0]))
	signingKey := hmacSHA256(kService, []byte(c.awsV4Request))
	return signingKey
}

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func hashSHA256(data []byte) []byte {
	h := sha256.New()
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func hashSHA256Reader(body io.Reader) []byte {
	h := sha256.New()
	rs := body.(io.ReadSeeker)
	_, _ = io.Copy(h, rs)
	_, _ = rs.Seek(0, 0)
	return h.Sum(nil)
}
