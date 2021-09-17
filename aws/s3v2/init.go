package s3v2

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
		threadMaxNum: 500,
		threadMinNum: 1,
	}
}
