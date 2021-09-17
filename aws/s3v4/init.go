package s3v4

// Client 客户端结构
type Client struct {
	host            string
	accessKeyID     string
	accessKeySecret string

	dateTimeGMT           string
	iso8601FormatDateTime string
	iso8601FormatDate     string
	authHeaderPrefix      string
	awsV4Request          string
	emptyStringSHA256     string

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

		iso8601FormatDateTime: "20060102T150405Z",
		iso8601FormatDate:     "20060102",
		authHeaderPrefix:      "AWS4-HMAC-SHA256",
		awsV4Request:          "aws4_request",
		emptyStringSHA256:     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",

		partMaxSize:  100 * 1024 * 1024,
		partMinSize:  1 * 1024 * 1024,
		maxRetryNum:  5,
		threadMaxNum: 500,
		threadMinNum: 1,
	}
}
