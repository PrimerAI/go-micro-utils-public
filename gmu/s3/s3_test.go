package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3PathJoin(t *testing.T) {
	type testCase struct {
		name   string
		input  Path
		output string
	}
	testCases := []testCase{
		{
			name:   "simple: bucket/key",
			input:  Path{Bucket: "test_bucket", Key: "test_key"},
			output: "test_bucket/test_key",
		},
		{
			name:   "long key: bucket/key/more/dirs",
			input:  Path{Bucket: "test_bucket", Key: "test_key/dir1/dir2"},
			output: "test_bucket/test_key/dir1/dir2",
		},
		{
			name:   "leading slashes: /bucket /key",
			input:  Path{Bucket: "/test_bucket", Key: "/test_key"},
			output: "test_bucket/test_key",
		},
		{
			name:   "trailing slashes: bucket/ key/",
			input:  Path{Bucket: "test_bucket/", Key: "test_key/"},
			output: "test_bucket/test_key",
		},
		{
			name:   "slashes everywhere: /bucket/ /key/",
			input:  Path{Bucket: "/test_bucket/", Key: "/test_key/"},
			output: "test_bucket/test_key",
		},
	}
	assert := assert.New(t)
	for _, tc := range testCases {
		t.Logf("Handling Path.join test: %v", tc.name)
		output := tc.input.Join()
		assert.Equal(output, tc.output)
	}
}

func TestS3PathMakeUri(t *testing.T) {
	type testCase struct {
		name   string
		input  Path
		output string
	}
	testCases := []testCase{
		{
			name:   "simple: bucket/key",
			input:  Path{Bucket: "test_bucket", Key: "test_key"},
			output: "s3://test_bucket/test_key",
		},
		{
			name:   "long key: bucket/key/more/dirs",
			input:  Path{Bucket: "test_bucket", Key: "test_key/dir1/dir2"},
			output: "s3://test_bucket/test_key/dir1/dir2",
		},
		{
			name:   "leading slashes: /bucket /key",
			input:  Path{Bucket: "/test_bucket", Key: "/test_key"},
			output: "s3://test_bucket/test_key",
		},
		{
			name:   "trailing slashes: bucket/ key/",
			input:  Path{Bucket: "test_bucket/", Key: "test_key/"},
			output: "s3://test_bucket/test_key",
		},
		{
			name:   "slashes everywhere: /bucket/ /key/",
			input:  Path{Bucket: "/test_bucket/", Key: "/test_key/"},
			output: "s3://test_bucket/test_key",
		},
		{
			name:   "no key: bucket",
			input:  Path{Bucket: "test_bucket", Key: ""},
			output: "s3://test_bucket",
		},
	}
	assert := assert.New(t)
	for _, tc := range testCases {
		t.Logf("Handling Path.makeUri test: %v", tc.name)
		output := tc.input.ToURI()
		assert.Equal(output, tc.output)
	}
}

func TestParseS3Uri(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output Path
		err    error
	}

	testCases := []testCase{
		{
			name:   "good bucket top level key",
			input:  "s3://test_bucket/test_key",
			output: Path{Bucket: "test_bucket", Key: "test_key"},
			err:    nil,
		},
		{
			name:   "good bucket, nested key",
			input:  "s3://test_bucket/test_dir/test_key",
			output: Path{Bucket: "test_bucket", Key: "test_dir/test_key"},
			err:    nil,
		},
		{
			name:   "missing key",
			input:  "s3://test_bucket",
			output: Path{Bucket: "test_bucket", Key: ""},
			err:    nil,
		},
		{
			name:   "missing key with /",
			input:  "s3://test_bucket/",
			output: Path{Bucket: "test_bucket", Key: ""},
			err:    nil,
		},
		{
			name:   "missing scheme s3://",
			input:  "test_bucket/test_key",
			output: Path{},
			err:    ParseError("test_bucket/test_key"),
		},
		{
			name:   "missing '/' in scheme",
			input:  "s3:/test_bucket/test_key",
			output: Path{},
			err:    ParseError("s3:/test_bucket/test_key"),
		},
		{
			name:   "scheme other than s3",
			input:  "https://test_bucket/test_key",
			output: Path{},
			err:    ParseError("https://test_bucket/test_key"),
		},
	}
	assert := assert.New(t)
	for _, tc := range testCases {
		t.Logf("Handling parseS3Uri test: %v", tc.name)
		output, err := FromURI(tc.input)
		assert.Equal(output, tc.output)
		assert.Equal(err, tc.err)
	}
}
