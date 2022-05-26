package s3

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/go-logr/logr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	defaultRegion string = "us-west-2"
)

// ParseError is to have an error with the apropiate location
type ParseError string

// Error is to make the error msg
func (s ParseError) Error() string {
	return fmt.Sprintf("s3 uri must be of form 's3://<bucket>', got: %s", string(s))
}

// Path is to store a bucket and key for s3 purposes
type Path struct {
	Bucket string
	Key    string
}

// Join is a helper method to ensure that there are no leading or training slashes, `/` on the
// Path's bucket and key, then joins them into a single path returning the result as a string.
func (p Path) Join() string {
	cleanBucket := strings.Trim(p.Bucket, "/")
	cleanKey := strings.Trim(p.Key, "/")
	return path.Join(cleanBucket, cleanKey)
}

// Append adds a path to an existing s3.Path
func Append(p Path, key string) Path {
	return Path{Bucket: p.Bucket, Key: path.Join(p.Key, key)}
}

// ToURI joins the bucket and key together into a single path, cleaning leading and training
// slashes `/`, as well as prepending the S3 scheme, `s3://`, returning a single string.
func (p Path) ToURI() string {
	u, _ := url.Parse("s3://")
	u.Path = p.Join()
	return u.String()
}

// FromURI accepts an s3 uri of format `s3://<bucket>(/<key>, optional)` where the key can
// have slashes, `/`. Returns the bucket and key in a struct, Path, with fields of those names,
// and an error if there was trouble parsing the passed uri.
func FromURI(uri string) (Path, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return Path{}, err
	}
	if u.Scheme != "s3" || len(u.Host) == 0 {
		return Path{}, ParseError(uri)
	}
	var key string
	if len(u.Path) <= 1 {
		key = ""
	} else {
		key = u.Path[1:]
	}
	return Path{Bucket: u.Host, Key: key}, nil
}

// Client is designed to be an Storage Interface for S3 s3Client
type Client interface {
	ListDirectories(p Path) ([]Path, error)
	List(p Path) ([]Path, error)
	Download(p Path) ([]byte, error)
	DownloadInFile(p Path, f *os.File) (int64, error)
	Copy(source Path, target Path) error
	Upload(b []byte, p Path) error
	DeleteRecursive(p Path) error
	DeleteBucket(name string, log logr.Logger) error
	DeleteObject(p Path) error
	Exists(p Path) bool
	CreateBucket(name string) error
	AddBucketTag(bucketName string, tags map[string]string, overwrite bool) error
	BlockBucketPublicAccess(name string) error
	BucketServerSideEncryption(bucket string) error
}

// s3Client is for using the s3 client to access the resources
type s3Client struct {
	client  *s3.S3
	session *session.Session
}

// ClientOption is desing to allow user overwrites to the aws config
type ClientOption func(*aws.Config)

// WithCredentials allows overwriting the credentials / useful for minio server
// see: https://docs.min.io/docs/how-to-use-aws-sdk-for-go-with-minio-server.html
func WithCredentials(cred *credentials.Credentials) ClientOption {
	return func(c *aws.Config) {
		c.Credentials = cred
	}
}

// WithDisableSSL disables SSL when set to true
func WithDisableSSL(ssl bool) ClientOption {
	return func(c *aws.Config) {
		c.DisableSSL = aws.Bool(ssl)
	}
}

// WithRegion allows to set the aws region
func WithRegion(region string) ClientOption {
	return func(c *aws.Config) {
		c.Region = aws.String(region)
	}
}

// WithEndpoint allows to overwrite the aws endpoint / useful for minio server
func WithEndpoint(endpoint string) ClientOption {
	return func(c *aws.Config) {
		c.Endpoint = aws.String(endpoint)
	}
}

// WithS3ForcePathStyle allows to set S3ForcePathStyle
func WithS3ForcePathStyle(force bool) ClientOption {
	return func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(force)
	}
}

// NewClient initializes the s3Client struct with a new session
func NewClient(options ...ClientOption) (Client, error) {
	config := &aws.Config{
		Region: aws.String(defaultRegion),
	}
	for _, option := range options {
		option(config)
	}
	session, err := session.NewSession(config)
	if err != nil {
		return &s3Client{}, err
	}
	s3 := s3Client{client: s3.New(session), session: session}
	return &s3, nil
}

func (s3C s3Client) listBase(p Path) (*s3.ListObjectsV2Output, error) {
	awsKey := aws.String(p.Key + "/")
	if p.Key == "" {
		awsKey = aws.String(p.Key)
	}

	resp, err := s3C.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(p.Bucket),
		Prefix:    awsKey,
		Delimiter: aws.String("/"),
	})

	return resp, err
}

// ListDirectories gets the subdirectories of this path
// s3 doesn't actually have directories, it's just a key-value store that allows keys to have
// `/`s in their name. These slashes can be used as "directory" delimiters. In addition the s3
// web ui displays "directories" within a bucket, by treating slashes as directory delimiters
// and only displaying the "common prefixes" (read: directory names) in the namespace currently
// being displayed. The idea of common prefixes is leveraged in this function, ListDirectories.
func (s3C s3Client) ListDirectories(p Path) ([]Path, error) {
	resp, err := s3C.listBase(p)
	if err != nil {
		return []Path{}, err
	}

	s3Paths := make([]Path, len(resp.CommonPrefixes))
	for i, cp := range resp.CommonPrefixes {
		s3Paths[i] = Path{Bucket: p.Bucket, Key: *cp.Prefix}
	}
	return s3Paths, nil
}

// List returns a list of Paths under the passed Path
func (s3C s3Client) List(p Path) ([]Path, error) {
	resp, err := s3C.listBase(p)
	if err != nil {
		return []Path{}, err
	}

	s3Paths := make([]Path, len(resp.Contents))
	for i, content := range resp.Contents {
		s3Paths[i] = Path{Bucket: p.Bucket, Key: *content.Key}
	}
	return s3Paths, nil
}

// Download gets bytes from S3 Path
func (s3C s3Client) Download(p Path) ([]byte, error) {
	resp, err := s3C.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.Key),
	})
	if err != nil {
		return []byte{}, err
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return bytes, nil
}

// DownloadInFile download s3 path object to provided file
func (s3C s3Client) DownloadInFile(p Path, f *os.File) (int64, error) {
	downloader := s3manager.NewDownloader(s3C.session)

	return downloader.Download(f,
		&s3.GetObjectInput{
			Bucket: aws.String(p.Bucket),
			Key:    aws.String(p.Key),
		})
}

// Copy copies resources from source Path to target Path
func (s3C s3Client) Copy(source Path, target Path) error {
	_, err := s3C.client.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(target.Bucket),
		Key:        aws.String(target.Key),
		CopySource: aws.String(source.Join()),
	})
	return err
}

// Upload writes byte array to S3 Path location
func (s3C s3Client) Upload(b []byte, p Path) error {
	_, err := s3C.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.Key),
		Body:   bytes.NewReader(b),
	})
	return err
}

// DeleteRecursive removes resources of S3 Bucket recursively
func (s3C s3Client) DeleteRecursive(p Path) error {
	// Code following https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_delete_objects.go

	// Setup BatchDeleteIterator to iterate through a list of objects.
	iter := s3manager.NewDeleteListIterator(s3C.client, &s3.ListObjectsInput{
		Bucket:    aws.String(p.Bucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(p.Key + "/"),
	})

	// Traverse iterator deleting each object
	return s3manager.NewBatchDeleteWithClient(s3C.client).Delete(aws.BackgroundContext(), iter)
}

// DeleteBucket removes the S3 Bucket
func (s3C s3Client) DeleteBucket(name string, log logr.Logger) error {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	}

	// Setup BatchDeleteIterator to iterate through a list of objects.
	iter := s3manager.NewDeleteListIterator(s3C.client, &s3.ListObjectsInput{
		Bucket: aws.String(name),
	})

	// Traverse iterator deleting each object
	if err := s3manager.NewBatchDeleteWithClient(s3C.client).Delete(aws.BackgroundContext(), iter); err != nil {
		log.Error(err, "unable to remove objects from bucket for deletion", "bucket", name)
		return err
	}

	log.Info("removed all object(s) from bucket for deletion", "bucket", name)

	_, err := s3C.client.DeleteBucket(input)
	return err
}

// DeleteObject removes a single S3 object
func (s3C s3Client) DeleteObject(p Path) error {
	_, err := s3C.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.Key),
	})
	return err
}

// Exists returns True if the object metatdata exists false otherwise
func (s3C s3Client) Exists(p Path) bool {
	_, err := s3C.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.Key),
	})

	return err == nil
}

// CreateBucket create a new bucket. This will raise an BucketAlreadyExitsError if the bucket is owned by someone else
func (s3C s3Client) CreateBucket(name string) error {
	_, err := s3C.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// BlockBucketPublicAccess blocks public access for a named bucket
func (s3C s3Client) BlockBucketPublicAccess(name string) error {
	_, err := s3C.client.PutPublicAccessBlock(&s3.PutPublicAccessBlockInput{
		Bucket: aws.String(name),
		PublicAccessBlockConfiguration: &s3.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(true),
			BlockPublicPolicy:     aws.Bool(true),
			IgnorePublicAcls:      aws.Bool(true),
			RestrictPublicBuckets: aws.Bool(true),
		},
	})
	if err != nil {
		return err
	}
	return nil
}

// AddBucketTag adds a tag to the bucket metadata
// tags accept any number of tags in a map format
// overwrite allows you to overwrite previous tags
func (s3C s3Client) AddBucketTag(bucketName string, tags map[string]string, overwrite bool) error {
	if getTagOutput, err := s3C.client.GetBucketTagging(
		&s3.GetBucketTaggingInput{
			Bucket: aws.String(bucketName),
		},
	); err != nil {
		// Handling special error code
		// * Error code: NoSuchTagSetError Description: There is no tag set associated with the bucket.
		if awsErr, ok := err.(awserr.Error); ok {
			errCode := awsErr.Code()
			switch errCode {
			case "NoSuchTagSet":
				break
			default:
				return awsErr
			}
		} else {
			return err
		}
	} else {
		for _, tag := range getTagOutput.TagSet {
			// Skip tags present in the list
			if tag.Key == nil || tag.Value == nil {
				// Defensive measure, prevent nil tags
				continue
			}
			if _, ok := tags[*tag.Key]; ok && overwrite {
				// overwrite
				continue
			}
			tags[*tag.Key] = *tag.Value
		}
	}

	tagSet := []*s3.Tag{}
	for key, value := range tags {
		tagSet = append(tagSet, &s3.Tag{
			Key:   aws.String(key),
			Value: aws.String(value),
		})
	}

	_, err := s3C.client.PutBucketTagging(
		&s3.PutBucketTaggingInput{
			Bucket: aws.String(bucketName),
			Tagging: &s3.Tagging{
				TagSet: tagSet,
			},
		})
	return err
}

// EncryptServerSideEncryption this will be used to encrypt using default awskms key
func (s3C s3Client) BucketServerSideEncryption(bucket string) error {
	bucketKeyEnabled := true
	// Encrypt with KMS by default
	defEnc := &s3.ServerSideEncryptionByDefault{SSEAlgorithm: aws.String(s3.ServerSideEncryptionAwsKms)}
	rule := &s3.ServerSideEncryptionRule{
		ApplyServerSideEncryptionByDefault: defEnc,
		BucketKeyEnabled:                   &bucketKeyEnabled,
	}
	rules := []*s3.ServerSideEncryptionRule{rule}
	serverConfig := &s3.ServerSideEncryptionConfiguration{Rules: rules}
	input := &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: serverConfig}

	_, err := s3C.client.PutBucketEncryption(input)
	if err != nil {
		return err
	}
	return err
}
