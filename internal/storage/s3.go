package storage

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nicolagi/muscle/internal/config"
	"github.com/nicolagi/signit"
	"github.com/pkg/errors"
)

type s3Store struct {
	client *s3.S3

	region    string
	bucket    string
	accessKey string
	secretKey string
}

var _ Store = (*s3Store)(nil)

func newS3Store(c *config.C) (Store, error) {
	const maxRetries = 16 // I have  very bad connectivity.
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(c.S3Region),
		Credentials: credentials.NewStaticCredentials(c.S3AccessKey, c.S3SecretKey, ""),
		MaxRetries:  aws.Int(maxRetries),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &s3Store{
		client:    s3.New(sess),
		region:    c.S3Region,
		bucket:    c.S3Bucket,
		accessKey: c.S3AccessKey,
		secretKey: c.S3SecretKey,
	}, nil
}

func (s *s3Store) Get(key Key) (contents Value, err error) {
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s.bucket, string(key))
	req, err := signit.NewRequest(s.accessKey, s.secretKey, s.region, "s3", "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("s3Store.Get %q: %w", key, err)
	}
	res, err := http.DefaultClient.Do(req.Sign())
	if err != nil {
		return nil, fmt.Errorf("s3Store.Get %q: %w", key, err)
	}
	body, err := ioutil.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("s3Store.Get %q: %w", key, err)
	}
	if res.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("s3Store.Get %q: %w", key, ErrNotFound)
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("s3Store.Get %q: %d status code", key, res.StatusCode)
	}
	return body, nil
}

func (s *s3Store) Put(key Key, value Value) (err error) {
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s.bucket, string(key))
	req, err := signit.NewRequest(s.accessKey, s.secretKey, s.region, "s3", "PUT", url, value)
	if err != nil {
		return fmt.Errorf("s3Store.Put %q: %w", key, err)
	}
	req.AddNextHeader("content-type", "application/octet-stream")
	res, err := http.DefaultClient.Do(req.Sign())
	if err != nil {
		return fmt.Errorf("s3Store.Put %q: %w", key, err)
	}
	_ = res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("s3Store.Put %q: %d status code", key, res.StatusCode)
	}
	return nil
}

func (s *s3Store) Delete(key Key) error {
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s.bucket, string(key))
	req, err := signit.NewRequest(s.accessKey, s.secretKey, s.region, "s3", "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("s3Store.Delete %q: %w", key, err)
	}
	res, err := http.DefaultClient.Do(req.Sign())
	if err != nil {
		return fmt.Errorf("s3Store.Delete %q: %w", key, err)
	}
	_ = res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3Store.Delete %q: %d status code", key, res.StatusCode)
	}
	return nil
}

func (s *s3Store) List() (keys chan string, err error) {
	keys = make(chan string)
	go s.list(keys)
	return keys, nil
}

func (s *s3Store) list(recv chan string) {
	input := &s3.ListObjectsInput{
		Bucket:    aws.String(s.bucket),
		Delimiter: aws.String(","),
	}
	var output *s3.ListObjectsOutput
	var err error
	for {
		output, err = s.client.ListObjects(input)
		if err != nil {
			log.Printf("warning: storage.s3Store.list: %v", err)
			// Retry indefinitely.
			time.Sleep(5 * time.Second)
			continue
		}
		for _, o := range output.Contents {
			recv <- *o.Key
		}
		if output.NextMarker == nil {
			break
		}
		input.Marker = output.NextMarker
	}
	close(recv)
}
