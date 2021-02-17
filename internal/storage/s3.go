package storage

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nicolagi/muscle/internal/config"
	"github.com/pkg/errors"
)

type s3Store struct {
	client *s3.S3
	bucket string
}

var _ Store = (*s3Store)(nil)

func newS3Store(c *config.C) (Store, error) {
	const maxRetries = 16 // I have  very bad connectivity.
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(c.S3Region),
		Credentials: credentials.NewSharedCredentials("", c.S3Profile),
		MaxRetries:  aws.Int(maxRetries),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &s3Store{
		client: s3.New(sess),
		bucket: c.S3Bucket,
	}, nil
}

func (s *s3Store) Get(key Key) (contents Value, err error) {
	output, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(string(key)),
	})
	if err != nil {
		if rfErr, ok := err.(awserr.RequestFailure); ok {
			if rfErr.StatusCode() == http.StatusNotFound {
				return nil, errors.Wrapf(ErrNotFound, "key=%q err=%+v", key, err)
			}
		}
		return nil, err
	}
	defer func() {
		if err := output.Body.Close(); err != nil {
			log.Printf("warning: storage.s3Store.Get: could not close response body: %v", err)
		}
	}()
	return ioutil.ReadAll(output.Body)
}

func (s *s3Store) Put(key Key, value Value) (err error) {
	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(string(key)),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *s3Store) Delete(key Key) error {
	if _, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(string(key)),
	}); err != nil {
		return errors.WithStack(err)
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
