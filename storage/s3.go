package storage // import "github.com/nicolagi/muscle/storage"

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nicolagi/muscle/config"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ Store = (*s3Store)(nil)

type s3Store struct {
	profile string
	region  string
	bucket  string
	client  *s3.S3
}

func newS3Store(c *config.C) Store {
	return &s3Store{
		profile: c.S3Profile,
		region:  c.S3Region,
		bucket:  c.S3Bucket,
	}
}

func (s *s3Store) Get(key Key) (contents Value, err error) {
	if err := s.ensureClient(); err != nil {
		return nil, err
	}
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
			log.WithFields(log.Fields{
				"op":  "get",
				"key": key,
			}).Warning("Could not close response body")
		}
	}()
	return ioutil.ReadAll(output.Body)
}

func (s *s3Store) Put(key Key, value Value) (err error) {
	err = s.ensureClient()
	if err == nil {
		_, err = s.client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(string(key)),
			Body:   bytes.NewReader(value),
		})
	}
	return
}

func (s *s3Store) Delete(key Key) error {
	if err := s.ensureClient(); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(string(key)),
	})
	return err
}

func (s *s3Store) List() (keys chan string, err error) {
	if err := s.ensureClient(); err != nil {
		return nil, err
	}
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
			log.WithField("cause", err.Error()).Error("Could not list")
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

func (s *s3Store) ensureClient() error {
	if s.client != nil {
		return nil
	}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.region),
		Credentials: credentials.NewSharedCredentials("", s.profile),
	})
	if err != nil {
		return err
	}
	client := s3.New(sess)
	s.client = client
	return nil
}
