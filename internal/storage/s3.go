package storage

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/nicolagi/muscle/internal/config"
	"github.com/nicolagi/signit"
)

type s3Store struct {
	region    string
	bucket    string
	accessKey string
	secretKey string
}

var _ Store = (*s3Store)(nil)

func newS3Store(c *config.C) (Store, error) {
	return &s3Store{
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
