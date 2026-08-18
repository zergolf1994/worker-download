package uploader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeMultipartClient struct {
	mu                                 sync.Mutex
	active, maxActive                  int
	parts                              map[int32][]byte
	completed                          []int32
	aborted, completeHit, failComplete bool
	failPart                           int32
}

func (f *fakeMultipartClient) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-1")}, nil
}
func (f *fakeMultipartClient) UploadPart(ctx context.Context, input *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	n := aws.ToInt32(input.PartNumber)
	f.mu.Lock()
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if n == f.failPart {
		return nil, errors.New("forced upload failure")
	}
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.parts == nil {
		f.parts = map[int32][]byte{}
	}
	f.parts[n] = body
	f.mu.Unlock()
	return &s3.UploadPartOutput{ETag: aws.String(fmt.Sprintf("etag-%d", n))}, nil
}
func (f *fakeMultipartClient) CompleteMultipartUpload(_ context.Context, input *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeHit = true
	if f.failComplete {
		return nil, errors.New("forced complete failure")
	}
	for _, part := range input.MultipartUpload.Parts {
		f.completed = append(f.completed, aws.ToInt32(part.PartNumber))
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (f *fakeMultipartClient) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	f.aborted = true
	f.mu.Unlock()
	return &s3.AbortMultipartUploadOutput{}, nil
}
func fixture(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "upload.bin")
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMultipartConcurrentOrdered(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	client := &fakeMultipartClient{}
	var uploaded int64
	if err := uploadMultipart(context.Background(), client, "bucket", "key", fixture(t, data), int64(len(data)), "application/octet-stream", 10, 3, func(done, _ int64) { uploaded = done }); err != nil {
		t.Fatal(err)
	}
	if client.maxActive < 2 {
		t.Fatalf("expected concurrency, max active=%d", client.maxActive)
	}
	if uploaded != int64(len(data)) {
		t.Fatalf("uploaded=%d want=%d", uploaded, len(data))
	}
	if fmt.Sprint(client.completed) != fmt.Sprint([]int32{1, 2, 3}) {
		t.Fatalf("parts=%v", client.completed)
	}
	combined := append(append([]byte{}, client.parts[1]...), client.parts[2]...)
	combined = append(combined, client.parts[3]...)
	if string(combined) != string(data) {
		t.Fatalf("data=%q want=%q", combined, data)
	}
}

func TestMultipartAbortsFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		client *fakeMultipartClient
	}{{"part", &fakeMultipartClient{failPart: 2}}, {"complete", &fakeMultipartClient{failComplete: true}}} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("abcdefghijklmnopqrstuvwxyz")
			err := uploadMultipart(context.Background(), tc.client, "bucket", "key", fixture(t, data), int64(len(data)), "application/octet-stream", 10, 3, nil)
			if err == nil {
				t.Fatal("expected error")
			}
			if !tc.client.aborted {
				t.Fatal("expected abort")
			}
		})
	}
}
