package rustfs

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/anovimous/automata-worker/orchestrator/internal/config"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Rustfs struct {
	client *minio.Client
	cfg    config.RustFSConfig
}

func New(cfg config.RustFSConfig) (*Rustfs, error) {

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rustfs client: %w", err)
	}

	return &Rustfs{client: client, cfg: cfg}, nil
}

func (r *Rustfs) DownloadJobDetailsAndData(jobId int, dir string) (detailsLocalLoc string, dataLocalLoc string, e error) {

	detailsRemoteObjectName := fmt.Sprintf("details/%d.json", jobId)

	detailsLocalTargetPath, dataLocalTargetPath := localPaths(dir, jobId)

	err := r.client.FGetObject(context.Background(), r.cfg.JobsBucket, detailsRemoteObjectName, detailsLocalTargetPath, minio.GetObjectOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to download job DETAILS for job %d: %w", jobId, err)
	}

	dataRemoteObjectName := fmt.Sprintf("data/%d.jsonl", jobId)

	err = r.client.FGetObject(context.Background(), r.cfg.JobsBucket, dataRemoteObjectName, dataLocalTargetPath, minio.GetObjectOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to download job DATA file for job %d: %w", jobId, err)
	}

	return detailsLocalTargetPath, dataLocalTargetPath, nil

}

// localPaths are the files in dir that the details and the data of the job are downloaded to.
func localPaths(dir string, jobId int) (details string, data string) {
	return filepath.Join(dir, fmt.Sprintf("details-%d.json", jobId)), filepath.Join(dir, fmt.Sprintf("data-%d.jsonl", jobId))
}

func (r *Rustfs) UploadResult(jobId int, localResultsLoc string) error {

	objectName := fmt.Sprintf("%d.result", jobId)

	_, err := r.client.FPutObject(context.Background(), r.cfg.ResultsBucket, objectName, localResultsLoc, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to upload result for job %d: %w", jobId, err)
	}

	return nil
}
