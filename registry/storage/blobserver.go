package storage

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/golang/gddo/httputil/header"
	"github.com/opencontainers/go-digest"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver   driver.StorageDriver
	statter  distribution.BlobStatter
	pathFn   func(dgst digest.Digest) (string, error)
	redirect bool // allows disabling URLFor redirects
}

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	if compression := r.Header.Get("Docker-Transport-Compression"); compression == "enabled" {
		found, err := bs.ServeBlobContent(ctx, w, r, dgst)
		if found || err != nil {
			return err
		}
	}

	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return err
		}
	}

	br, err := newFileReader(ctx, bs.driver, path, desc.Size)
	if err != nil {
		return err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
	}

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
	return nil
}

func (bs *blobServer) ServeBlobContent(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (bool, error) {
	path, err := bs.pathFn(dgst)
	if err != nil {
		return false, err
	}

	accepted := header.ParseAccept(r.Header, "Accept-Encoding")
	for _, enc := range accepted {
		path = filepath.Join(filepath.Dir(path), "data."+enc.Value)

		stat, err := bs.driver.Stat(ctx, path)
		if err != nil {
			if _, ok := err.(driver.PathNotFoundError); ok {
				continue
			}
			return false, err
		}
		size := stat.Size()

		br, err := newFileReader(ctx, bs.driver, path, size)
		if err != nil {
			return false, err
		}
		defer br.Close()

		w.Header().Set("ETag", dgst.String()) // If-None-Match handled by ServeContent
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))
		w.Header().Set("Content-Type", "application/octet-stream") // FIXME should be the actual content media-type
		w.Header().Set("Content-Length", fmt.Sprint(size))
		w.Header().Set("Content-Encoding", enc.Value)

		http.ServeContent(w, r, dgst.String(), time.Time{}, br)
		return true, nil
	}

	return false, nil
}
