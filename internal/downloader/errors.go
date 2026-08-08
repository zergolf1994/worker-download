package downloader

import "errors"

// ErrIncompleteDownload — the transfer finished but fewer bytes arrived than
// the server advertised. run.go maps this to queue.ErrPermanent: re-running
// the same fetch against the same source yields the same truncated file, and
// the retry path keeps the partial download and skips straight to encoding it.
var ErrIncompleteDownload = errors.New("incomplete download")

// ErrByteRangeNotHonored prevents retrying a server that ignored a Range
// request. Retrying would only repeat the same unsafe full-resource response.
var ErrByteRangeNotHonored = errors.New("byte range request not honored")

// ErrInvalidVideo identifies a source that ffprobe could open only far enough
// to reject its container or that contains no video stream. Tool startup errors
// are intentionally excluded so callers do not discard valid sources when the
// worker environment is broken.
var ErrInvalidVideo = errors.New("invalid video source")
