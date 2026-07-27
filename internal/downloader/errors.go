package downloader

import "errors"

// ErrIncompleteDownload — the transfer finished but fewer bytes arrived than
// the server advertised. run.go maps this to queue.ErrPermanent: re-running
// the same fetch against the same source yields the same truncated file, and
// the retry path keeps the partial download and skips straight to encoding it.
var ErrIncompleteDownload = errors.New("incomplete download")
