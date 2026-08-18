package dashboard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProcessLogPathRejectsTraversal(t *testing.T) {
	valid, ok := processLogPath("Abc_123-test")
	if !ok || valid != filepath.Join("logs", "process", "Abc_123-test.log") {
		t.Fatalf("valid path = %q, %v", valid, ok)
	}
	for _, slug := range []string{"../secret", "a/b", `a\\b`, "", "name.log"} {
		if path, ok := processLogPath(slug); ok {
			t.Errorf("processLogPath(%q) unexpectedly allowed %q", slug, path)
		}
	}
}

func TestReadLogTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.log")
	if err := os.WriteFile(path, []byte("0123456789"), 0644); err != nil {
		t.Fatal(err)
	}
	content, truncated, err := readLogTail(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated || string(content) != "6789" {
		t.Fatalf("tail = %q, truncated=%v", content, truncated)
	}
	content, truncated, err = readLogTail(path, 20)
	if err != nil || truncated || !strings.HasPrefix(string(content), "0123") {
		t.Fatalf("full read = %q, truncated=%v, err=%v", content, truncated, err)
	}
}
