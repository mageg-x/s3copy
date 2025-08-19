package utils

import (
	"fmt"
	"net/url"
	"strings"
)

var logger = GetLogger("s3copy")

func GetFileNameFromURL(urlStr string) (string, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		logger.Errorf("Error parsing URL: %v", err)
		return "", fmt.Errorf("invalid URL: %v", err)
	}

	path := parsedURL.Path
	if path == "" || path == "/" {
		return "downloaded_file", nil
	}

	segments := strings.Split(path, "/")
	filename := segments[len(segments)-1]

	if filename == "" {
		return "downloaded_file", nil
	}

	return filename, nil
}
