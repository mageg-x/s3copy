package filesource

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

type EndpointConfig struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	Region    string
}

// parseEndpoint parses endpoint URL and extracts bucket information
// Supports formats:
// - http://bucket.endpoint.com (virtual hosted-style)
// - http://endpoint.com/bucket (path-style)
// - http://endpoint.com/bucket/prefix (path-style with prefix)
func ParseEndpoint(endpoint string, isDest bool) (*EndpointConfig, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint cannot be empty")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		logger.Errorf("failed to parse endpoint: %v", err)
		return nil, fmt.Errorf("invalid endpoint URL: %w", err)
	}

	config := &EndpointConfig{
		Region: "us-east-1", // default region
	}

	// Get credentials from environment variables
	if isDest {
		config.AccessKey = os.Getenv("DST_ACCESS_KEY")
		config.SecretKey = os.Getenv("DST_SECRET_KEY")
	} else {
		config.AccessKey = os.Getenv("SRC_ACCESS_KEY")
		config.SecretKey = os.Getenv("SRC_SECRET_KEY")
	}

	if config.AccessKey == "" || config.SecretKey == "" {
		logger.Errorf("failed to parse endpoint URL")
		return nil, fmt.Errorf("missing ak/sk in environment variables")
	}

	// Extract host without port for IP check
	hostWithoutPort := u.Hostname()
	if net.ParseIP(hostWithoutPort) != nil {
		// IP address format - always use path-style
		return parsePathStyle(u, config)
	}

	// Try virtual hosted-style first if valid
	if isValidVirtualHost(u.Host) {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			config.Bucket = parts[0]
			config.Endpoint = fmt.Sprintf("%s://%s", u.Scheme, parts[1])
			config.Prefix = strings.TrimPrefix(u.Path, "/")
			return config, nil
		}
	}

	// Fall back to path-style parsing
	return parsePathStyle(u, config)
}

// isValidVirtualHost checks if host is valid for virtual hosted-style
func isValidVirtualHost(host string) bool {
	if strings.Count(host, ".") < 1 {
		return false // No dots, can't split bucket
	}
	// Check for valid domain structure (avoid over-splitting IP-like formats)
	parts := strings.Split(host, ".")
	if len(parts) < 2 {
		logger.Errorf("invalid virtual host")
		return false
	}
	// Last part should be TLD (at least 2 characters)
	if len(parts[len(parts)-1]) < 2 {
		logger.Errorf("invalid virtual host")
		return false
	}
	return true
}

// parsePathStyle handles path-style URLs (bucket in path)
func parsePathStyle(u *url.URL, config *EndpointConfig) (*EndpointConfig, error) {
	config.Endpoint = fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	// Trim leading/trailing slashes and split path
	trimmedPath := strings.Trim(u.Path, "/")
	if trimmedPath == "" {
		logger.Errorf("invalid path style")
		return nil, fmt.Errorf("bucket not found in path for endpoint: %s", u.String())
	}

	// Split into bucket and optional prefix
	parts := strings.SplitN(trimmedPath, "/", 2)
	config.Bucket = parts[0]
	if len(parts) > 1 {
		config.Prefix = parts[1]
	}
	return config, nil
}
