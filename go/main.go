package main


import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Constants
const (
	chunkSize = 50000
	bufferSize = 10 * 1024 * 1024 // 10MB

)

// Variables
var (
	inputDir  = "D:/reddit/dumps/reddit/submissions"
	outputDir = "D:/reddit/dumps/reddit/submissions/organized"
)

// Structs
type RedditPost struct {
	Subreddit  string  `json:"subreddit"`
	CreatedUTC float64 `json:"created_utc"`
}

// Main function
func main() {
	setupDirectories()

	files, err := getFiles(inputDir)
	if err != nil {
		fmt.Printf("Error getting files: %v\n", err)
		return
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 4) // Limit concurrent file processing

	for _, file := range files {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(file string) {
			defer wg.Done()
			defer func() { <-semaphore }()
			if err := processFile(file); err != nil {
				fmt.Printf("Error processing file %s: %v\n", file, err)
			}
		}(file)
	}

	wg.Wait()

	fmt.Println("Processing complete. Compressing output files...")
	compressOutputFiles()
	fmt.Println("Done :>")
}


// Utility functions
func setupDirectories() {
	inputDir = filepath.FromSlash(inputDir)
	outputDir = filepath.FromSlash(outputDir)
}

func getFiles(path string) ([]string, error) {
	var files []string
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".zst") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// File processing functions
func processFile(path string) error {
	fmt.Printf("Processing file %s\n", path)

	filename := filepath.Base(path)
	monthYear := strings.TrimPrefix(strings.TrimSuffix(filename, ".zst"), "RS_")

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", path, err)
	}
	defer file.Close()

	zReader, err := zstd.NewReader(file)
	if err != nil {
		return fmt.Errorf("error creating zstd reader for file %s: %v", path, err)
	}
	defer zReader.Close()

	scanner := bufio.NewScanner(zReader)
	scanner.Buffer(make([]byte, bufferSize), bufferSize)

	chunk := make(map[string][]RedditPost)
	rowCount := 0

	progressLog, err := NewFileProgressLog(path, file)
	if err != nil {
		return fmt.Errorf("error creating progress log: %v", err)
	}

	start := time.Now()
	for scanner.Scan() {
		var post RedditPost
		if err := json.Unmarshal(scanner.Bytes(), &post); err != nil {
			fmt.Printf("Error parsing JSON: %v\n", err)
			continue
		}

		subreddit := sanitizeSubredditName(post.Subreddit)

		chunk[subreddit] = append(chunk[subreddit], post)

		rowCount++
		progressLog.OnRow()

		if rowCount >= chunkSize {
			if err := writeChunksToDisk(monthYear, chunk); err != nil {
				return fmt.Errorf("error writing chunk to disk: %v", err)
			}
			chunk = make(map[string][]RedditPost)
			rowCount = 0
		}

		// Check for timeout every 1000 rows
		if rowCount%1000 == 0 && time.Since(start) > 5*time.Minute {
			return fmt.Errorf("timeout reached while processing file")
		}
	}

	if len(chunk) > 0 {
		if err := writeChunksToDisk(monthYear, chunk); err != nil {
			return fmt.Errorf("error writing final chunk to disk: %v", err)
		}
	}

	progressLog.LogProgress("\n")

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %v", path, err)
	}

	return nil
}

func writeChunksToDisk(monthYear string, chunk map[string][]RedditPost) error {
	for subreddit, posts := range chunk {
		if err := writeJSONLChunk(monthYear, subreddit, posts); err != nil {
			return fmt.Errorf("error writing JSONL chunk for %s: %v", subreddit, err)
		}
	}
	return nil
}

func writeJSONLChunk(monthYear, subreddit string, data []RedditPost) error {
	monthDir := filepath.Join(outputDir, monthYear)
	if err := os.MkdirAll(monthDir, 0755); err != nil {
		return fmt.Errorf("error creating directory %s: %v", monthDir, err)
	}

	outputFile := filepath.Join(monthDir, fmt.Sprintf("%s.jsonl", subreddit))
	file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", outputFile, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, item := range data {
		jsonData, err := json.Marshal(item)
		if err != nil {
			fmt.Printf("Error marshaling JSON: %v\n", err)
			continue
		}
		if _, err := writer.Write(append(jsonData, '\n')); err != nil {
			return fmt.Errorf("error writing to file %s: %v", outputFile, err)
		}
	}

	return nil
}

// Helper functions
func sanitizeSubredditName(name string) string {
	re := regexp.MustCompile("[^\\w\\-]")
	sanitized := re.ReplaceAllString(name, "")
	if len(sanitized) > 50 {
		sanitized = sanitized[:50]
	}
	return sanitized
}

// Compression functions
func compressOutputFiles() error {
	return filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".jsonl") {
			if err := compressToZst(path); err != nil {
				fmt.Printf("Error compressing file %s: %v\n", path, err)
			}
		}
		return nil
	})
}

func compressToZst(inputFile string) error {
	outputFile := strings.TrimSuffix(inputFile, ".jsonl") + ".zst"

	input, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("error opening input file %s: %v", inputFile, err)
	}
	defer input.Close()

	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %v", outputFile, err)
	}
	defer output.Close()

	encoder, err := zstd.NewWriter(output)
	if err != nil {
		return fmt.Errorf("error creating zstd encoder: %v", err)
	}
	defer encoder.Close()

	if _, err = io.Copy(encoder, input); err != nil {
		return fmt.Errorf("error compressing file %s: %v", inputFile, err)
	}

	if err := os.Remove(inputFile); err != nil {
		return fmt.Errorf("error removing original file %s: %v", inputFile, err)
	}

	return nil
}