package main

import (
	"context"
	"io"
	"os"
	"fmt"
	"net/http"
	"strconv"
	"cloud.google.com/go/storage"
	"log"
)

var (
	bucket      *storage.BucketHandle
	ctx         = context.Background()
	index       = "index.html"
)
func httpMain(write http.ResponseWriter, read *http.Request) {
	uri := read.URL.Path[1:]
	if "" == uri {
		uri = index
	}
	obj := bucket.Object(uri)
	objAttrs, err := obj.Attrs(ctx)
	obj = obj.ReadCompressed(true)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		fmt.Println("Error accessing GCS:", err)
		http.Error(write, "Internal server error", 500)
		return
	}
	defer reader.Close()
	write.Header().Set("Content-Type", objAttrs.ContentType)
	write.Header().Set("Content-Encoding", objAttrs.ContentEncoding)
	write.Header().Set("Content-Length", strconv.Itoa(int(objAttrs.Size)))
	write.Header().Set("Content-Disposition", objAttrs.ContentDisposition)
	write.Header().Set("Cache-Control", objAttrs.CacheControl)
	write.Header().Set("ETag", objAttrs.Etag)
	write.WriteHeader(200)
	if _, err := io.Copy(write, reader); nil != err {
		fmt.Println("Error:", err)
		http.Error(write, "Internal server error", 500)
		return
	}
}
func main() {
	client, err := storage.NewClient(ctx)
	bucket = client.Bucket(os.Getenv("BUCKET"))
	if (err != nil) {
		log.Fatal("Cannot access GCS bucket")
	}
	port := os.Getenv("PORT")
	if port == "" {
			port = "8080"
			log.Printf("Defaulting to port %s", port)
	}
	http.HandleFunc("/", httpMain)
	http.ListenAndServe(":"+port, nil)
}