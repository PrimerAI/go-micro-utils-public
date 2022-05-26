# Example of use

Here is a basic example of how to use the Client

```go
package main

import (
	"fmt"

	"github.com/PrimerAI/go-micro-utils-public/gmu/s3"
)

func main() {
	client, err := s3.NewClient(
        // AWS s3
		s3.WithEndpoint(""),
        // Minio
		// s3.WithEndpoint("http://127.0.0.1:9000"), 
		s3.WithS3ForcePathStyle(true),
	)
	if err != nil {
		print("panic")
		panic(err)
	}
	list, err := client.List(s3.Path{
		Bucket: "some-bucket",
		Key:    "some-dir",
	})
	if err != nil {
		print("panic")
		panic(err)
	}
	for _, ele := range list {
		fmt.Println(ele.Key)
	}
}

```

This code will list the keys under `s3://some-bucket/some-dir`.

## AWS
For normal AWS use `s3.NewClient()` should be enough

## Minio
To use a MINIO server use ```go
client, err := s3.NewClient(
        s3.WithEndpoint("minio server uri"), 
		s3.WithS3ForcePathStyle(true),
	)
```
Note that `s3.WithS3ForcePathStyle(true)` is required
