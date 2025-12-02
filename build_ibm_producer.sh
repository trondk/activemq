# Build the image
podman build -t mq-go-builder .

# Run and mount your code directory
podman run --rm -v $(pwd):/app:Z mq-go-builder go build -o producer_ibm producer_ibm.go
