set -e
echo ">>> Pulling latest changes from the Git repository..."
git pull
echo ">>> Building the Go application..."
go build -o rwecc-ingester ingester_rwecc.go
echo ">>> Build complete! Binary 'rwecc-ingester' is ready."