#!/bin/bash

echo "Make sure current directory is code's root folder(main.go in the root folder)"

go mod tidy
go mod vendor
echo "Downloaded dependencies for go modules"

echo "Building the code"
#go build -o main . || exit
echo "Build successful! Starting on port 5000, make sure this port is not already used by another process"

#./main