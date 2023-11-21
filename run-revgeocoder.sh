#!/bin/bash

# Check if a command line argument is provided
if [ $# -eq 0 ]; then
    echo "Error: No directory path provided."
    echo "Usage: $0 <path-to-local-data-directory>"
    exit 1
fi

# Get data directory path
local_data_directory="$1"

# Pull the Docker image
sudo docker pull meyassu/revgeocoder:latest

# Run the container with the data directory mounted
sudo docker run --rm -v "$local_data_directory:/usr/src/app/user_data" meyassu/revgeocoder:latest

