#!/bin/bash

# Activate the Python environment
source ./.venv/bin/activate

# Create the list of files to download
# python ./script_crawler.py

# Download the files
python ./script_download.py "$@"
