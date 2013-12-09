#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# This tests a processor, and takes a processor job_bundle as input argument
# that is a tarball containing a script called `processor` which is to be given
# files as input and output results into a single file

echo "### Setting up test environment";

# Create test-folders
mkdir -p test-folder/input;
mkdir -p test-folder/output;

# Copy in job_bundle
cp $1 test-folder/job_bundle.tar.gz

# Copy in test files
cp $DIR/ss-ff-n-22.lzma test-folder/input/ss-ff-n-22.lzma
cp $DIR/ss-ff-n-28.lzma test-folder/input/ss-ff-n-28.lzma

# Extract job_bundle
cd test-folder;
tar -xzf job_bundle.tar.gz;

# Run tests
echo "### Running processor";
cat $DIR//input.txt | ./processor output/;

echo "### Files produced";
find output/;

if [ `ls input/ | wc -l` -ne "0" ]; then
  echo "### WARNING";
  echo "Input files where not deleted, please do this as they are consumed.";
fi; 
