#! /bin/bash

# root folder path
folderpath=${1}
# filename
filename=${2}
# going to the folder
cd ${folderpath}
echo ${folderpath}
# compress file
gzip ${filename}
