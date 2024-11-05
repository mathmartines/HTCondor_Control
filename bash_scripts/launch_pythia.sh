#! /bin/bash

# root folder path
folderpath=${1}
# going to the folder
cd ${folderpath}
echo $(pwd)
# launch pythia
./run_shower.sh