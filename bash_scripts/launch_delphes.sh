#! /bin/bash

# root folder path
folderpath=${1}
# pythia file
pythia_file=${2}
# root file
root_file=${3}
# configuration card
conf_card=${4}
# delphes executable
delphes_exe=/data/01/martines/Tools/Delphes-3.5.0/DelphesHepMC2

#export ROOTSYS=/usr/include/root/:${ROOTSYS}
#export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/usr/local/bin:${PATH}
#export DYLD_LIBRARY_PATH=/usr/include/root/lib:${DYLD_LIBRARY_PATH}

cd ${folderpath}

# Check if the root file exists
if [ -f ${folderpath}/${root_file} ]; then
    echo "The root file ${root_file} exists. Deleting it."
    rm -f "${folderpath}/${root_file}"
fi

# Decompress the Pythia file
echo "Decompressing ${pythia_file}.gz..."
# Check if the file exists
if [ -f ${pythia_file}.gz ]; then
    echo "File ${pythia_file}.gz exists."

    # Check if it's a valid gzip file
    if gzip -t ${pythia_file}.gz 2>/dev/null; then
        echo "File ${pythia_file}.gz is a valid gzip file."
        gunzip -f ${pythia_file}.gz

    else
        echo "File ${pythia_file}.gz is not a valid gzip file. Renaming..."
        mv ${pythia_file}.gz ${pythia_file}
        echo "File has been renamed ."
    fi
else
    echo "File ${pythia_file}.gz does not exist."
fi


# Run Delphes
echo "Running Delphes..."
${delphes_exe} ${conf_card} ${folderpath}/${root_file} ${folderpath}/${pythia_file}

## Compress the Pythia file again
#echo "Compressing ${pythia_file}..."
#gzip -f ${pythia_file}
#
#echo "Script completed."