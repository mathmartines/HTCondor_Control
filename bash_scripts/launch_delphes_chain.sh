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

#cd ${folderpath}
#gunzip ${pythia_file}.gz
#${delphes_exe} ${conf_card} ${folderpath}/${root_file} ${folderpath}/${pythia_file}



# Check if the root file already exists
if [ -f "${folderpath}/${root_file}" ]; then
    echo "The root file ${root_file} already exists. Skipping Delphes execution."
else
    # Decompress the Pythia file if it is compressed
    gunzip -f ${pythia_file}.gz
    # Run Delphes if the root file does not exist
    ${delphes_exe} ${conf_card} ${folderpath}/${root_file} ${folderpath}/${pythia_file}
fi
