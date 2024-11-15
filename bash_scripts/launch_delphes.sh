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
gunzip ${pythia_file}.gz
${delphes_exe} ${conf_card} ${folderpath}/${root_file} ${folderpath}/${pythia_file}
gzip ${pythia_file}