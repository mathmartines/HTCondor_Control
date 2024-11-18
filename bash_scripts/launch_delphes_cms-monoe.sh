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


cd ${folderpath}
if [ -f "${folderpath}/${root_file}" ]; then
    rm -rf ${root_file}

gunzip temp-pythia-events.hepmc.gz
${delphes_exe} ${conf_card} ${folderpath}/${root_file} ${folderpath}/temp-pythia-events.hepmc
rm -rf temp-pythia-events.hepmc
rm -rf delphes_events.root