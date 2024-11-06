#! /bin/bash

# root folder path
folderpath=${1}
# going to the folder
LD_LIBRARY_PATH=/data/01/martines/MG5_aMC_v3_1_1/HEPTools/lib:$LD_LIBRARY_PATH
cd ${folderpath}
/data/01/martines/MG5_aMC_v3_1_1/HEPTools/MG5aMC_PY8_interface/MG5aMC_PY8_interface tag_3_pythia8.cmd
