#! /bin/bash

# path to the script
script_path=${1}
cd /data/01/martines/MG5_aMC_v3_1_1
# Setting up the environment
export PYTHONPATH=/data/01/martines/MG5_aMC_v3_1_1/:$PYTHONPATH
# going to the folder
./bin/mg5_aMC ${script_path}
