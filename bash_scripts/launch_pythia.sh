#! /bin/bash

# root folder path
run_name=${1}

# launch pythia
/data/01/martines/MG5_aMC_v3_1_1/HEPTools/MG5aMC_PY8_interface/MG5aMC_PY8_interface tag_3_pythia8.cmd
mv pythia8_events.hepmc $run_name

# Check if the file was created
if [ -f $run_name ]; then
    echo $(pwd)
    echo "File pythia8_events.hepmc generated successfully."
else
    echo "Error: pythia8_events.hepmc was not generated."
    exit 1
fi
