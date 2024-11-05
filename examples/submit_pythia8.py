#! /usr/bin/env python3

"""Example on how to use the SubmitBuilder class"""

from src.SubmitBuilder import SubmitBuilder
from src.DagCreator import DagCreator
from htcondor import dags
import htcondor
from pathlib import Path

if __name__ == "__main__":
    # list of folders containning the simulations
    simulations_folders = [
       # "D4FT", "cphi1T", "cBWT", "cBWT-cBWT", "cphi1T-cBWT", "cphi1T-cphi1T", "cphi1T-D4FT", "D4FT-cBWT", "D4FT-D4FT"
        "SM"
    ]
    # creating folders we wish to simulate
    # the key represents the name we use to identify the logs and the values the parameters for the executable
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-dimuon-13TEV/UniversalSMEFT_d8"
    submission_dict = {
        f"{coef}_bin_{bin_index}-pythia8": {
            "args": [f"{folderpath}/{coef}/bin_{bin_index}/Events/run_01/"],
            "transfer_input_files": f"{folderpath}/{coef}/bin_{bin_index}/Events/run_01/tag_3_pythia8.cmd"
        }
        for coef in simulations_folders for bin_index in range(13, 29)
    }

    # creates the list of jobs and the dagger
    submit_builder = SubmitBuilder(
        executable="./bash_scripts/launch_pythia.sh",
        jobs=submission_dict,
        logs_folderpah="/data/01/martines/hep_programs/HTCondor_Control/Logs/DY",
        requirements='(Machine!="fmahep02.if.usp.br")'
    )

    dag_creator = DagCreator(max_number_jobs=20, jobs_list=submit_builder.create_submission_list())
    dag = dag_creator.build_dag()

    # saves the dag file in the Logs folder
    dag_dir = (Path.cwd() / "Logs/DY").absolute()
    dag_file = dags.write_dag(dag, dag_dir, dag_file_name="pythia.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
