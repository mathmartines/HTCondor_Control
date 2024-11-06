#! /usr/bin/env python3

"""Example on how to use the SubmitBuilder class"""

from src.SubmitBuilder import SubmitBuilder
from src.DagCreator import DagCreator
from htcondor import dags
import htcondor
from pathlib import Path


def delphes_job_template(
    arguments: str, input_files: str, output_files: str, log_name: str
):
    """Template for delphes jobs"""
    return htcondor.Submit(
        universe="vanilla",
        executable="./bash_scripts/launch_delphes.sh",
        arguments=arguments,
        transfer_input_files=input_files,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        getenv=True,
        requirements='(Machine=="fmahep.if.usp.br")',
        transfer_output_files=output_files,
        when_to_transfer_output="ON_EXIT",
    )


if __name__ == "__main__":
    jobs_list = []  # stores the list of jobs
    # list of folders containning the simulations
    simulations_folders = [
        # "cBWT-cBWT", "cphi1T-cBWT", "cphi1T-cphi1T", "cphi1T-D4FT", "D4FT-cBWT",
        "D4FT-D4FT"
    ]
    # creating folders we wish to simulate
    # the key represents the name we use to identify the logs and the values the parameters for the executable
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-dielectron-13TEV/UniversalSMEFT_d8"

    # constructs all the jobs
    for eft_term in simulations_folders:
        for bin_index in range(5, 30):
            jobs_list.append(
                delphes_job_template(
                    arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01 pythia8_events.hepmc "
                              f"delphes_events_final.root "
                              f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_CMS_dilepton.tcl",
                    input_files=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/pythia8_events.hepmc, "
                                f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_CMS_dilepton.tcl",
                    output_files="delphes_events_final.root",
                    log_name=f"/data/01/martines/hep_programs/HTCondor_Control/Logs/DY/{eft_term}-{bin_index}-delphes"
                )
            )

    dag_creator = DagCreator(max_number_jobs=5, jobs_list=jobs_list)
    dag = dag_creator.build_dag()

    # saves the dag file in the Logs folder
    dag_dir = (Path.cwd() / "Logs/DY").absolute()
    dag_file = dags.write_dag(dag, dag_dir, dag_file_name="delphes.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
