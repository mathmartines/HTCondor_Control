#! /usr/bin/env python3

"""Example on how to use the SubmitBuilder class"""

from HTCondor_Control.src.DagCreator import DagCreator
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
        # transfer_input_files=input_files,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        getenv=True,
        requirements='(Machine=="fmahep.if.usp.br")',
        when_to_transfer_output="ON_EXIT"
    )


if __name__ == "__main__":
    jobs_list = []  # stores the list of jobs
    # list of folders containning the simulations
    # simulations_folders = [
    #     "SM"
    #     # "cphi1T", "D4FT", "cBWT", "cBWT-cBWT", "cphi1T-cBWT", "cphi1T-cphi1T", "cphi1T-D4FT", "D4FT-cBWT",
    #     # "D4FT-D4FT",
    #     # "c2JB", "c2JW", "c2JB-c2JW", "c2JW-c2JW",
    #     # "c2JB-c2JWrenorm", "c2JB-cBW", "c2JB-cphi1", "c2JW-c2JWrenorm", "c2JW-cBW", "c2JW-cphi1",
    #     # "c1psi2H2D3", "c2psi2H2D3", "c5psi4H2", "c4psi4H2", "c7psi4H2", "c2psi4D2", "c3psi4D2"
    # ]
    simulations_folders = ("cphi1T cBWT D4FT c3W2H4T cphi1T-cphi1T cphi1T-cBWT cphi1T-D4FT cphi1T-c3W2H4T D4FT-D4FT "
                           "D4FT-cBWT D4FT-c3W2H4T cBWT-cBWT cBWT-c3W2H4T c3W2H4T-c3W2H4T").split()
    # creating folders we wish to simulate
    # the key represents the name we use to identify the logs and the values the parameters for the executable
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-monoe-13TEV/UniversalSMEFT_d8"

    # constructs all the jobs
    for eft_term in simulations_folders:
        for bin_index in range(1, 8):
            jobs_list.append(
                delphes_job_template(
                    arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01 pythia8_events.hepmc "
                              f"delphes_events.root "
                              f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_CMS_monolepton.tcl",
                    input_files=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/pythia8_events.hepmc, "
                                f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_CMS_monolepton.tcl",
                    output_files="delphes_events.root",
                    log_name=f"/data/01/martines/hep_programs/HTCondor_Control/Logs/DY/{eft_term}-{bin_index}-delphes"
                )
            )

    # creates the dag where only 5 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=10, jobs_list=jobs_list)
    dag = dag_creator.build_dag()

    # saves the dag file in the Logs folder
    dag_dir = (Path.cwd() / "Logs/DY").absolute()
    dag_file = dags.write_dag(dag, dag_dir, dag_file_name="delphes.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
