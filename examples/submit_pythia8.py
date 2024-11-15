#! /usr/bin/env python3

"""Example on how to use the SubmitBuilder class"""

from HTCondor_Control.src.DagCreator import DagCreator
from htcondor import dags
import htcondor
from pathlib import Path


def pythia8_jobs_template(
        arguments: str, input_files: str, log_name: str
) -> htcondor.Submit:
    """
    Creates the submit job for pythia8.

    :param arguments: arguments to the executable
    :param input_files: files needed to run
    :param log_name: name to give to the log file
    """
    return htcondor.Submit(
        universe="vanilla",
        executable="./bash_scripts/launch_pythia.sh",
        arguments=arguments,
        transfer_input_files=input_files,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        request_cpus=1,
        # request_memory="1GB",
        getenv=True,
        requirements='(Machine!="fmahep.if.usp.br")',
        when_to_transfer_output="ON_EXIT",
    )


if __name__ == "__main__":
    jobs_list = []  # stores the list of jobs we want to simulate
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-monoe-13TEV/UniversalSMEFT_d8"  # root folder
    # list of folders containning the eft terms to shower
    # simulations_folders = ("cphi1T cBWT D4FT c3W2H4T cphi1T-cphi1T cphi1T-cBWT cphi1T-D4FT cphi1T-c3W2H4T D4FT-D4FT "
    #                        "D4FT-cBWT D4FT-c3W2H4T cBWT-cBWT cBWT-c3W2H4T c3W2H4T-c3W2H4T").split()
    simulations_folders = "c2JW c2JW-c2JW c2JW-cphi1 c2psi2H2D3 c5psi4H2 c3psi4D2".split()

    for eft_term in simulations_folders:
        for bin_index in range(1, 8):
            jobs_list.append(
                pythia8_jobs_template(
                    arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01",
                    input_files=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/tag_3_pythia8.cmd, "
                                f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/unweighted_events.lhe.gz",
                    log_name=f"/data/01/martines/hep_programs/HTCondor_Control/Logs/DY/{eft_term}-{bin_index}-pythia"
                )
            )

    # creates a dag where only 30 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=20, jobs_list=jobs_list)
    dag = dag_creator.build_dag()

    # saves the dag file in the Logs folder
    dag_dir = (Path.cwd() / "Logs/DY").absolute()
    dag_file = dags.write_dag(dag, dag_dir, dag_file_name="pythia.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
