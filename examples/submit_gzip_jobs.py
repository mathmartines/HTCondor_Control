#! /usr/bin/env python3

"""Example on how to use the SubmitBuilder class"""

from src.SubmitBuilder import SubmitBuilder
from src.DagCreator import DagCreator
from htcondor import dags
import htcondor
from pathlib import Path


def pythia8_jobs_template(
        arguments: str, log_name: str
) -> htcondor.Submit:
    """
    Creates the submit job for pythia8.

    :param arguments: arguments to the executable
    :param log_name: name to give to the log file
    """
    return htcondor.Submit(
        universe="vanilla",
        executable="./bash_scripts/gzip_files.sh",
        arguments=arguments,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        request_cpus=1,
        # requirements='(Machine!="fmahep.if.usp.br")',
        # request_memory="1GB",
        getenv=True,
        when_to_transfer_output="ON_EXIT",
    )


if __name__ == "__main__":
    jobs_list = []  # stores the list of jobs we want to simulate
    # folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-dimuon-13TEV/UniversalSMEFT_d8"  # root folder
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/VBS/ATLAS_ZZjj"
    # list of folders containning the eft terms to shower
    simulations_folders = [
        # "P6", "P11", "P23", "P24",
        # "P26",
        # "P6_2", "P6_P11", "P6_P23", "P6_P24", "P6_P26",
        # "P11_2", "P11_P23", "P11_P24", "P11_P26", "P23_2", "P23_P24", "P23_P26", "P24_2", "P24_P26", "P26_2"
        "T0", "T0_2"
        # "SM", "cphi1T", "D4FT", "cBWT", "cBWT-cBWT", "cphi1T-cBWT", "cphi1T-cphi1T", "cphi1T-D4FT", "D4FT-cBWT",
        # "D4FT-D4FT",
        # "c2JB", "c2JW", "c2JB-c2JW", "c2JW-c2JW",
        # "c2JB-c2JWrenorm", "c2JB-cBW", "c2JB-cphi1", "c2JW-c2JWrenorm", "c2JW-cBW", "c2JW-cphi1",
        # "c1psi2H2D3", "c2psi2H2D3", "c5psi4H2", "c4psi4H2", "c7psi4H2",
        # "c2psi4D2", "c3psi4D2"
    ]

    for eft_term in simulations_folders:
        for bin_index in range(1, 4):
            jobs_list.append(
                pythia8_jobs_template(
                    arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01 tag_1_pythia8_events.hepmc",
                    log_name=f"/data/01/martines/hep_programs/HTCondor_Control/Logs/{eft_term}-{bin_index}-gzip"
                )
            )

    # creates a dag where only 30 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=20, jobs_list=jobs_list)
    dag = dag_creator.build_dag()

    # saves the dag file in the Logs folder
    dag_dir = (Path.cwd() / "Logs/DY").absolute()
    dag_file = dags.write_dag(dag, dag_dir, dag_file_name="gzip_vbs3.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
