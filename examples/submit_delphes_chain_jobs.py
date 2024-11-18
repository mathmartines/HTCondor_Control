"""Generates the MG5 runs"""

from pathlib import Path
from HTCondor_Control.src.NestedDagsCreator import (create_layer, create_child_layer, create_subdag,
                                                    create_child_subdag, NestedDags)
import htcondor
from htcondor import dags


def delphes_job_template(
        arguments: str, log_name: str
):
    """Template for delphes jobs"""
    return htcondor.Submit(
        universe="vanilla",
        executable="./bash_scripts/launch_delphes_chain.sh",
        arguments=arguments,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        request_cpus=1,
        request_memory="1GB",
        getenv=True,
        requirements='(Machine=="fmahep.if.usp.br")',
        when_to_transfer_output="ON_EXIT"
    )


def gzip_jobs_template(
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
    dags_list = []  # stores the list of dags we want to simulate
    # list all the directories we need to run the simulations
    # simulations_folders = ["SM"]
    simulations_folders = ('c5psi4H2 c3psi4D2 c3W2H4T-c3W2H4T D4FT c3W2H4T cphi1T-c3W2H4T cBWT cphi1T-cphi1T '
                           'c2JW-cphi1 cphi1T-cBWT D4FT-c3W2H4T c2JW cBWT-c3W2H4T cphi1T cphi1T-D4FT D4FT-D4FT '
                           'cBWT-cBWT c2psi2H2D3 c2JW-c2JW D4FT-cBWT').split()

    # dag creater to generate the pythia + delphes jobs for each simulation
    dag_detector_simulations = NestedDags(max_number_jobs=1, initial_layer_creator=create_layer,
                                          nodes_creators=create_child_layer)

    # holds all the jobs to run
    logs_folder = "/data/01/martines/hep_programs/HTCondor_Control/Logs/UniversalSMEFT"

    # path to where the simulations are stored
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-monoe-13TEV/UniversalSMEFT_d8"

    # list of delphes cards we need to run
    delphes_cards = {
        "cms-monomu": "delphes_card_CMS_monomuons.tcl",
        "atlas-monoe": "delphes_card_ATLAS_default.tcl",
        # create the card below
        "atlas-monomu": "delphes_card_ATLAS_default_muons.tcl",
    }

    # creates the script for the run and runs the simulation
    for simulation in simulations_folders:
        for bin_index in range(1, 8):
            delphes_jobs_list = []
            # for each simulation we need to run each delphes card
            for exp, card in delphes_cards.items():
                delphes_jobs_list.append(
                    (f"{logs_folder}/{exp}-{simulation}-{bin_index}-delphes", delphes_job_template(
                        arguments=f"{folderpath}/{simulation}/bin_{bin_index}/Events/run_01 pythia8_events.hepmc "
                                  f"{exp}-events.root "
                                  f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/{card}",
                        log_name=f"{logs_folder}/{exp}-{simulation}-{bin_index}"))
                )
            # adds the gzip job
            delphes_jobs_list.append(
                (f"{logs_folder}/{simulation}-{bin_index}-gzip-files", gzip_jobs_template(
                    arguments=f"{folderpath}/{simulation}/bin_{bin_index}/Events/run_01 pythia8_events.hepmc",
                    log_name=f"{logs_folder}/{simulation}-{bin_index}-gzip"
                ))
            )
            # Creates the dag for the two jobs
            combined_job = dag_detector_simulations.build_dag(delphes_jobs_list)
            # saves the dag file in the Logs folder
            dag_file_path = (Path.cwd() / "Logs/UniversalSMEFT").absolute()
            dag_file = dags.write_dag(combined_job, dag_file_path, dag_file_name=f"{simulation}-{bin_index}-dag.dag")
            # Adds the job to the list pf jobs
            dags_list.append((f"{logs_folder}/{simulation}-{bin_index}-dag", dag_file))

    dag_jobs = NestedDags(max_number_jobs=10, initial_layer_creator=create_subdag, nodes_creators=create_child_subdag)
    jobs = dag_jobs.build_dag(dags_list)
    dag_jobs_path = (Path.cwd() / "Logs/UniversalSMEFT").absolute()
    dag_jobs_file = dags.write_dag(jobs, dag_jobs_path, dag_file_name=f"delphes-chains.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_jobs_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()
    print(f"Id of the job: {cluster_id}")
