"""Generates the MG5 runs"""

from pathlib import Path
from ServerScripts.src.Commands import list_folders
from HTCondor_Control.src.NestedDagsCreator import (create_layer, create_child_layer, create_subdag,
                                                    create_child_subdag, NestedDags)
import htcondor
from HTCondor_Control.examples.submit_delphes_jobs import delphes_job_template
from htcondor import dags
import shutil


def pythia8_jobs_template(
        arguments: str, input_files: str, output_file: str, simulation_folder: str, log_name: str
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
        request_cpus=2,
        request_memory="1GB",
        RunAsOwner=True,
        getenv=True,
        requirements='(Machine!="fmahep.if.usp.br")',
        when_to_transfer_output="ON_EXIT",
        transfer_output_files=output_file,
        transfer_output_remaps=f'"{output_file}={simulation_folder}"'
    )


if __name__ == "__main__":
    dags_list = []  # stores the list of dags we want to simulate
    # list all the directories we need to run the simulations
    simulations_folders = list_folders(Path("/data/01/martines/MG5_aMC_v3_1_1/PhD/High-PT/atlas-ditau-13TEV"))

    # dag creater to generate the pythia + delphes jobs for each simulation
    dag_shower_detector = NestedDags(max_number_jobs=1, initial_layer_creator=create_layer,
                                     nodes_creators=create_child_layer)

    # holds all the jobs to run
    jobs_list = []
    logs_folder = "/data/01/martines/hep_programs/HTCondor_Control/Logs/High-PT"

    # path to the folder to get the pythia files
    pythia_files = ("/data/01/martines/MG5_aMC_v3_1_1/PhD/DY"
                    "/cms-dielectron-13TEV/UniversalSMEFT_d8/SM/bin_1/Events/run_01")

    # creates the script for the run and runs the simulation
    for simulation in simulations_folders:
        print(f"INFO: Creating files to run shower for folder {simulation}")
        shutil.copy(f"{pythia_files}/run_shower.sh", simulation / "Events/run_01")
        shutil.copy(f"{pythia_files}/tag_3_pythia8.cmd", simulation / "Events/run_01")

        # name for the log file
        *_, initial_flavor, eft_terms, bin_number = str(simulation).split("/")
        log_file = f"{initial_flavor}-{eft_terms}-{bin_number}"

        # Jobs for each simulation
        pythia_job = pythia8_jobs_template(
            arguments=f"{log_file}.hepmc",
            input_files=f"{simulation}/Events/run_01/tag_3_pythia8.cmd, {simulation}/Events/run_01/run_shower.sh,"
                        f"{simulation}/Events/run_01/unweighted_events.lhe.gz",
            log_name=f"{logs_folder}/{log_file}-pythia",
            output_file=f"{log_file}.hepmc",
            simulation_folder=f"{simulation}/Events/run_01/pythia8_events.hepmc"
        )
        delphes_job = delphes_job_template(
            arguments=f"{simulation}/Events/run_01 pythia8_events.hepmc "
                      f"delphes_events.root "
                      f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_ta_ta_ATLAS_2002_12223.dat",
            input_files=f"{simulation}/Events/run_01_/pythia8_events.hepmc, "
                        f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_ta_ta_ATLAS_2002_12223.dat",
            output_files="delphes_events.root",
            log_name=f"{logs_folder}/{log_file}-delphes"
        )
        # Creates the dag for the two jobs
        combined_job = dag_shower_detector.build_dag(
            [(f"{logs_folder}/{log_file}-shower", pythia_job),
             (f"{logs_folder}/{log_file}-detector", delphes_job)]
        )

        # saves the dag file in the Logs folder
        dag_file_path = (Path.cwd() / "Logs/High-PT").absolute()
        dag_file = dags.write_dag(combined_job, dag_file_path, dag_file_name=f"{log_file}-dag.dag")
        # Adds the job to the list pf jobs
        dags_list.append((f"{logs_folder}/{log_file}-dag", dag_file))


    # builds the graph with all the jobs
    dag_jobs = NestedDags(max_number_jobs=20, initial_layer_creator=create_subdag, nodes_creators=create_child_subdag)
    jobs = dag_jobs.build_dag(dags_list)
    dag_jobs_path = (Path.cwd() / "Logs/High-PT").absolute()
    dag_jobs_file = dags.write_dag(jobs, dag_jobs_path, dag_file_name=f"shower-detector-high-pt.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_jobs_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()
    print(f"Id of the job: {cluster_id}")
