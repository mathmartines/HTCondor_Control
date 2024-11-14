"""Generates the Pythia runs"""


from pathlib import Path
from ServerScripts.src.Commands import list_folders
import shutil
from subprocess import Popen
from HTCondor_Control.src.DagCreator import DagCreator
import htcondor
from htcondor import dags


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
        RunAsOwner=True,
        getenv=True,
        # requirements='(Machine!="fmahep.if.usp.br") && (Machine!="fmahep02.if.usp.br")',
        when_to_transfer_output="ON_EXIT",
    )


if __name__ == "__main__":
    # list all the directories we need to run the simulations
    simulations_folders = list_folders(Path("/data/01/martines/MG5_aMC_v3_1_1/PhD/HighPT/atlas-ditau-13TEV"))

    # holds all the jobs to run
    jobs_list = []
    logs_folder = "/data/01/martines/hep_programs/HTCondor_Control/Logs/HighPT"
    # path to the folder to get the pythia files
    pythia_files = ("/data/01/martines/MG5_aMC_v3_1_1/PhD/DY"
                    "/cms-dielectron-13TEV/UniversalSMEFT_d8/SM/bin_1/Events/run_01")

    # creates the script for the run and runs the simulation
    for simulation in simulations_folders:
        print(f"INFO: Creating files to run shower for folder {simulation}")
        # folder to run pythia
        pythia_folder = simulation / "Events/run_01_shower"
        pythia_folder.mkdir(exist_ok=True)
        # copy the files need for the shower
        shutil.copy(simulation / "Events/run_01/unweighted_events.lhe.gz", pythia_folder)
        shutil.copy(f"{pythia_files}/run_shower.sh", pythia_folder)
        shutil.copy(f"{pythia_files}/tag_3_pythia8.cmd", pythia_folder)
        # gives permission to write inside the folder
        process = Popen(['chmod', '-R', '777', str(pythia_folder)])
        # name for the log file
        *_, initial_flavor, eft_terms, bin_number = str(simulation).split("/")
        log_file = f"{initial_flavor}-{eft_terms}-{bin_number}-pythia"
        # creates the job
        jobs_list.append(
            pythia8_jobs_template(
                arguments=str(pythia_folder),
                input_files=f"{pythia_folder}/tag_3_pythia8.cmd, "
                            f"{pythia_folder}/unweighted_events.lhe.gz",
                log_name=f"{logs_folder}/{log_file}"
            )
        )

    # creates a dag where only 30 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=30, jobs_list=jobs_list)
    dag = dag_creator.build_dag()
    dag_file = dags.write_dag(dag, logs_folder, dag_file_name="highpt_pythia.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
