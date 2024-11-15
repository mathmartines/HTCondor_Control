"""Generates the Pythia runs"""


from pathlib import Path
from ServerScripts.src.Commands import list_folders
from HTCondor_Control.examples.submit_delphes_jobs import delphes_job_template
from HTCondor_Control.src.DagCreator import DagCreator
import htcondor
from htcondor import dags


if __name__ == "__main__":
    # list all the directories we need to run the simulations
    simulations_folders = list_folders(Path("/data/01/martines/MG5_aMC_v3_1_1/PhD/HighPT/atlas-ditau-13TEV"))

    # holds all the jobs to run
    jobs_list = []
    logs_folder = "/data/01/martines/hep_programs/HTCondor_Control/Logs/HighPT"

    # creates the script for the run and runs the simulation
    for simulation in simulations_folders:
        # folder to run pythi
        # name for the log file
        *_, initial_flavor, eft_terms, bin_number = str(simulation).split("/")
        log_file = f"{initial_flavor}-{eft_terms}-{bin_number}-delphes"
        # creates the job
        jobs_list.append(
            delphes_job_template(
                arguments=f"{simulation}/Events/run_01_shower pythia8_events.hepmc "
                          f"delphes_events.root "
                          f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_ta_ta_ATLAS_2002_12223.dat",
                input_files=f"{simulation}/Events/run_01_shower/pythia8_events.hepmc, "
                            f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_ta_ta_ATLAS_2002_12223.dat",
                output_files="delphes_events.root",
                log_name=f"/data/01/martines/hep_programs/HTCondor_Control/Logs/HighPT/{log_file}-delphes"
            )
        )

    # creates a dag where only 30 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=20, jobs_list=jobs_list)
    dag = dag_creator.build_dag()
    dag_file = dags.write_dag(dag, logs_folder, dag_file_name="highpt_delphes.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
