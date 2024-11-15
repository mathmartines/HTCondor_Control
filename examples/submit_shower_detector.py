"""Submits jobs where each node is a dag"""

from subprocess import Popen
from pathlib import Path
from HTCondor_Control.src.NestedDagsCreator import (create_layer, create_child_layer, create_subdag,
                                                    create_child_subdag, NestedDags)
from HTCondor_Control.examples.submit_pythia8 import pythia8_jobs_template
from HTCondor_Control.examples.submit_delphes_jobs import delphes_job_template
from htcondor import dags
import htcondor


if __name__ == "__main__":
    dags_list = []  # stores the list of dags we want to simulate
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-monoe-13TEV/UniversalSMEFT_d8"  # root folder
    # folders we need to simulate
    simulations_folders = "c2JW c2JW-c2JW c2JW-cphi1 c2psi2H2D3 c5psi4H2 c3psi4D2".split()
    # simulations_folders = ["c2JW"]

    # dag creater to generate the pythia + delphes jobs for each simulation
    dag_shower_detector = NestedDags(max_number_jobs=1, initial_layer_creator=create_layer,
                                     nodes_creators=create_child_layer)

    # path to the logs folder
    log_path = "/data/01/martines/hep_programs/HTCondor_Control/Logs/UniversalSMEFT"
    # creates the dags
    for eft_term in simulations_folders:
        # gives permission to write inside the folder
        process = Popen(['chmod', '-R', '777', str(f"{folderpath}/{eft_term}")])
        for bin_index in range(1, 8):
            # Jobs for each simulation
            pythia_job = pythia8_jobs_template(
                    arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01",
                    input_files=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/tag_3_pythia8.cmd, "
                                f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/unweighted_events.lhe.gz",
                    log_name=f"{log_path}/{eft_term}-{bin_index}-pythia"
                )
            delphes_job = delphes_job_template(
                arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01 pythia8_events.hepmc "
                          f"delphes_events.root "
                          f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_CMS_monolepton.tcl",
                input_files=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01/pythia8_events.hepmc, "
                            f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_CMS_monolepton.tcl",
                output_files="delphes_events.root",
                log_name=f"{log_path}/{eft_term}-{bin_index}-delphes"
            )
            # Creates the dag for the two jobs
            combined_job = dag_shower_detector.build_dag(
                [(f"{log_path}/{eft_term}-{bin_index}-shower",  pythia_job),
                 (f"{log_path}/{eft_term}-{bin_index}-detector", delphes_job)]
            )

            # saves the dag file in the Logs folder
            dag_file_path = (Path.cwd() / "Logs/UniversalSMEFT").absolute()
            dag_file = dags.write_dag(combined_job, dag_file_path, dag_file_name=f"{eft_term}-{bin_index}.dag")
            # Adds the job to the list pf jobs
            dags_list.append((f"{log_path}/{eft_term}-{bin_index}", dag_file))

    # builds the graph with all the jobs
    dag_jobs = NestedDags(max_number_jobs=20, initial_layer_creator=create_subdag, nodes_creators=create_child_subdag)
    jobs = dag_jobs.build_dag(dags_list)
    dag_jobs_path = (Path.cwd() / "Logs/UniversalSMEFT").absolute()
    dag_jobs_file = dags.write_dag(jobs, dag_jobs_path, dag_file_name=f"showr-detector.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_jobs_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()
    print(f"Id of the job: {cluster_id}")

