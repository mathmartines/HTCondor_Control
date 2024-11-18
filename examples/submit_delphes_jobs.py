#! /usr/bin/env python3

"""Example on how to use the SubmitBuilder class"""

from HTCondor_Control.src.DagCreator import DagCreator
from htcondor import dags
import htcondor
from pathlib import Path


def delphes_job_template(
        arguments: str, log_name: str
):
    """Template for delphes jobs"""
    return htcondor.Submit(
        universe="vanilla",
        executable="./bash_scripts/launch_delphes.sh",
        arguments=arguments,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        # request_cpus=1,
        # request_memory="5GB",
        getenv=True,
        requirements='(Machine=="fmahep.if.usp.br")',
        when_to_transfer_output="ON_EXIT"
    )


if __name__ == "__main__":
    jobs_list = []  # stores the list of jobs
    # list of folders containning the simulations
    simulations_folders = ('c5psi4H2 c3psi4D2 c3W2H4T-c3W2H4T D4FT c3W2H4T cphi1T-c3W2H4T cBWT cphi1T-cphi1T '
                           'c2JW-cphi1 cphi1T-cBWT D4FT-c3W2H4T c2JW cBWT-c3W2H4T cphi1T cphi1T-D4FT D4FT-D4FT '
                           'cBWT-cBWT c2psi2H2D3 c2JW-c2JW D4FT-cBWT').split()

    # creating folders we wish to simulate
    # the key represents the name we use to identify the logs and the values the parameters for the executable
    folderpath = "/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/cms-monoe-13TEV/UniversalSMEFT_d8"

    # constructs all the jobs
    for eft_term in simulations_folders:
        for bin_index in range(1, 8):
            jobs_list.append(
                delphes_job_template(
                    arguments=f"{folderpath}/{eft_term}/bin_{bin_index}/Events/run_01 pythia8_events.hepmc "
                              f"atlas-monoe-events.root "
                              f"/data/01/martines/MG5_aMC_v3_1_1/PhD/DY/delphes_card_ATLAS_default.tcl",
                    log_name=f"/data/01/martines/hep_programs/HTCondor_Control/Logs/UniversalSMEFT/atlas-monoe-{eft_term}-{bin_index}-delphes"
                )
            )

    # creates the dag where only 5 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=20, jobs_list=jobs_list)
    dag = dag_creator.build_dag()

    # saves the dag file in the Logs folder
    dag_dir = (Path.cwd() / "Logs/UniversalSMEFT").absolute()
    dag_file = dags.write_dag(dag, dag_dir, dag_file_name="atlas-monoe-delphes.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
