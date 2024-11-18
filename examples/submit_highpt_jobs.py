"""Generates the MG5 runs"""

from pathlib import Path
from ServerScripts.src.Commands import list_folders
from ScriptCreatorMG5.src.ScriptCommand import MG5CardCommands, FlavorWilsonCoefs, generate_mg5_script
from HTCondor_Control.src.DagCreator import DagCreator
import htcondor
from htcondor import dags

invariant_masses = list(map(float, "150 200 250 300 350 400 450 500 600 700 800 900 1000 1150 15000".split()))
quark_indices = {"u": 1, "c": 2, "d": 1, "s": 2, "b": 3}


def get_simulation_info(folderpath: str):
    """Extract the information about the simulation"""
    *_, initial_flavor, eft_terms, bin_number = str(folderpath).split("/")
    # information about the flavors
    initial_quarks, *_ = initial_flavor.split("_")
    wilson_coefs = set(eft_terms.split("-"))
    _, bin_number = bin_number.split("_")
    # returns all information we need for the run
    return {
        "flavor-indices": (quark_indices[initial_quarks[0]], quark_indices[initial_quarks[1]]),
        "coefs": list(wilson_coefs), "path": folderpath.__str__(),
        "bin-edges": (invariant_masses[int(bin_number) - 1], invariant_masses[int(bin_number)]),
        "log-name": f"{initial_flavor}-{eft_terms}-{bin_number}"
    }


def mg5_job_template(input_files: str, log_name: str) -> htcondor.Submit:
    """
    Creates the submit job for MG5.

    :param input_files: files needed to run
    :param log_name: name to give to the log file
    """
    return htcondor.Submit(
        universe="vanilla",
        executable="./bash_scripts/submit_mg5.sh",
        arguments=input_files,
        log=f"{log_name}.log",
        output=f"{log_name}.out",
        error=f"{log_name}.err",
        should_transfer_files="YES",
        request_cpus=5,
        request_memory=4096,
        RunAsOwner=True,
        getenv=True,
        requirements='(Machine=="fmahep.if.usp.br")',
        when_to_transfer_output="ON_EXIT",
    )


if __name__ == "__main__":
    # list all the directories we need to run the simulations
    simulations_folders = list_folders(Path("/data/01/martines/MG5_aMC_v3_1_1/PhD/High-PT/atlas-ditau-13TEV"))

    # default commands
    commands = MG5CardCommands(nevents=50000, ptl=0, etal=2.8, drll=0.0, pdlabel="lhapdf", lhaid=91500, use_syst=False)
    # command to generate the Wilson coefficients
    wc_commands = FlavorWilsonCoefs(all_coefs=["C1lq", "C3lq"], number_indices_pairs=2)
    commands.add_command(wc_commands)

    # holds all the jobs to run
    jobs_list = []
    logs_folder = "/data/01/martines/hep_programs/HTCondor_Control/Logs/High-PT"
    # creates the script for the run and runs the simulation
    for simulation in map(get_simulation_info, simulations_folders):
        print(f"INFO: creating script for simulation {simulation['path']}")
        # invariant mass cuts
        inv_mass_cuts = MG5CardCommands(mmll=simulation["bin-edges"][0], mmllmax=simulation["bin-edges"][1])
        commands.add_command(inv_mass_cuts)
        # defines the wilson coefficients
        wc_commands.turn_coefs_on(simulation["coefs"], 3, 3, *simulation["flavor-indices"])
        # generates the script
        mg5_script = generate_mg5_script(foldername=simulation["path"], commands=commands)
        # remove the invariant mass cuts
        commands.remove_command(inv_mass_cuts)
        # saves the script for the run
        with open(f"{simulation['path']}/script_mg5.txt", "w") as script_file:
            script_file.write(mg5_script)
        print(f"INFO: script done")
        # creates the job
        jobs_list.append(
            mg5_job_template(
                input_files=f"{simulation['path']}/script_mg5.txt",
                log_name=f"{logs_folder}/{simulation['log-name']}"
            )
        )

    # creates a dag where only 30 jobs are allowed to run at the same time
    dag_creator = DagCreator(max_number_jobs=5, jobs_list=jobs_list)
    dag = dag_creator.build_dag()

    dag_file = dags.write_dag(dag, logs_folder, dag_file_name="mg5.dag")

    # submit the job
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {'force': True})
    schedd = htcondor.Schedd()
    cluster_id = schedd.submit(dag_submit).cluster()

    print(f"Id of the job: {cluster_id}")
