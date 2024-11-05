"""Defines the class to handle the creation of the Submit objects"""

import htcondor
from typing import Dict, List


class SubmitBuilder:
    """Responsible to create a list of jobs to be submited to the server"""

    def __init__(self, executable: str, jobs: Dict[str, Dict[str, List]], logs_folderpah: str, requirements: str):
        self._exectuble = executable  # stores the path of the executable file
        self._jobs = jobs  # stores a dict, where the keys represent one job and the values a dict with the arguments
        self._logs_folderpath = logs_folderpah  # path to where the log files must be saved
        self._requirements = requirements

    def create_submission_list(self) -> List[htcondor.Submit]:
        """Creates the list with all the Submit objects"""
        submission_list = []

        for sub_name, args_list in self._jobs.items():
            submission_list.append(
                htcondor.Submit(
                    universe="vanilla",
                    executable=self._exectuble,
                    arguments=" ".join(args_list["args"]),
                    transfer_input_files=args_list['transfer_input_files'],
                    log=f"{self._logs_folderpath}/{sub_name}.log",
                    output=f"{self._logs_folderpath}/{sub_name}.out",
                    error=f"{self._logs_folderpath}/{sub_name}.err",
                    should_transfer_files="YES",
                    getenv=True,
                    request_cpus=10,
                    requirements=self._requirements,
                    request_memory="4GB",
                    # transfer_output_files="unweighted_events.lhe.gz, RunWeb",
                    when_to_transfer_output="ON_EXIT",
                    # transfer_output_remaps=f"'unweighted_events.lhe.gz=f"
                )
            )

        return submission_list

