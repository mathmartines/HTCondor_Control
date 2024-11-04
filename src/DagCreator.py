"""
Class responsible to create the Dag file.
It assumes that all the jobs are independent, but only a fixed number of them
can be run at the same time. If the number of jobs running is equal to max_number_jobs,
then only one finishes the next can start.
This can be done by creating a retangular grid, where each layer contains the maximum
number of jobs as nodes, and each node contains only one child.
"""

from typing import List
import htcondor
from htcondor import dags


class DagCreator:
    """Creates a topology where only a fixed number of jobs can be run at time."""

    def __init__(self, max_number_jobs: int, jobs_list: List[htcondor.Submit]):
        self._max_number_jobs = max_number_jobs  # maximum number of jobs that we allow to run in parallel
        self._jobs_list = jobs_list  # list with all the jobs that we need to run
        self._layers = []  # stores the structure of the graph
        self._dag = dags.DAG()

    def build_dag(self):
        """Creates the structure for the dag"""
        self.create_first_layer()
        # creates additional layer if needed
        if len(self._jobs_list) > self._max_number_jobs:
            for index in range(self._max_number_jobs, len(self._jobs_list), self._max_number_jobs):
                self.create_layer(self._jobs_list[index: index + self._max_number_jobs])
        return self._dag

    def create_first_layer(self):
        """Creates the first layers in the graph - nodes without parents"""
        first_layer = []

        for job in self._jobs_list[:self._max_number_jobs]:
            first_layer.append(self._dag.layer(
                name=job['log'].split(".")[0], submit_description=job
            ))
        # stores the layer
        self._layers.append(first_layer)

    def create_layer(self, jobs):
        """
        Creates a new layer to the graph.
        The parents nodes are taken from the previous layes
        """
        layer = []
        for index, job in enumerate(jobs):
            layer.append(
                self._layers[-1][index].child_layer(
                    name=job['log'].split(".")[0], submit_description=job
                )
            )
        self._layers.append(layer)
