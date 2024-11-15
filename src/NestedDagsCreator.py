"""Creates Nested DAGs"""

import htcondor
from pathlib import Path
from htcondor import dags


def create_layer(dag: dags.DAG, name: str, job: htcondor.Submit):
    """"Creates a new Layer in the DAG"""
    return dag.layer(name=name, submit_description=job)


def create_subdag(dag: dags.DAG, name: str, job: Path):
    """Creates a subdag as a node of the dag"""
    return dag.subdag(name=name, dag_file=job)


def create_child_subdag(node: dags.BaseNode, name: str, job: Path):
    """Creates a subdag as a child of the node"""
    return node.child_subdag(name=name, dag_file=job)


def create_child_layer(node: dags.BaseNode, name: str, job: htcondor.Submit):
    """Creates a layer as a child of the node"""
    return node.child_layer(name=name, submit_description=job)


class NestedDags:
    """Creates DAGs that can be nested"""

    def __init__(self, max_number_jobs: int, initial_layer_creator, nodes_creators):
        # max number of jobs that can run at the same time
        self._max_jobs = max_number_jobs
        # specifies how the first layer must be created
        self._initial_layer_builder = initial_layer_creator
        # specifies how the sublayers must be created
        self._nodes_builder = nodes_creators
        # stores the structure of the graph
        self._layers = []

    def build_dag(self, jobs):
        """Creates the structure for the dag"""
        dag = dags.DAG()
        self._layers.clear()
        self.create_first_layer(jobs[:self._max_jobs], dag)
        # creates additional layer if needed
        if len(jobs) > self._max_jobs:
            for index in range(self._max_jobs, len(jobs), self._max_jobs):
                self.create_layer(jobs[index: index + self._max_jobs])
        return dag

    def create_first_layer(self, jobs, dag):
        """Creates the first layers in the graph - nodes without parents"""
        first_layer = [self._initial_layer_builder(dag, name, job) for (name, job) in jobs]
        # stores the layer
        self._layers.append(first_layer)

    def create_layer(self, jobs):
        """
        Creates a new layer to the graph.
        The parents nodes are taken from the previous layes
        """
        layer = [self._nodes_builder(self._layers[-1][index], name, job) for index, (name, job) in enumerate(jobs)]
        self._layers.append(layer)
