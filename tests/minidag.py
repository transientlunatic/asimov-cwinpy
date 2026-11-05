"""
Run the jobs of a DAG one after another, as HTCondor would, without a pool.

Enough of DAGMan for the DAGs that cwinpy writes: a job per node, its arguments and file
transfers from the ``VARS`` of the node. It does not follow ``PARENT`` and ``CHILD``,
retry, or write a rescue file. Tests say what DAGMan would have said.
"""

import os
import re
import shlex
import shutil
import subprocess

_VARS = re.compile(r'(\w+)="([^"]*)"')


def read_dag(path):
    """
    The nodes of a DAG.

    Returns
    -------
    dict
        ``{node: {"submit": file, **VARS}}``, in the order of the file.
    """
    nodes = {}
    directory = os.path.dirname(path)
    with open(path) as handle:
        for line in handle:
            words = line.split(None, 2)
            if len(words) >= 3 and words[0] == "JOB":
                nodes[words[1]] = {"submit": os.path.join(directory, words[2].strip())}
            elif len(words) >= 3 and words[0] == "VARS":
                nodes[words[1]].update(dict(_VARS.findall(words[2])))
    return nodes


def submit_value(path, key):
    """A value from a submit file."""
    with open(path) as handle:
        for line in handle:
            name, _, value = line.partition("=")
            if name.strip() == key:
                return value.strip()
    return None


def run_node(node, scratch, env=None):
    """
    Run one node: copy its input files into a scratch directory, run its executable
    there, write its output and error files, and copy its output files back.

    Returns
    -------
    int
        The exit code of the job.
    """
    work = os.path.join(scratch, re.sub(r"\W", "_", node["submit"] + node.get("ARGS", "")))
    os.makedirs(work, exist_ok=True)
    initial = node["INITIALDIR"]
    for name in filter(None, node.get("TRANSFERINPUT", "").split(",")):
        shutil.copy(os.path.join(initial, name), work)

    executable = submit_value(node["submit"], "executable")
    for key in ("OUTPUTFILE", "ERRORFILE"):
        os.makedirs(os.path.dirname(node[key]), exist_ok=True)
    with open(node["OUTPUTFILE"], "w") as out, open(node["ERRORFILE"], "w") as err:
        done = subprocess.run(
            [executable, *shlex.split(node["ARGS"])], cwd=work, stdout=out, stderr=err, env=env
        )
    for name in filter(None, node.get("TRANSFEROUTPUT", "").split(",")):
        produced = os.path.join(work, name)
        if os.path.exists(produced):
            shutil.copy(produced, os.path.join(initial, name))
    return done.returncode


def run_dag(path, scratch, env=None):
    """Run every node of a DAG. Returns ``{node: exit code}``."""
    return {name: run_node(node, scratch, env) for name, node in read_dag(path).items()}
