from flytekit import conditional, workflow

from workflows.extract import extract_workflow, transform_workflow
from workflows.load import load_workflow


@workflow
def main():
    extract = extract_workflow()
    transform = transform_workflow()
    load = load_workflow()
    extract >> transform
    transform >> load


@workflow
def load():
    load = load_workflow()
