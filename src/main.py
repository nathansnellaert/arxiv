from subsets_utils import DAG, validate_environment
from nodes import oai_harvest, papers


workflow = DAG({
    oai_harvest.run: [],
    papers.run: [oai_harvest.run],
})


def main():
    validate_environment()
    workflow.run()
    workflow.save_state()


if __name__ == "__main__":
    main()
