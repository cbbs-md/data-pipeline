"""<please add content here>"""

import click
# from datalad.distribution.dataset import require_dataset

# default call from datalad is of the form
# interpreter {script} {ds} {arguments}


@click.command()
@click.option("dataset", help="The dataset to operate on")
def main(dataset: str):
    print("Worked!", dataset)


if __name__ == "__main__":
    main()
