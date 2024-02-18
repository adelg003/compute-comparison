from argparse import ArgumentParser, Namespace
from pathlib import Path

import dask

# Set current dask quey plan and hove disk spill larger files to local folder
dask.config.set({"dataframe.query-planning": True})  # type: ignore
dask.config.set({"temporary-directory": "dask_tmp"})  # type: ignore

from dask.base import compute  # type: ignore
from dask.dataframe.core import DataFrame  # type: ignore
from dask.dataframe.io.parquet.core import read_parquet  # type: ignore
from dask.distributed import Client  # type: ignore
from packs.completeness import completeness_test  # type: ignore
from packs.unbalanced import unbalanced_test  # type: ignore


def main():

    # Pull args from CLI
    args = parse_args()

    # Start DASK
    client = Client()
    print(client)

    # Read GL and TB data
    gl: DataFrame = read_parquet(args.gl)  # type: ignore
    tb: DataFrame = read_parquet(args.tb)  # type: ignore

    # Plan Lazy Unbalanced and Completeness JE Test
    print("Preping for Unbalanced test")
    unbalanced = unbalanced_test(gl)
    print("Preping for Completeness test")
    completeness = completeness_test(gl, tb)

    # How we will write the results out
    unbalanced_parquet = unbalanced.to_parquet(
        path=args.output.joinpath("unbalanced.parquet/"),
        overwrite=True,
        compute=False,
    )

    completeness_parquet = completeness.to_parquet(
        path=args.output.joinpath("completeness.parquet/"),
        overwrite=True,
        compute=False,
    )

    # Run completeness and unbalanced test at the same time
    print("Running test")
    compute(
        unbalanced_parquet,
        completeness_parquet,
    )

    # Shut DASK down
    client.shutdown()


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--gl", type=Path)
    parser.add_argument("--tb", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
