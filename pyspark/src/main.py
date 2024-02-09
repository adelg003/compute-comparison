from argparse import ArgumentParser, Namespace
from pathlib import Path

from packs.completeness import completeness_test
from packs.unbalanced import unbalanced_test
from pyspark.sql import SparkSession  # type: ignore


def main():

    # Pull args from CLI
    args = parse_args()

    # Start Spark Session, with enough ram to nto die on us
    spark = SparkSession.builder.config("spark.driver.memory", "32g").getOrCreate()  # type: ignore
    # Have Spark play nice with decial datatypes
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    # https://kb.databricks.com/scala/spark-job-fail-parquet-column-convert

    # Read GL and TB data
    gl = spark.read.parquet(str(args.gl))  # type: ignore
    tb = spark.read.parquet(str(args.tb))  # type: ignore

    # Plan Lazy Unbalanced and Completeness JE Test
    unbalanced = unbalanced_test(gl)
    completeness = completeness_test(gl, tb)

    # Write results out
    print("Running Unbalanced test")
    unbalanced_path = args.output.joinpath("unbalanced.parquet")
    unbalanced.write.parquet(
        path=str(unbalanced_path),
        mode="overwrite",
    )

    print("Running Completeness test")
    completeness_path = args.output.joinpath("completeness.parquet")
    completeness.write.parquet(
        path=str(completeness_path),
        mode="overwrite",
    )


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--gl", type=Path)
    parser.add_argument("--tb", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
