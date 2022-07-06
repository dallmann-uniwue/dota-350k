import typer

from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = typer.Typer()


def filter_incomplete_matches(df: DataFrame) -> DataFrame:
    match_id_with_flag_df = df.select("id", "hero_id").dropDuplicates().groupBy("id").count().withColumn(
        "complete_flag", F.col("count") == 10
    ).drop("count")

    return df.join(match_id_with_flag_df, on="id").where(F.col("complete_flag") == True).drop("complete_flag")


@app.command()
def process(input_file_path: Path,
            output_base_path: Path,
            threads: int = 8):

    spark = SparkSession.builder.appName("add team info").master(f"local[{threads}]").config("spark.driver.memory", "12g").getOrCreate()
    df = spark.read.parquet(str(input_file_path))
    df_only_complete_matches = filter_incomplete_matches(df)

    input_file_name = input_file_path.name
    input_file_base_name = f"{input_file_name.split('.')[0]}"

    df_only_complete_matches.write.parquet(str(output_base_path / f"{input_file_base_name}.parquet"))
    df_only_complete_matches.coalesce(1).write.csv(str(output_base_path / f"{input_file_base_name}.csv.gz"), compression="gzip", sep="\t", header=True)


if __name__ == "__main__":
    app()
