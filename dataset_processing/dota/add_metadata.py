import json
from typing import Optional

import pandas as pd
import typer

from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = typer.Typer()

RADIANT_TEAM = 2
DIRE_TEAM = 3

RADIANT_TEAM_OPENDOTA = 1
DIRE_TEAM_OPENDOTA = 2


def get_team(df: DataFrame, team: int) -> DataFrame:
    column_name = "radiant_team" if team == RADIANT_TEAM else "dire_team"
    return df.where(F.col("team") == team)\
        .groupBy("id", "team")\
        .agg(F.collect_set("hero_id"))\
        .withColumnRenamed("collect_set(hero_id)", column_name)\
        .select(F.col("id"), F.col(column_name))\
        .withColumn(column_name, F.concat_ws(",", column_name))


def add_time_bin(df: DataFrame, divisor: float = 60.0) -> DataFrame:
    # this will round both numbers in (-1;0) and [0,1) to 0
    return df.withColumn("time_bin", F.floor(F.col("time") / divisor))


def add_bracket_data(df: DataFrame, spark: SparkSession, bracket_data_path: Path) -> DataFrame:
    metadata_df = spark.read.csv(str(bracket_data_path), sep="\t", inferSchema=True, header=True).withColumnRenamed("match_id", "id")
    return df.join(metadata_df, on="id")


def add_role_data(df: DataFrame, spark: SparkSession, roles_json_path: Path, heros_json_path: Path, delimiter=",", add_role_names: bool = False):
    with open(roles_json_path, "r") as f:
        role_data = json.load(f)
    role_dict = {role["id"]: role["name"] for role in role_data}

    with open(heros_json_path, "r") as f:
        hero_data = json.load(f)

    if add_role_names:
        hero_role_mapping = {hero_info["id"]: [role_dict[role["roleId"]] for role in hero_info["roles"]]
                             for hero_info in hero_data.values()}
    else:
        hero_role_mapping = {hero_info["id"]: [role["roleId"] for role in hero_info["roles"]]
                             for hero_info in hero_data.values()}

    hero_ids = list(hero_role_mapping.keys())
    roles = [delimiter.join(map(lambda x: str(x), hero_role_mapping[hero_id])) for hero_id in hero_ids]
    role_df = pd.DataFrame({"hero_id": hero_ids, "roles": roles})
    role_df = spark.createDataFrame(role_df)
    return df.join(role_df, on="hero_id")


def filter_incomplete_matches(df: DataFrame) -> DataFrame:
    match_id_with_flag_df = df.select("id", "hero_id").dropDuplicates().groupBy("id").count().withColumn(
        "complete_flag", F.col("count") == 10
    ).drop("count")

    return df.join(match_id_with_flag_df, on="id").where(F.col("complete_flag") == True).drop("complete_flag")


@app.command()
def process(input_file_path: Path,
            output_base_path: Path,
            role_json_path: Path,
            hero_json_path: Path,
            bracket_data_path: Optional[Path] = None,
            threads: int = 8,
            opendota: bool = False,
            add_role_names: bool = False):

    spark = SparkSession.builder.appName("add team info")\
        .master(f"local[{threads}]")\
        .config("spark.driver.memory", "20g")\
        .config("spark.local.dir", ".tmp")\
        .getOrCreate()
    df = spark.read.parquet(str(input_file_path))

    df = add_time_bin(df)
    df = add_role_data(df, spark, role_json_path, hero_json_path, add_role_names=add_role_names)

    if bracket_data_path is not None:
        df = add_bracket_data(df, spark, bracket_data_path)

    if opendota:
        radiant_team_df = get_team(df, RADIANT_TEAM_OPENDOTA)
        dire_team_df = get_team(df, DIRE_TEAM_OPENDOTA)
    else:
        radiant_team_df = get_team(df, RADIANT_TEAM)
        dire_team_df = get_team(df, DIRE_TEAM)

    both_teams_df = radiant_team_df.join(dire_team_df, on="id")
    result_df = df.join(both_teams_df, on="id")

    wo_incomplete_matches_df = filter_incomplete_matches(result_df)

    df_final = wo_incomplete_matches_df.sort(F.col("id"), F.col("hero_id"), F.col("time"))

    input_file_name = input_file_path.name
    input_file_base_name = f"{input_file_name.split('.')[0]}"

    df_final.write.parquet(str(output_base_path / f"{input_file_base_name}.parquet"))
    df_final.coalesce(1).sort(F.col("id"), F.col("hero_id"), F.col("time")).write.csv(str(output_base_path / f"{input_file_base_name}.csv.gz"), compression="gzip", sep="\t", header=True)


if __name__ == "__main__":
    app()
