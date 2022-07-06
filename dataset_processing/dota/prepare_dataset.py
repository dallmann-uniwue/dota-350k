from pathlib import Path
from typing import Tuple

import typer
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, col, collect_list, asc

from util import count_matches_in_df, \
    load_non_purchaseable_items, \
    remove_non_purchaseable_items, \
    load_complete_dataset

app = typer.Typer()


@app.command()
def to_parquet(input_file: Path, output_file: Path):
    spark = SparkSession.builder.appName("DotA To Parquet Conversion").master("local[8]").getOrCreate()
    df = spark.read.csv(str(input_file), sep="\t", inferSchema=True, header=True)
    df.write.parquet(str(output_file))


def remove_unranked_games(df: DataFrame) -> DataFrame:
    return df.filter(col("lobby_type") == 7)


def remove_games_with_abandons(df: DataFrame) -> DataFrame:
    # build a DF that associates each match id with the highest leaver_status over all players, if a player abandons
    # for some reason this yields a value != 0.
    match_abandonment_state = df.groupBy(df["id"]).agg(max(df["leaver_status"])).withColumnRenamed(
        "max(leaver_status)", "match_leaver_status"
    )

    # join the match abandonment state to all entries for the match
    df_wo_leavers = df.join(match_abandonment_state, on="id")
    # select only entries from matches that are not affected by player abandonment.
    df_wo_leavers = df_wo_leavers.filter(df_wo_leavers["match_leaver_status"] == 0)

    return df_wo_leavers


def remove_matches_with_short_sessions(df: DataFrame, min_session_length: int) -> DataFrame:
    # calculate the minimum session length for each match
    entries_by_hero = df.groupBy(col("id"), col("hero_id")).count().select(col("id"), col("count"))
    match_min_session_length = entries_by_hero.groupBy(col("id")).agg(min(col("count"))).withColumnRenamed(
        "min(count)", "min_session_length"
    )

    # join the information to the original table and then select only matches with a minimum session length > min_session_length
    df_with_min_session_length = df.join(match_min_session_length, on="id")
    df_wo_short_sessions = df_with_min_session_length.filter(col("min_session_length") > min_session_length)
    return df_wo_short_sessions


def get_maximum_session_length(df: DataFrame, high_quantile: float = 0.99) -> int:
    entries_by_hero = df.groupBy(col("id"), col("hero_id")).count()
    boundaries = entries_by_hero.approxQuantile("count", [high_quantile], 0.001)
    return int(boundaries[0])


def find_boundary(df: DataFrame, low_quantile: float = 0.025, high_quantile: float = 0.975) -> Tuple[int, int]:
    boundaries = df.approxQuantile("duration", [low_quantile, high_quantile], 0.001)

    return int(boundaries[0]), int(boundaries[1])


def select_matches_in_boundary(df: DataFrame, low: int, high: int) -> DataFrame:
    return df.filter((col("duration") <= high) & (col("duration") >= low))


@app.command()
def process(input_file: Path,
            output_path: Path,
            min_session_length: int = 2,
            low_quantile: float = 0.025,
            high_quantile: float = 0.975,
            drop_unranked: bool = True,
            items_file: Path = Path("resources/items.json")):

    spark = SparkSession.builder.appName("DotA Preprocess")\
        .master("local[8]")\
        .config("spark.driver.memory", "20g") \
        .config("spark.local.dir", ".tmp") \
        .getOrCreate()
    df = spark.read.parquet(str(input_file))

    num_matches_total = count_matches_in_df(df)

    # only consider ranked games
    if drop_unranked:
        df = remove_unranked_games(df)
        num_ranked_matches = count_matches_in_df(df)
    else:
        num_ranked_matches = num_matches_total

    # remove items that can't be purchased but show up sometimes (neutral items, aegis)
    non_purchaseable_item_ids = load_non_purchaseable_items(items_file)
    df = remove_non_purchaseable_items(df, non_purchaseable_item_ids)

    df_games_wo_abandons = remove_games_with_abandons(df)
    num_matches_wo_abandons = count_matches_in_df(df_games_wo_abandons)
    num_matches_wi_abandons = num_ranked_matches - num_matches_wo_abandons

    df_wo_short_sessions = remove_matches_with_short_sessions(df_games_wo_abandons, min_session_length)
    num_matches_wo_short_sessions = count_matches_in_df(df_wo_short_sessions)
    num_matches_wi_short_sessions = num_matches_wo_abandons - num_matches_wo_short_sessions

    low, high = find_boundary(df_wo_short_sessions, low_quantile=low_quantile, high_quantile=high_quantile)
    df_pruned = select_matches_in_boundary(df_wo_short_sessions, low, high)

    num_matches_in_boundary = count_matches_in_df(df_pruned)
    num_matches_outside_boundary = num_matches_wo_short_sessions - num_matches_in_boundary

    print(f"#Matches in Dataset: {num_matches_total}")
    if drop_unranked:
        print(f"#Ranked Matches: {num_ranked_matches}")
    print(f"#Matches with Abandon: {num_matches_wi_abandons} ({(num_matches_wi_abandons / num_ranked_matches):.4f})%")
    print(f"#Matches with Abandons removed: {num_matches_wo_abandons}")
    print(f"#Matches with Short Sessions: {num_matches_wi_short_sessions} ({num_matches_wi_short_sessions / num_matches_wo_abandons:.2f}%")
    print(f"#Matches with Short Sessions removed: {num_matches_wo_short_sessions}")
    print(f"Removing very short and long games by restricting to matches within the {low_quantile} and {high_quantile} quantiles.")
    print(f"Low Boundary: {low}")
    print(f"High Boundary: {high}")
    print(f"#Matches within Quantiles: {num_matches_in_boundary}")

    df_final = df_pruned.drop("leaver_status").drop("match_leaver_status").drop("min_session_length").coalesce(numPartitions=1)
    df_final = df_final.sort(asc("id"), asc("team"), col("hero_id"), asc("time"))

    input_file_name = input_file.name
    input_file_base_name = f"{input_file_name.split('.')[0]}-processed"

    df_final.write.parquet(str(output_path / f"{input_file_base_name}.parquet"))

    csv_output_path = output_path / f"{input_file_base_name}.csv"
    df_final.write.csv(str(csv_output_path), sep="\t", header=True)

    # rename output csv
    output_csv_files = list(csv_output_path.glob("part-*"))
    if len(output_csv_files) > 1:
        raise Exception("There should be only one file.")
    csv_output_file_path = output_path / "dota.csv"
    output_csv_files[0].rename(csv_output_file_path)


def match_id_boundaries(df: DataFrame,
                        train_ratio: float,
                        validation_ratio: float) -> Tuple[int, int]:

    # get all match ids
    match_ids = df.agg(collect_list("id")).collect()[0][0]

    num_matches = count_matches_in_df(df)

    # determine highest train match id
    num_train_matches = int(train_ratio * num_matches)
    train_matches = match_ids[:num_train_matches]
    max_train_id = train_matches[-1]

    # determine highest validation match id
    num_val_matches = int(validation_ratio * num_matches)
    val_matches = match_ids[num_train_matches:(num_train_matches + num_val_matches)]
    max_validation_id = val_matches[-1]

    return max_train_id, max_validation_id


def write_split(df: DataFrame, output_path: Path, split_name: str):
    # after coalescing to one partition, we need to make sure that the order is still correct!
    coalesced_df = df.coalesce(numPartitions=1).orderBy(col("id"), col("hero_id"), col("time"))

    coalesced_df.write.parquet(str(output_path / f"{split_name}.parquet"))

    spark_output_path = output_path / f"{split_name}.csv"
    coalesced_df.write.csv(str(spark_output_path), sep="\t", header=True)

    csv_file = list(spark_output_path.glob("part-*csv"))[0]
    csv_file.rename(output_path / f"dota.{split_name}.csv")


@app.command()
def split(input_file: Path,
          output_path: Path,
          training_ratio: float = 0.94,
          validation_ratio: float = 0.01,
          test_ratio: float = 0.05):

    spark = SparkSession.builder.appName("DotA Split")\
        .master("local[8]")\
        .config("spark.driver.memory", "20g") \
        .config("spark.local.dir", ".tmp") \
        .getOrCreate()
    df = spark.read.parquet(str(input_file))

    # order matches by id, then select the first k as training match,
    # the following n as validation and the remaining m as test.
    df = df.orderBy(col("id"), col("hero_id"), col("time"))
    match_ids = df.select(col("id")).distinct().orderBy(col("id"))

    last_train_match_id, last_validation_match_id = match_id_boundaries(match_ids,
                                                                        training_ratio,
                                                                        validation_ratio)

    train_samples = df.filter(col("id") <= last_train_match_id)
    train_and_val_samples = df.filter(col("id") <= last_validation_match_id)

    val_samples = df.filter((col("id") > last_train_match_id) & (col("id") <= last_validation_match_id))
    test_samples = df.filter(col("id") > last_validation_match_id)

    split_path = output_path / f"split-{training_ratio}_{validation_ratio}_{test_ratio}"
    split_path.mkdir(parents=True, exist_ok=True)

    write_split(train_samples, split_path, "train")
    write_split(train_and_val_samples, split_path, "train_validation")
    write_split(val_samples, split_path, "validation")
    write_split(test_samples, split_path, "test")


@app.command()
def max_session_length(input_file: Path):
    spark = SparkSession.builder.appName("DotA max session length").master("local[12]").config("spark.driver.memory", "12g").getOrCreate()
    df = spark.read.parquet(str(input_file))
    high = get_maximum_session_length(df, 0.995)
    print(f"{high}")


@app.command()
def schema(split_dir: Path):
    spark = SparkSession.builder.appName("DotA Schema").master("local[12]").config("spark.driver.memory", "8g").getOrCreate()
    df = load_complete_dataset(spark, split_dir)
    df.show()


if __name__ == "__main__":
    app()


