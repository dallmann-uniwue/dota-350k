from pathlib import Path

import typer
import pandas as pd
import numpy as np


MATCH_FILE_NAME = "match.csv"
PURCHASE_LOG_FILE_NAME = "purchase_log.csv"
PLAYERS_FILE_NAME = "players.csv"
ITEMS_FILE_NAME = "item_ids.csv"

app = typer.Typer()


def fix_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses the `start_time` information together with `time` in `df` to calculate the exact timestamp when
    an item was bought and adds this information to the `df`.

    :param df: a DataFrame with purchases for every game and hero.

    :return: a DataFrame with purchases for every game and hero that contains a `timestamp` column.
    """
    # note that this is not the "real" time an item is bought, since the start_time is likely the time when the game
    # was initialized on the game coordinator.
    start_time_column = df["start_time"]
    timestamp_column = start_time_column + (df["time"] + 90)  # game clock start at -1:30
    df["timestamp"] = pd.to_datetime(timestamp_column, unit="s")
    df["start_time"] = pd.to_datetime(start_time_column, unit="s")

    return df


def add_team(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column `team` that contains what team the player belongs to. `1` for radiant, `2` for dire. The information
    is derived from the `player_slot` column.

    :param df: a DataFrame with purchases for every game and hero.
    :return: the DataFrame with team information added.
    """
    conditions = [
        (df["player_slot"] <= 127),  # radiant player slots
        (df["player_slot"] > 127)  # dire player slots
    ]
    team_values = [1, 2]  # radiant = 1, dire = 2
    team_column = np.select(conditions, team_values)
    df["team"] = team_column

    return df


def add_start_item_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a `start_item` column that shows whether the item was bought before the players spawned.
    We can't infer any order for these items.

    :param df: a DataFrame with purchases for every game and hero.
    :return: a DataFrame with `start_time` column added.
    """
    df["is_start_item"] = (df["time"] == -90)
    return df


def add_winner_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the `winner` column that shows what team won the match. `0` radiant, `1` dire.

    :param df: a DataFrame with purchases for every game and hero.
    :return: a DataFrame with `winner` column added.
    """
    df["winner"] = df["radiant_win"]
    return df


def select_and_reorder(df: pd.DataFrame) -> pd.DataFrame:
    return df[["match_id", "start_time", "duration", "winner", "leaver_status", "hero_id", "team", "timestamp", "time",
               "item_id", "is_start_item"]]


@app.command()
def process(base_directory: Path, output_file: Path, file_type: str = "csv"):
    if file_type not in ["csv", "parquet"]:
        raise RuntimeError(f"Specify file_type = csv or file_type = parquet.")

    match_df = pd.read_csv(f"{base_directory}/{MATCH_FILE_NAME}",
                           usecols=["match_id", "duration", "radiant_win", "start_time"])
    players_df = pd.read_csv(f"{base_directory}/{PLAYERS_FILE_NAME}",
                             usecols=["match_id", "hero_id", "player_slot", "leaver_status", "account_id"])
    purchaselog_df = pd.read_csv(f"{base_directory}/{PURCHASE_LOG_FILE_NAME}")

    mp = pd.merge(match_df, players_df, on=["match_id"])
    mpp = pd.merge(mp, purchaselog_df, on=["match_id", "player_slot"])

    dataset = mpp
    dataset = fix_timestamp(dataset)
    dataset = add_team(dataset)
    dataset = add_start_item_flag(dataset)
    dataset = add_winner_flag(dataset)
    dataset = select_and_reorder(dataset)

    # Reformat to match current schema
    dataset = dataset.rename({"match_id": "id"}, axis=1)
    dataset = dataset.drop(columns=["timestamp"])
    # Artificially set game_mode and lobby_type to ranked all pick
    dataset["game_mode"] = 22
    dataset["lobby_type"] = 7
    # Artificially insert player id
    dataset["player_id"] = -1
    # Convert start_time to epoch
    dataset["start_time"] = (dataset["start_time"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")
    # Reorder columns
    dataset = dataset[["id", "game_mode", "lobby_type", "start_time", "duration", "winner", "leaver_status", "hero_id", "team",
                       "player_id", "time", "item_id", "is_start_item"]]

    if file_type == "csv":
        dataset.to_csv(f"{output_file}", sep="\t", index=False)
    else:
        dataset.to_parquet(f"{output_file}", index=False)


if __name__ == "__main__":
    app()
