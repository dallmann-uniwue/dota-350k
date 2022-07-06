def count_matches_in_df(df: DataFrame) -> int:
    return df.select(df['id']).distinct().count()

def load_non_purchaseable_items(file_path: Path) -> List[int]:
    import json
    with file_path.open("r") as items_file:
        items_json = json.load(items_file)
        non_purchaseable_items_ids = []
        for id, item_info in items_json.items():
            item_id = int(id)
            if "stat" in item_info and item_info["stat"]["isPurchaseable"] == False:
                non_purchaseable_items_ids.append(item_id)
        return non_purchaseable_items_ids


def remove_non_purchaseable_items(dataset: DataFrame, non_purchaseable_items: List[int]) -> DataFrame:
    return dataset.filter(~col("item_id").isin(non_purchaseable_items))

def load_complete_dataset(spark: SparkSession, split_directory: Path) -> DataFrame:
    split_names = ["train.parquet", "val.parquet", "test.parquet"]
    dfs = [spark.read.parquet(str(split_directory / d)) for d in split_names]
    df = dfs[0]
    for other in dfs[1:]:
        df = df.union(other)

    return df