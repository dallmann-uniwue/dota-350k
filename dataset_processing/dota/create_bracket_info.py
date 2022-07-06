from typing import Optional, Any, Dict

import psycopg2
import typer
import json
import tqdm
import csv
from pathlib import Path

app = typer.Typer()


def get_rank(response: Dict[str, Any]) -> int:
    return int(response["rank"])


def get_bracket(response: Dict[str, Any]) -> int:
    return int(response["bracket"])


@app.command()
def run(dbname: str, host: str = "localhost", port: int = 5432, user: str = "dota", password: Optional[str] = None, fetch_size: int = 100, output_file_path: Path = "matches_metainfo.csv"):

    with psycopg2.connect(dbname=dbname,
                          port=port,
                          user=user,
                          host=host,
                          password=password) as connection:

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(match_id) FROM replay_info")
            num_matches = cursor.fetchall()[0][0]

        with connection.cursor("read_meta_info") as cursor:
            cursor.execute("SELECT match_id, raw FROM replay_info")

            with tqdm.tqdm(total=num_matches) as progress_bar, output_file_path.open("w") as output_file:
                csv_writer = csv.writer(output_file, dialect=csv.excel_tab)
                csv_writer.writerow(["match_id", "rank", "bracket"])

                results = cursor.fetchmany(fetch_size)

                while len(results) != 0:

                    rows = []
                    for result in results:
                        match_id = result[0]
                        response = json.loads(result[1])

                        rank = get_rank(response)
                        bracket = get_bracket(response)

                        rows.append([match_id, rank, bracket])

                    csv_writer.writerows(rows)
                    results = cursor.fetchmany(fetch_size)
                    progress_bar.update(fetch_size)


if __name__ == "__main__":
    app()
