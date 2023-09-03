import duckdb
import os

from flytekit import task, workflow, map_task
from typing import Dict


CREATE_FROM_CSV_QUERY = """
CREATE TABLE {table_name} AS
SELECT *
FROM read_csv_auto('{filename}')
""".strip()

CREATE_FROM_JSON_QUERY = """
CREATE TABLE {table_name} AS
SELECT *
FROM read_json_auto('{filename}')
""".strip()

CREATE_FROM_PARQUET_QUERY = """
CREATE TABLE {table_name} AS
SELECT *
FROM read_parquet('{filename}')
""".strip()

CREATE_TEMPLATE_BY_FORMAT = {
    "csv": CREATE_FROM_CSV_QUERY,
    "json": CREATE_FROM_JSON_QUERY,
    "parquet": CREATE_FROM_PARQUET_QUERY
}

DATABASE = "database.db"

TABLES_TO_CREATE = [
    {
        "name": "player_week_data",
        "path": "data/processed/player_week_data.parquet",
        "format": "parquet",
    },
    {
        "name": "player",
        "path": "data/processed/player.csv",
        "format": "csv",
    },
    {
        "name": "league_roster",
        "path": "data/raw/league_rosters.json",
        "format": "json",
    },
    {
        "name": "league_user",
        "path": "data/raw/league_users.json",
        "format": "json",
    },
]


def create_table(con, config: Dict):
    table_name = config.get("name")
    if not table_name:
        raise ValueError("Must specify table name.")

    table_path = config.get("path")
    if not table_path:
        raise ValueError("Must specify table path.")

    table_format = config.get("format")
    if not table_format:
        raise ValueError("Must specify table format.")
    if table_format not in CREATE_TEMPLATE_BY_FORMAT:
        valid_formats = ", ".join(CREATE_TEMPLATE_BY_FORMAT.keys())
        error = f"Invalid format `{table_format}`. Must be one of: {valid_formats}."
        raise ValueError(error)

    template = CREATE_TEMPLATE_BY_FORMAT.get(table_format)
    create_query = template.format(table_name=table_name, filename=table_path)
    con.execute(create_query)


@task
def create_database():
    if os.path.exists(DATABASE):
        os.remove(DATABASE)

    con = duckdb.connect(DATABASE)
    for table_config in TABLES_TO_CREATE:
        create_table(con, table_config)


@workflow
def load_workflow():
    create_database()
