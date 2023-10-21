import pandas as pd
import requests
import time

from flytekit import task, workflow, map_task
from pathlib import Path
from typing import Callable, Dict

from workflows.metadata import STATS_BY_NAME


SLEEPER_API = "https://api.sleeper.com"
DATASET_TYPES = {"stats", "projections"}
POSITIONS = {"QB", "RB", "TE", "WR", "DEF", "K"}
MAX_WEEKS = 17
WEEKS_PER_SEASON = {2021: MAX_WEEKS, 2022: MAX_WEEKS, 2023: 7}
LEAGUE_ID = "999538310517309440"


def fetch_from_sleeper(query: str):
    time.sleep(0.1)
    url = f"{SLEEPER_API}{query}"
    res = requests.get(url)
    return res


def run_if_not_exists(filename: str, task_fn: Callable, kwargs: Dict):
    if Path(filename).exists():
        return

    content = task_fn(**kwargs)
    with open(filename, "wb") as file:
        file.write(content)


@task
def cached_fetch_from_sleeper(filename: str, query: str):

    def do_fetch():
        res = fetch_from_sleeper(query)
        return res.content

    run_if_not_exists(filename, do_fetch, kwargs=dict())


def fetch_week_data(dataset: str, position: str, season: int, week: int):
    if dataset not in DATASET_TYPES:
        raise ValueError(f"Invalid dataset type `{dataset}`.")
    if position not in POSITIONS:
        raise ValueError(f"Invalid position `{position}`.")
    if season not in WEEKS_PER_SEASON:
        raise ValueError(f"Invalid season `{season}`.")
    weeks_in_season = WEEKS_PER_SEASON.get(season, MAX_WEEKS)
    if week < 1 or week > weeks_in_season:
        raise ValueError(f"Invalid week `{week}` for season `{season}`.")

    query = f"/{dataset}/nfl/{season}/{week}?season_type=regular&position={position}"
    res = fetch_from_sleeper(query=query)
    return res.content


@task
def cached_fetch_week_data(kwargs: Dict):
    dataset = kwargs["dataset"]
    position = kwargs["position"]
    season = int(kwargs["season"])
    week = int(kwargs["week"])

    filename = f"data/raw/{dataset}_{season}_{week:02d}_{position}.json"
    kwargs = dict(dataset=dataset, position=position, season=season, week=week)
    run_if_not_exists(
        filename=filename,
        task_fn=fetch_week_data,
        kwargs=kwargs
    )


@task
def fetch_season_data(dataset: str, position: str, season: int, weeks: int):
    if weeks <= 0:
        return
    if weeks > MAX_WEEKS:
        raise ValueError(f"Invalid number of weeks: `{weeks}`.")

    weeks = range(1, weeks + 1, 1)
    season_kwargs = dict(dataset=dataset, season=season, position=position)
    all_kwargs = [dict(**season_kwargs, week=week) for week in weeks]
    map_task(cached_fetch_week_data)(kwargs=all_kwargs)


def generate_season_arguments():
    for season, weeks in WEEKS_PER_SEASON.items():
        for position in POSITIONS:
            for dataset in DATASET_TYPES:
                yield (season, dataset, position, weeks)


def cast_player_id(player_id):
    if isinstance(player_id, int):
        return str(player_id)
    if isinstance(player_id, float):
        return str(int(player_id))
    if isinstance(player_id, str):
        try:
            return str(int(float(player_id)))
        except ValueError:
            return player_id
    return str(player_id)


@task
def union_player_week_data():
    dfs = []
    for season, dataset, position, weeks in generate_season_arguments():
        if weeks <= 0:
            continue
        for week in range(1, weeks + 1):
            filename = f"data/raw/{dataset}_{season}_{week:02d}_{position}.json"
            df = pd.read_json(filename, orient="records")
            df["position"] = position
            dfs.append(df)

    non_null_columns = ["season", "week", "player_id", "team", "opponent"]
    df = pd.concat(dfs, ignore_index=True)
    df.dropna(subset=non_null_columns, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["player_id"] = df["player_id"].apply(cast_player_id).astype(str)
    df["season"] = df["season"].astype(int)
    df["week"] = df["week"].astype(int)
    df["player_name"] = df["player"].apply(lambda d: d["first_name"] + " " + d["last_name"])
    df["latest_team"] = df["player"].apply(lambda d: d.get("team")).fillna("None")
    df["last_modified"] = df["last_modified"].fillna(0).astype(int)
    df["years_experience"] = df["player"].apply(lambda d: d["years_exp"])
    df.drop(columns=["player"], inplace=True)

    df_stats = pd.json_normalize(df["stats"]).fillna(0)
    df_stats = df_stats[STATS_BY_NAME.keys()]
    df_stats.rename(columns=STATS_BY_NAME, inplace=True)

    df_final = pd.concat([df, df_stats], axis=1)
    df_final.drop(columns=["stats"], inplace=True)
    df_final.to_parquet("data/processed/player_week_data.parquet", index=False)


@task
def convert_player_data():
    df = pd.read_json("data/raw/all_players.json", orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "player_id"}, inplace=True)
    df["player_name"] = df["first_name"] + " " + df["last_name"]
    df.to_csv("data/processed/player.csv", index=False)


@workflow
def extract_workflow():
    cached_fetch_from_sleeper(
        filename="data/raw/all_players.json",
        query="/v1/players/nfl"
    )
    for season, dataset, position, weeks in generate_season_arguments():
        fetch_season_data(
            dataset=dataset,
            position=position,
            season=season,
            weeks=weeks
        )

    cached_fetch_from_sleeper(
        filename="data/raw/league_users.json",
        query=f"/v1/league/{LEAGUE_ID}/users"
    )
    cached_fetch_from_sleeper(
        filename="data/raw/league_rosters.json",
        query=f"/v1/league/{LEAGUE_ID}/rosters"
    )


@workflow
def transform_workflow():
    union_player_week_data()
    convert_player_data()
