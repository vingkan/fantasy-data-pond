# Fantasy Data Pond

A data lake is where you store a bunch of data in the format that in came in.

A data pond is like that, but smaller.

## Goals

- Analyze fantasy football data.
- Create tables and visualizations.
- Win with logic and reason.

## Instructions

### Initial Setup

1. Create a virtual environment and activate it.

  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  ```

2. Install the dependencies.

  ```bash
  pip3 install -r requirements.txt
  ```
3. Install a kernel and start a Jupyter notebook server.

  ```bash
  ipython kernel install --user 
  jupyter notebook
  ```

When Jupyter opens in your browser, open the `notebooks/demo.ipynb` notebook.

### Future Uses

After installation, you should only need these commands to get started.

```bash
source .venv/bin/activate
jupyter notebook
```

When new dependencies are added, you will need to run this command again.

```bash
pip3 install -r requirements.txt
```

### Database

We use DuckDB to store our data. Review `notebooks/demo.ipynb` for a tutorial on how to access data from the database via Jupyter notebook.

For a list of metrics available in the database, refer to the mapping in `workflows/metadata.py`.

You can also access the data using the DuckDB command line utility. You can install that tool with this command.

```bash
brew install duckdb
```

To open our database in the DuckDB command line tool, use this command.

```bash
duckdb database.db
```

## Pipeline

The pipeline populates the data in our database. Usually, you will not need to run it because `database.db` already contains the results of the last successful pipeline run.

If you ever do need to run the pipeline, you will use this command.

```bash
pyflyte run workflows/main.py main
```

The pipeline consists of three sections:

1. **Extract:** Downloads data about the league and players.
2. **Transform:** Converts the data to a format suitable for analysis.
3. **Load:** Copies the data into the database.

If the first two sections have already run successfully, you can choose to run only the `load` section of the pipeline with this command.

```bash
pyflyte run workflows/main.py load
```
