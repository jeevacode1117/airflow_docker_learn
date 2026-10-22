from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task

DATA_DIR = Path("/opt/airflow/data")

@dag(
    dag_id="file_passing",
    start_date=datetime(2026, 10, 14),
    schedule=None,
    catchup=False
)
def file_passing():

    @task.python
    def extract(**kwargs):
        import pandas as pd                      # imported only when the task runs
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"n": range(1, 1_000_001)})
        path = DATA_DIR / f"raw_{kwargs['run_id']}.csv"
        df.to_csv(path, index=False)
        return str(path)

    @task.python
    def transform(raw_path):
        import pandas as pd
        df = pd.read_csv(raw_path)
        df = df[df["n"] % 2 == 0]
        out_path = DATA_DIR / Path(raw_path).name.replace("raw_", "clean_")
        df.to_csv(out_path, index=False)
        return str(out_path)

    @task.python
    def load(clean_path):
        import pandas as pd
        df = pd.read_csv(clean_path)
        print(f"Rows: {len(df)}")

    load(transform(extract()))

file_passing()