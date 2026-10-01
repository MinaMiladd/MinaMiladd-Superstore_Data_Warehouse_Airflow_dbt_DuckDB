from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = PROJECT_ROOT / "my_project" / "dev.duckdb"
EXCEL_PATH = PROJECT_ROOT / "data" / "raw" / "Superstore.xlsx"

SCHEMA = "ods"
TABLE = "superstore"

con = duckdb.connect(str(DB_PATH))

con.execute("INSTALL excel")
con.execute("LOAD excel")
con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")


con.execute(f"""
    CREATE OR REPLACE TABLE {SCHEMA}.{TABLE} AS
    SELECT *
    FROM read_xlsx('{EXCEL_PATH.as_posix()}', sheet = 'Sample - Superstore', header = true)
""")

count = con.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}").fetchone()[0]
print(f"Loaded {SCHEMA}.{TABLE}: {count} rows")

con.close()
print("ODS load complete")