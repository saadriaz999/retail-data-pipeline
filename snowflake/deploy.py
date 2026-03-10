"""
Deploy Streamlit app to Snowflake. Creates stage, uploads streamlit_app.py, creates STREAMLIT object.
Run: python deploy.py (from snowflake folder, with .env configured)
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
for p in [Path(__file__).parent / ".env", Path(__file__).parent.parent / ".env"]:
    if p.exists():
        load_dotenv(p)
        break

import snowflake.connector


def main():
    cfg = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }
    if not all([cfg["account"], cfg["user"], cfg["password"]]):
        print("Missing SNOWFLAKE_ACCOUNT, USER, PASSWORD in .env")
        sys.exit(1)

    conn = snowflake.connector.connect(**{k: v for k, v in cfg.items() if v})
    cur = conn.cursor()

    stage = "RETAIL_STREAMLIT_STAGE"
    app_name = "retail_analytics"
    cur.execute(f"CREATE STAGE IF NOT EXISTS {stage}")

    fpath = Path(__file__).parent / "streamlit_app.py"
    if not fpath.exists():
        print("Missing streamlit_app.py")
        sys.exit(1)
    put_sql = f"PUT 'file://{fpath}' @{stage} overwrite=true auto_compress=false"
    try:
        cur.execute(put_sql)
        print("Uploaded streamlit_app.py")
    except Exception as e:
        print(f"PUT failed: {e}")
        print("Manual: upload streamlit_app.py to stage in Snowsight, then:")
        print(f"  CREATE OR REPLACE STREAMLIT {app_name} FROM '@{cfg['database']}.{cfg['schema']}.{stage}' MAIN_FILE = 'streamlit_app.py' QUERY_WAREHOUSE = {cfg['warehouse']};")
        sys.exit(1)

    cur.execute(f"""
        CREATE OR REPLACE STREAMLIT {app_name}
        FROM '@{cfg['database']}.{cfg['schema']}.{stage}'
        MAIN_FILE = 'streamlit_app.py'
        QUERY_WAREHOUSE = {cfg['warehouse']}
        TITLE = 'Retail Analytics'
    """)
    print(f"Created STREAMLIT {app_name}")

    cur.execute(f"ALTER STREAMLIT {app_name} ADD LIVE VERSION FROM LAST")
    conn.close()
    print("Done. Open Snowsight → Streamlit → retail_analytics")


if __name__ == "__main__":
    main()
