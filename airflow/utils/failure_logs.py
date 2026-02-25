# Updated airflow/utils/failure_logs.py
import psycopg2
from datetime import datetime, timezone

def log_failure(context):
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="de_warehouse",
            user="de_user",
            password="de_pass",
            port=5432
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_failures (dag_id, task_id, run_id, error_message, failed_at)
                VALUES (%s, %s, %s, %s, %s)       
                """,
                (
                    context["dag"].dag_id,
                    context["task_instance"].task_id,
                    context["run_id"],
                    str(context.get("exception")),
                    datetime.now(timezone.utc),
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to log failure to DB: {e}")