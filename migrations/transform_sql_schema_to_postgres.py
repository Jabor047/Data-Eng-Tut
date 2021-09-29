def sql_to_postgres_schema(sql_file_path: str) -> None:
    query = open(sql_file_path, 'r', encoding='utf-8')
    sql_cmd = query.read()

    sql_to_postgres_mapper = {
        "NOT NULL AUTO_INCREMENT", "SERIAL",
    }

    sql_cmd_split = sql_cmd.split()

    print(sql_cmd_split)


if __name__ == "__main__":
    sql_to_postgres_schema('../airflow_dag-dwh_schema/station_summary_schema.sql')


