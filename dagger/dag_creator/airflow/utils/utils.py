from pathlib import Path


def get_sql_queries(path):
    sql_queries = {}
    for query_file in Path(path).glob("*.sql"):
        with open(query_file, "r") as f:
            sql_queries[query_file.stem] = f.read()
    return sql_queries
