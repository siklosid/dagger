DBT_MANIFEST_FILE_FIXTURE = {
    "nodes": {
        "model.main.model1": {
            "database": "awsdatacatalog",
            "schema": "analytics_engineering",
            "name": "fct_supplier_revenue",
            "config": {
                "external_location": "s3://bucket1-data-lake/path1/model1",
                "materialized": "incremental",
                "incremental_strategy": "insert_overwrite",
            },
            "description": "Details of revenue calculation at supplier level for each observation day",
            "tags": ["daily"],
            "unrendered_config": {
                "materialized": "incremental",
                "external_location": "s3://bucket1-data-lake/path1/model1",
                "incremental_strategy": "insert_overwrite",
                "partitioned_by": ["year", "month", "day", "dt"],
                "tags": ["daily"],
                "on_schema_change": "fail",
            },
            "depends_on": {
                "macros": [
                    "macro.main.macro1",
                    "macro.main.macro2",
                ],
                "nodes": [
                    "model.main.stg_core_schema1__table1",
                    "model.main.model2",
                    "model.main.int_model3",
                ],
            },
        },
        "model.main.stg_core_schema1__table1": {
            "schema": "analytics_engineering",
        },
        "model.main.model2": {
            "schema": "analytics_engineering",
            "config": {
                "external_location": "s3://bucket1-data-lake/path2/model2",
            },
        },
        "model.main.int_model3": {
            "schema": "analytics_engineering",
        },
    }
}

DBT_PROFILE_FIXTURE = {
    "main": {
        "outputs": {
            "data": {
                "aws_profile_name": "data",
                "database": "awsdatacatalog",
                "num_retries": 10,
                "region_name": "eu-west-1",
                "s3_data_dir": "s3://bucket1-data-lake/path1/tmp",
                "s3_data_naming": "schema_table",
                "s3_staging_dir": "s3://bucket1-data-lake/path1/",
                "schema": "analytics_engineering",
                "threads": 4,
                "type": "athena",
                "work_group": "primary",
            },
        }
    }
}