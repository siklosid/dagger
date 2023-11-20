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
                    "model.main.stg_core_schema2__table2",
                    "model.main.model2",
                    "model.main.int_model3",
                ],
            },
        },
        "model.main.stg_core_schema1__table1": {
            "schema": "analytics_engineering",
            "name": "stg_core_schema1__table1",
            "depends_on": {
                "macros": [],
                "nodes": ["source.main.core_schema1.table1"],
            },
        },
        "model.main.stg_core_schema2__table2": {
            "schema": "analytics_engineering",
            "name": "stg_core_schema2__table2",
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.main.core_schema2.table2",
                    "source.main.core_schema2.table3",
                ],
            },
        },
        "model.main.model2": {
            "name": "model2",
            "schema": "analytics_engineering",
            "config": {
                "external_location": "s3://bucket1-data-lake/path2/model2",
            },
        },
        "model.main.int_model3": {
            "name": "int_model3",
            "schema": "analytics_engineering",
        },
        "seed.main.seed_buyer_country_overwrite": {
            "database": "awsdatacatalog",
            "schema": "analytics_engineering",
            "name": "seed_buyer_country_overwrite",
            "resource_type": "seed",
            "alias": "seed_buyer_country_overwrite",
            "tags": ["analytics"],
            "description": "",
            "created_at": 1700216177.105391,
            "depends_on": {"macros": []},
        },
    },
    "sources": {
        "source.main.core_schema1.table1": {
            "source_name": "table1",
            "database": "awsdatacatalog",
            "schema": "core_schema1",
            "name": "table1",
            "tags": ["analytics"],
            "description": "",
        },
        "source.main.core_schema2.table2": {
            "source_name": "table2",
            "database": "awsdatacatalog",
            "schema": "core_schema2",
            "name": "table2",
            "tags": ["analytics"],
            "description": "",
        },
        "source.main.core_schema2.table3": {
            "source_name": "table3",
            "database": "awsdatacatalog",
            "schema": "core_schema2",
            "name": "table3",
            "tags": ["analytics"],
            "description": "",
        },
    },
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

EXPECTED_STAGING_NODE = [
    {
        "type": "athena",
        "name": "stg_core_schema1__table1",
        "schema": "core_schema1",
        "table": "table1",
        "follow_external_dependency": True,
    }
]
EXPECTED_STAGING_NODE_MULTIPLE_DEPENDENCIES = [
    {
        "type": "athena",
        "name": "stg_core_schema2__table2",
        "schema": "core_schema2",
        "table": "table2",
        "follow_external_dependency": True,
    },
    {
        "type": "athena",
        "name": "stg_core_schema2__table3",
        "schema": "core_schema2",
        "table": "table3",
        "follow_external_dependency": True,
    },
]

EXPECTED_DAGGER_INPUTS = [
    {
        "name": "stg_core_schema2__table2",
        "schema": "core_schema2",
        "table": "table2",
        "type": "athena",
        "follow_external_dependency": True,
    },
    {
        "name": "stg_core_schema2__table3",
        "schema": "core_schema2",
        "table": "table3",
        "type": "athena",
        "follow_external_dependency": True,
    },
    {
        "name": "analytics_engineering_model2_athena",
        "schema": "analytics_engineering",
        "table": "model2",
        "type": "athena",
        "follow_external_dependency": True,
    },
    {
        "bucket": "bucket1-data-lake",
        "name": "analytics_engineering_model2_s3",
        "path": "path2/model2",
        "type": "s3",
    },
]

EXPECTED_DAGGER_OUTPUTS = [
    {
        "name": "analytics_engineering_fct_supplier_revenue_athena",
        "schema": "analytics_engineering",
        "table": "fct_supplier_revenue",
        "type": "athena",
        "follow_external_dependency": True,
    },
    {
        "bucket": "bucket1-data-lake",
        "name": "analytics_engineering_fct_supplier_revenue_s3",
        "path": "path1/model1",
        "type": "s3",
    },
]
