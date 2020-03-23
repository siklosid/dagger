from datetime import datetime

import boto3


class DynamoDb:
    def __init__(self, region, ddb_table):
        dynamo_db = boto3.resource("dynamodb", region_name=region)
        self._ddb_table = dynamo_db.Table(ddb_table)

    def get_item(self, key):
        return self._ddb_table.get_item(Key=key)["Item"]


def dynamo_db_get_value(table, key):
    os = DynamoDb("eu-central-1", table)
    return os.get_item(key)


def from_unix_timestamp_with_ms(timestamp):
    date = datetime.fromtimestamp(timestamp / 1000)
    date = date.replace(microsecond=timestamp % 1000 * 1000)
    return date


user_defined_macros = {
    "dynamo_db_get_value": dynamo_db_get_value,
    "from_unix_timestamp_with_ms": from_unix_timestamp_with_ms,
}
