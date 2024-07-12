#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping, Optional

from airbyte_cdk.sources.file_based.config.file_based_stream_config import ValidationPolicy
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, StopSyncPerValidationPolicy
from airbyte_cdk.sources.file_based.schema_helpers import conforms_to_schema
from airbyte_cdk.sources.file_based.schema_validation_policies import AbstractSchemaValidationPolicy


class EmitRecordPolicy(AbstractSchemaValidationPolicy):
    name = "emit_record"

    def record_passes_validation_policy(self, record: Mapping[str, Any], schema: Optional[Mapping[str, Any]]) -> bool:
        return True


class SkipRecordPolicy(AbstractSchemaValidationPolicy):
    name = "skip_record"

    def record_passes_validation_policy(self, record: Mapping[str, Any], schema: Optional[Mapping[str, Any]]) -> bool:
        return schema is not None and conforms_to_schema(record, schema)


class WaitForDiscoverPolicy(AbstractSchemaValidationPolicy):
    name = "wait_for_discover"
    validate_schema_before_sync = True

    def record_passes_validation_policy(self, record: Mapping[str, Any], schema: Optional[Mapping[str, Any]]) -> bool:
        if schema is None or not conforms_to_schema(record, schema):
            raise StopSyncPerValidationPolicy(FileBasedSourceError.STOP_SYNC_PER_SCHEMA_VALIDATION_POLICY)
        return True


def is_equal_or_narrower_type(value: Any, expected_type: str) -> bool:
    if expected_type == "null":
        return value is None
    elif expected_type == "boolean":
        return isinstance(value, bool)
    elif expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    elif expected_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    elif expected_type == "string":
        return isinstance(value, str)
    elif expected_type == "array":
        return isinstance(value, list)
    elif expected_type == "object":
        return isinstance(value, dict)
    else:
        return False


DEFAULT_SCHEMA_VALIDATION_POLICIES = {
    ValidationPolicy.emit_record: EmitRecordPolicy(),
    ValidationPolicy.skip_record: SkipRecordPolicy(),
    ValidationPolicy.wait_for_discover: WaitForDiscoverPolicy(),
}
