#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from datetime import timedelta
from typing import Mapping, Optional, Union

import requests
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.error_handlers.default_error_mapping import DEFAULT_ERROR_MAPPING
from airbyte_cdk.sources.streams.http.error_handlers.error_handler import ErrorHandler
from airbyte_cdk.sources.streams.http.error_handlers.response_models import ErrorResolution, ResponseAction


class HttpStatusErrorHandler(ErrorHandler):
    def __init__(
        self,
        logger: logging.Logger,
        error_mapping: Optional[Mapping[Union[int, str, type[Exception]], ErrorResolution]] = None,
        max_retries: int = 5,
        max_time: timedelta = timedelta(seconds=600),
    ) -> None:
        """
        Initialize the HttpStatusErrorHandler.

        :param error_mapping: Custom error mappings to extend or override the default mappings.
        """
        self._logger = logger
        self._error_mapping = error_mapping or DEFAULT_ERROR_MAPPING
        self._max_retries = max_retries
        self._max_time = int(max_time.total_seconds())

    @property
    def max_retries(self) -> Optional[int]:
        return self._max_retries

    @property
    def max_time(self) -> Optional[int]:
        return self._max_time

    def interpret_response(self, response_or_exception: Optional[Union[requests.Response, Exception]] = None) -> ErrorResolution:
        """
        Interpret the response and return the corresponding response action, failure type, and error message.

        :param response: The HTTP response object.
        :return: A tuple containing the response action, failure type, and error message.
        """
        if response_or_exception is None:
            self._logger.error("Response or exception is None.")
            return ErrorResolution(
                response_action=ResponseAction.FAIL, failure_type=FailureType.system_error, error_message="Response or exception is None."
            )

        if isinstance(response_or_exception, Exception):
            error_response = self._error_mapping.get(type(response_or_exception))
            if error_response is None:
                error_message = f"Unexpected exception in error handler: {response_or_exception}"
                self._logger.error(error_message)
                return ErrorResolution(
                    response_action=ResponseAction.RETRY, failure_type=FailureType.system_error, error_message=error_message
                )
            return error_response

        if isinstance(response_or_exception, requests.Response):
            status_code = response_or_exception.status_code
            if status_code is None:
                error_message = "Response does not include an HTTP status code."
                self._logger.error(error_message)
                return ErrorResolution(
                    response_action=ResponseAction.RETRY, failure_type=FailureType.transient_error, error_message=error_message
                )

            if response_or_exception.ok:
                return ErrorResolution(response_action=ResponseAction.SUCCESS)

            error_response = self._error_mapping.get(status_code)
            if error_response is None:
                self._logger.warning(f"Unexpected HTTP Status Code in error handler: '{status_code}'")
                return ErrorResolution(
                    response_action=ResponseAction.RETRY,
                    failure_type=FailureType.system_error,
                    error_message=f"Unexpected HTTP Status Code in error handler: {status_code}",
                )
            return error_response

        error_message = f"Received unexpected response type: {type(response_or_exception)}"
        self._logger.error(error_message)
        return ErrorResolution(response_action=ResponseAction.FAIL, failure_type=FailureType.system_error, error_message=error_message)
