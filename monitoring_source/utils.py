import os
import logging

import requests


logger = logging.getLogger(os.environ.get('LOGGER'))


def report_error(result: requests.Response):
    logger.warning('----------------------------------------------------------------------------------')
    logger.warning(f"Response returned with status code: {result.status_code}. Reason: {result.reason}")
    logger.warning(f"Requested URL: {result.url}")
    logger.warning(f"Message: {result.json()}")
    logger.warning('----------------------------------------------------------------------------------')