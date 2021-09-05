from google.cloud import logging
from config import GCP_PROJECT

logging_client = logging.Client(project=GCP_PROJECT)

def write_entry(logger_name, log_struct, severity):
    """Writes log entries to the given logger."""

    # This log can be found in the Cloud Logging console under 'Custom Logs'.
    logger = logging_client.logger(logger_name)

    logger.log_struct(
        log_struct,
        severity = severity
    )

    # print("Wrote logs to {}.".format(logger.name))

