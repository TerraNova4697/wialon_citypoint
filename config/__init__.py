import logging
import os


def config_log():
    logger = logging.getLogger(os.environ.get('LOGGER'))

    file_handler = logging.FileHandler(
        filename='logs/development.log', mode="a", encoding="utf-8"
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)s - %(message)s"
        )
    )
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
