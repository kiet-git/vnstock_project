import logging

# Set up logger
def create_logger():
    logger = logging.getLogger('server_logger')
    logger.setLevel(logging.ERROR)
    logger.setLevel(logging.INFO)

    for handler in logger.handlers:
        print(handler)
        logger.removeHandler(handler)

    if not len(logger.handlers) > 0:
        file_handler = logging.FileHandler(f'/opt/airflow/data/logs.log')

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger