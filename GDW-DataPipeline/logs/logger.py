import logging


class Log:

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logfilepath = 'app.log'
    LOG_FORMAT = '[%(asctime)s] | %(module)s | %(levelname)s : %(message)s'
    logging.basicConfig(filename=logfilepath,level=logging.INFO,format=LOG_FORMAT,datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger()
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    STREAM_FORMATTER = logging.Formatter(LOG_FORMAT)
    stream_handler.setFormatter(STREAM_FORMATTER)
    logger.addHandler(stream_handler)
    logger =logger