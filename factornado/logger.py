import logging
import sys


def get_logger(name=None, **kwargs):
    """Create a logger based on a given config.

    Parameters
    ----------
    name: str, default None
        The name of the logger.
        You'll be able to access the handler through `logging.getLogger(name)`.
        If None, the root logger is used.
    stdout: bool, default True
        Whether you want a STDOUT handler.
    file: str, default None
        Eventually the filename where you want a FileHandler to write.
    format: str
        The format that you want the handlers to use.
        Default: '%(asctime)s (%(filename)s:%(lineno)s)- %(levelname)s - %(message)s'
    level: int, default 10
        The level of your logger.
        (DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50)
    purge_handlers: bool, default None
        Whether existing handlers shall be purged.
        If None, `(name is not None)` is used.
    levels: dict, default {}
        If you want to set the level of other loggers.
        Exemple: {'requests': 30, 'factornado': 20}
    """
    logger = logging.getLogger(name) if name else logging.root
    logger_format = logging.Formatter(kwargs.get(
            'format',
            '%(asctime)s (%(name)s:%(filename)s:%(lineno)s)- %(levelname)s - %(message)s',
            ))

    # Eventually remove previously defined handlers
    if kwargs.get('purge_handlers') or (kwargs.get('purge_handlers') is None and name is not None):
        while len(logger.handlers):
            logger.removeHandler(logger.handlers[0])

    # Eventually set a STDOUT logger
    if kwargs.get('stdout', True):
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.setFormatter(logger_format)
        logger.addHandler(stdout_handler)

    # Eventually set a stream logger
    if kwargs.get('stream'):
        stdout_handler = logging.StreamHandler(stream=kwargs['stream'])
        stdout_handler.setFormatter(logger_format)
        logger.addHandler(stdout_handler)

    # Eventually set a file logger
    if kwargs.get('file'):
        file_handler = logging.FileHandler(filename=kwargs['file'], mode='a')
        file_handler.setFormatter(logger_format)
        logger.addHandler(file_handler)

    logger.setLevel(kwargs.get('level', 10))

    for lib, lib_level in kwargs.get('levels', {}).items():
        logging.getLogger(lib).setLevel(lib_level)

    return logger
