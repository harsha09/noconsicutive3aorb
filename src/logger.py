from logging import (Formatter, StreamHandler,
	getLogger, DEBUG)
from logging.handlers import TimedRotatingFileHandler
import sys


def get_logger(logger_name):
	__formatter__ = Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
	__logfile__ = 'data/{}.txt'.format(logger_name)

	__consolehandler__ = StreamHandler(sys.stdout)
	__consolehandler__.setFormatter(__formatter__)

	__filehandler__ = TimedRotatingFileHandler(__logfile__, when="midnight")
	__filehandler__.setFormatter(__formatter__)

	logger = getLogger(logger_name)
	logger.setLevel(DEBUG)
	logger.addHandler(__consolehandler__)
	logger.addHandler(__filehandler__)
	logger.propagate = False

	return logger
