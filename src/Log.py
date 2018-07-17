# -*- coding: utf-8 -*-


import logging
from GlobalData import *
# import Test
import os


class Log(object):
    """

    This class guides user over the software execution, it allows to provide information messages (for instance if web
    service of the geocatalog was found), warning messages (example: Un record found is already in the Database) and
    error messages like connection to the catalog service web has been lost.

    """

    _instance = None

    @staticmethod
    def get_instance():
        """
        Returns an unique instance of the Log class

        Test:
        >>> GlobalData.get_instance().parser.set_default('log_file', '../test/test_log')
        >>> log = Log.get_instance()
        >>> log is not None
        True

        """
        if Log._instance is None:
            Log()
        return Log._instance

    def __init__(self):
        """
        
        Test:
        >>> GlobalData.get_instance().parser.set_default('log_file', '../test/test_log')
        >>> log = Log.get_instance()
        >>> log = Log()
        Traceback (most recent call last):
        ...
        Exception: Class Log is a singleton!

        """

        if Log._instance is not None:
            raise Exception("Class Log is a singleton!")
        else:
            self._init_logging()
            Log._instance = self

    @staticmethod
    def _init_logging(level=logging.INFO):
        """

        Initialize log's set up

        """
        format = '%(asctime)-15s %(name)s: %(levelname)s: %(message)s'
        logging.basicConfig(format=format, level=level)
        root = logging.getLogger('')
        formatter = logging.Formatter(format)
        g = GlobalData.get_instance()
        options = g.get_options()
        hdlr = logging.FileHandler(options.log_file)
        hdlr.setFormatter(formatter)
        root.addHandler(hdlr)

    @staticmethod
    def insert_info(place, message):
        """

        Insert an INFO message in the log

        :param place: type of log message
        :param message: message to be printed in the log

        Test:
        >>> file = open('../test/test_log','w')
        >>> file.close()
        >>> GlobalData.get_instance().parser.set_default('log_file', '../test/test_log')
        >>> log = Log.get_instance()
        >>> log.insert_info('here', 'test info')
        ...
        >>> file = open('../test/test_log','r')
        >>> line = file.read()
        >>> 'here: INFO: test info' in line
        True

        """
        logging.getLogger(place).info(message)

    @staticmethod
    def insert_warning(place, message):
        """

        Insert an WARNING message in the log

        :param place: type of log message
        :param message: message to be printed in the log

        Test:
        >>> file = open('../test/test_log','w')
        >>> file.close()
        >>> GlobalData.get_instance().parser.set_default('log_file', '../test/test_log')
        >>> log = Log.get_instance()
        >>> log.insert_warning('here', 'test Warning')
        ...
        >>> file = open('../test/test_log','r')
        >>> line = file.read()
        >>> 'here: WARNING: test Warning' in line
        True

        """
        logging.getLogger(place).warn(message)

    @staticmethod
    def insert_error(place, message):
        """

        Insert an ERROR message in the log

        :param place: type of log message
        :param message: message to be printed in the log

        Test:
        >>> file = open('../test/test_log','w')
        >>> file.close()
        >>> GlobalData.get_instance().parser.set_default('log_file', '../test/test_log')
        >>> log = Log.get_instance()
        >>> log.insert_error('here', 'test Error')
        ...
        >>> file = open('../test/test_log','r')
        >>> line = file.read()
        >>> 'here: ERROR: test Error' in line
        True

        """
        logging.getLogger(place).error(message)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
