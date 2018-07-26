# -*- coding: utf-8 -*-

import ConfigParser
import datetime
from optparse import OptionParser
import sys
from Source import *
from Log import *


class GlobalData(object):
    """

     This module manages not only the external resources required for performing the harvesting such as database
    (to archive information for future analysis) and list of geocatalogs in a CSV File (it is used to find server where
    we request the metadata geographic), but also the execute options.

    """
    _instance = None

    @staticmethod
    def get_instance():
        """

        Returns the unique instance of the GlobalData class

        :return:  the single instance of global data

        Test:
        >>> globalData = GlobalData.get_instance()
        >>> globalData is not None
        True
        >>> g = GlobalData()
        Traceback (most recent call last):
         ...
        Exception: This class is a singleton!
        >>> GlobalData.get_instance().parser.set_default('sources', None)
        >>> GlobalData.get_instance().list_geocatalog_sources = []
        >>> GlobalData.get_instance()._init_csv_file()
        Traceback (most recent call last):
        ...
        NameError: Error CSV file Name : None

        """
        if GlobalData._instance is None:
            GlobalData()
        return GlobalData._instance

    def __init__(self):
        """

        Test:
        >>> globalData = GlobalData.get_instance()
        >>> globalData.parser is not None
        True
        >>> globalData.dictDataBaseParameter is not None
        True
        >>> globalData.list_geocatalog_sources is not None
        True

        """
        # print("renteO" + GlobalData._instance )
        if GlobalData._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            self._option()
            self._init_dico_base_parameters()
            self._init_csv_file()
            GlobalData._instance = self

    def _option(self):
        """

        Initialize script's execute options

        Test:
        >>> globalData = GlobalData.get_instance()
        >>> (options, args) = globalData.parser.parse_args()
        >>> options.outputschema == 'http://www.isotc211.org/2005/gmd'
        True
        >>> options.sources == "../sources/source_test.csv"
        True
        >>> options.log_file == '../csw-harvester.log'
        True
        >>> options.date == datetime.datetime.today().strftime('%Y-%m-%d')
        True


        """
        outputschema = 'http://www.isotc211.org/2005/gmd'
        esn = 'summary'
        sources_files = "../sources/source_test.csv"
        log_file = '../csw-harvester.log'
        date = datetime.datetime.today().strftime('%Y-%m-%d')

        self.parser = OptionParser()

        self.parser.add_option("-f", "--outputschema", dest="outputschema", action="store", default=outputschema,
                               help="the outputschema for CSW ; default = http://www.isotc211.org/2005/gmd")
        self.parser.add_option("-e", "--esn", dest="esn", action="store", default=esn,
                               help="ElementSetName : brief, summary or full ; default = summary")
        self.parser.add_option("-s", "--sources", dest="sources",
                               action="store", default=sources_files,
                               help="the CSV sources files ; default = ./sources/source_test.csv")
        self.parser.add_option("-l", "--log-file", dest="log_file", action="store", default=log_file,
                               help="the log files ; default : ../csw-harvester.log")
        self.parser.add_option("-d", "--date", dest="date", action="store", default=date,
                               help="the extraction date ; default : the current date")

    def _init_dico_base_parameters(self):
        """
        Initialize the dictionary with the database settings in the file "config_database.cfg"

        Test:
        >>> globalData = GlobalData.get_instance()
        >>> globalData.dictDataBaseParameter != {}
        True

        """

        config_file = "config_database.cfg"
        self.dictDataBaseParameter = dict()

        config = ConfigParser.RawConfigParser()
        config.read(config_file)
        for key in config.options("base"):
            self.dictDataBaseParameter[key] = config.get("base", key)

    def _init_csv_file(self):
        """

        Parser file source with the list of geocatalogs

        Test:
        >>> globalData = GlobalData.get_instance()
        >>> globalData.list_geocatalog_sources != []
        True

        """

        self.list_geocatalog_sources = list()
        try:
            (options, args) = self.parser.parse_args()

            source_file = open(options.sources)
        except:
            Log.get_instance().insert_error('GlobalData', 'Unable to open : %s' % options.sources)
            raise NameError('Error CSV file Name : %s' % options.sources)
        for source in source_file.readlines():
            if source[0] != '#':
                num = source.split(',')[0]
                name = source.split(',')[1]
                begin_record = int(source.split(',')[2])
                end_record = int(source.split(',')[3])
                MAXR = int(source.split(',')[4])
                url = source.split(',')[5]
                url_csw = source.split(',')[6].strip('\n')
                self.list_geocatalog_sources.append(Source(num, name, begin_record, end_record, MAXR, url, url_csw))

    def get_data_base_parameter(self, key):
        """

        Get options of database : port, dbname, host, user, password.

        :param key: the option of database to recover
        :return: string to option

        Test:
        >>> glob = GlobalData.get_instance()
        >>> res = glob.get_data_base_parameter('port')
        >>> res is not None
        True

        """
        return self.dictDataBaseParameter[key]

    def get_options(self):
        """

        Get all possible options for execute the application.

        :return: all option of the application

        Test:
        >>> glob = GlobalData.get_instance()
        >>> res = glob.get_options()
        >>> res is not None
        True

        """
        (options, args) = self.parser.parse_args()
        return options

    def get_geocatalog_sources(self):
        """

        Get the list entire of geocatalogs in the file source.

        :return: sources.

        Test:
        >>> glob = GlobalData.get_instance()
        >>> res = glob.get_geocatalog_sources()
        >>> res is not None
        True

        """
        return self.list_geocatalog_sources


if __name__ == "__main__":
    import doctest

    doctest.testmod()
