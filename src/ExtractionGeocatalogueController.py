# -*- coding: utf-8 -*-

from owslib.csw import CatalogueServiceWeb
from owslib.ows import ExceptionReport

from Log import *
from GlobalData import *


class ExtractionGeocatalogue(object):
    """

    This class establishes a connection with the catalogue web service in order to extract the metadata that matches with the
    output schema. If it does, a generator of OrderedDict is returned containing the set of values(records) for every
    geocatalogue.


     """

    def __init__(self, source):
        """

        :param source: A Source object of a Geocatalogue

        Test:
        >>> source = Source()
        >>> extract = ExtractionGeocatalogue(source)
        >>> extract.queries['maxrecords'] == 1
        True
        >>> extract.source is not None
        True
        >>> extract.csw is None
        True
        >>> source.url_csw = 'http://www.mongeosource.fr/geosource/1033/fre/csw?version=2.0.2'
        >>> extract2 = ExtractionGeocatalogue(source)
        >>> extract2.csw is not None
        True

        """
        options = GlobalData.get_instance().get_options()
        outputschema = options.outputschema
        self.queries = {'maxrecords': 1, 'esn': 'full', 'outputschema': outputschema, 'startposition': 0}
        self.source = source
        self.csw = None
        self._connect_service_web(source.url_csw)
        self.nmbr_records_to_get = 0

    def _connect_service_web(self, url_csw):
        """

        Connection to the Catalog Service for the Web with the URL, if the connection fails, an exception is raised

        :param url_csw: CSW URL of a geocatalogue

        Test:
        >>> source = Source(None, None, None, None, None, None, 'http://www.mongeosource.fr/geosource/1033/fre/csw?version=2.0.2')
        >>> extract = ExtractionGeocatalogue(source)
        >>> extract.csw is not None
        True

        
        """
        Log.get_instance().insert_info('ExtractionGeocatalogueController', "connect to :%s" % url_csw)
        try:
            self.csw = CatalogueServiceWeb(url_csw)
        except:
            Log.get_instance().insert_error('ExtractionGeocatalogueController', 'Cannot connect to %s' % url_csw)
        if self.csw is not None:
            Log.get_instance().insert_info('ExtractionGeocatalogueController', "connected")

    def get_number_of_record_to_harvest(self):
        """
            This function returns the real number that we can get.
            Compare how much records you want with the number of records of catalogue service web.
        :return: the number of records you can get. on source initialize on this class

        Test:
        >>> source = Source(None, None, None, 4, None, None, 'http://www.mongeosource.fr/geosource/1033/fre/csw?version=2.0.2')
        >>> extract = ExtractionGeocatalogue(source)
        >>> extract.get_number_of_record_to_harvest()
        4

        """
        nmbr_records_to_get = 0
        try:
            self.csw.getrecords2(**self.queries)
            Log.get_instance().insert_info('ok', 'ok')

            if self.source.end_record and self.source.end_record < self.csw.results['matches']:
                nmbr_records_to_get = self.source.end_record
            else:
                nmbr_records_to_get = self.csw.results['matches']
        except Exception as e:
            Log.get_instance().insert_error('ExtractionGeocatalogueController', e)
        return nmbr_records_to_get

    def launch_next_request_CSW(self):
        """
            This function creates request and send it to
            OSWLIB  (library wich get data from geocatalogue).
            After this function you can get results with self.csw.records

        Test:
        >>> source = Source(None, None, 0, 4, 2, None, 'http://www.mongeosource.fr/geosource/1033/fre/csw?version=2.0.2')
        >>> extract = ExtractionGeocatalogue(source)
        >>> extract.get_number_of_record_to_harvest()
        4
        >>> extract.queries['startposition'] = extract.source.begin_record
        >>> extract.csw.results['nextrecord'] = extract.source.begin_record
        >>> extract.queries['maxrecords'] = extract.source.step
        >>> extract.launch_next_request_CSW()
        >>> extract.queries['startposition'] == 0
        True
        >>> extract.queries['maxrecords'] == 1
        True
        
        """
        self.queries['startposition'] = self.csw.results['nextrecord']
        if self.csw.results['nextrecord'] + self.source.step - 1 > self.nmbr_records_to_get:
            self.queries['maxrecords'] = ((self.nmbr_records_to_get - self.source.begin_record)
                                          % self.source.step) + 1
        self.csw.getrecords2(**self.queries)
            
        # if record cannot be integrated (for example if it's a feature catalogue) 
        while len(self.csw.records) == 0:
            # jumps to next record
            Log.get_instance().insert_warning('ExtractionGeocatalogueController', 'could not insert record %s, jumping to next record' % str(self.csw.results['nextrecord']-1))
            self.csw.results['nextrecord'] +=1
            self.queries['startposition'] = self.csw.results['nextrecord']
            self.csw.getrecords2(**self.queries)
                
        Log.get_instance().insert_info('ExtractionGeocatalogueController',
                                       'matches %s (from %s to %s) ; first record %s ;' %
                                       (self.nmbr_records_to_get - self.source.begin_record + 1,
                                        self.source.begin_record, self.nmbr_records_to_get,
                                        self.queries['startposition']) + 'maxrecord %s ; nextrecord %s ; returned %s' %
                                       (self.queries['maxrecords'],self.csw.results['nextrecord'], self.csw.results['returned']))
    def extract_generator_data(self):
        """
        
        Extraction of CSW data from a geocatalogue.
        
         Test:
        >>> source = Source(1,'test Name Idg',0,1,1,"testURL IDG",
        ...        'http://www.mongeosource.fr/geosource/1033/fre/csw?version=2.0.2')
        >>> extract2 = ExtractionGeocatalogue(source)
        >>> extract2.csw is not None
        True
        >>> for i in  extract2.extract_generator_data():
        ...     print (i is None)
        False
        >>> extract2.source.step = 10000000
        >>> for i in  extract2.extract_generator_data():
        ...     print (i is None)
        False
        >>> extract2.source.step = 1
        >>> extract2.source.end_record =0
        >>> t =0
        >>> for i in  extract2.extract_generator_data():
        ...     print (i is None)
        ...     t += 1
        ...     if t >= 2:
        ...         break
        False
        False
        >>> extract2.csw.url = None
        >>> for i in  extract2.extract_generator_data():
        ...     print (i is None)
        >>> extract2.csw = None
        >>> for i in  extract2.extract_generator_data():
        ...     print (i is None)
        >>> source.end_record = 5
        >>> extract3 = ExtractionGeocatalogue(source)
        >>> for i in  extract3.extract_generator_data():
        ...     print (i is None)
        ...     extract3.csw.url = None
        False
        >>> for i in  extract2.extract_generator_data():
        ...     print (i is None)
        ...     extract2.csw.url = None

        """
        if self.csw is  None:
            Log.get_instance().insert_error('ExtractionGeocatalogueController',
                                            ' connexion csw lost or not initialised')
            return
        try:
            self.nmbr_records_to_get = self.get_number_of_record_to_harvest()

            #initialize queries
            self.queries['startposition'] = self.source.begin_record
            self.csw.results['nextrecord'] = self.source.begin_record
            self.queries['maxrecords'] = self.source.step
    	    temp = True
            while self.csw.results['nextrecord'] <= self.nmbr_records_to_get and self.csw.results['nextrecord'] !=0 or temp:
    	       temp = False

               while True:
                    try:
                        self.launch_next_request_CSW()
                        yield self.csw.records
                        break

                    except Exception as e:
                        self.queries['startposition'] = self.queries['startposition'] + 1
                        Log.get_instance().insert_error('ExtractionGeocatalogueController',
                                                            'connexion aborted from %s' % self.source.name_idg)
                        return

        except Exception as e:
            Log.get_instance().insert_error("ExtractionGeocatalogueController", "records are not correct : \n \t %s \n aborted."%e)

if __name__ == "__main__":
    import doctest

    doctest.testmod()
