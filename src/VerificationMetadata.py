# -*- coding: utf-8 -*-

from owslib.iso import *
# import to test
from owslib.util import OrderedDict
import datetime
# import base
from sqlalchemy.orm import sessionmaker
from Declarative import *
# import utilities.
from Log import *


class VerificationMetadata:
    """

    This module allows verify if every records in Orderecdict (data geographic retrieve from server for every
    geocatalog) is provided correctly. For that this class verify if every attribute is provided and well informed
    according to the ISO 19115.

    """


    def __init__(self, num):
        """
        initialize session database and get the only instance of global data.
        Test:
        >>> verif = VerificationMetadata(1)
        >>> verif.sdi == 1
        True
        >>> verif.globalData is not None
        True
        >>> verif.session is not None
        True

        """
        self.sdi = num
        self.globalData = GlobalData.get_instance()
        engine = create_engine('postgresql://' + self.globalData.get_data_base_parameter('user')
                               + ':' + self.globalData.get_data_base_parameter('password') + '@' +
                               self.globalData.get_data_base_parameter('host') + ':' +
                               self.globalData.get_data_base_parameter('port') + '/'
                               + self.globalData.get_data_base_parameter('dbname'))
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        self.session = db_session()

    def check(self, data):
        """
        Check Collections.ordereddict correspond to ISO1915.
        :param data: Collections.ordereddict


        Test:
        >>> verify = VerificationMetadata(1)
        >>> o = OrderedDict()
        >>> md = MD_Metadata()
        >>> o['test'] = md
        >>> verify.check(o)
        >>> len(o.values()) == 0
        True
        >>> md.identification = MD_DataIdentification()
        >>> o['test2'] = md
        >>> verify.check(o)
        >>> len(o.values()) ==0
        True
        >>> md.identification.bbox = EX_GeographicBoundingBox()
        >>> o['test3'] = md
        >>> verify.check(o)
        >>> len(o.values()) == 1
        True

        """
        self._check_all_field(data)
        self._check_existing_previous_record(data)


    def _check_all_field(self,data):
        """

        Remove incorrect data
        :param data: data to test

        Test:
        >>> verify = VerificationMetadata(1)
        >>> o = OrderedDict()
        
        >>> md = MD_Metadata()
        >>> md.identification = MD_DataIdentification()
        >>> md.identification.bbox = EX_GeographicBoundingBox()
        >>> o['test'] = md
        >>> verify._check_all_field(o)
        >>> len(o.values()) == 1
        True

        >>> md = MD_Metadata()
        >>> o['test'] = md
        >>> verify._check_all_field(o)
        >>> len(o.values()) == 0
        True
        """

        nb_bad = 0
        for j in data:
            i = data.get(j)
            correspond = False
            if all([hasattr(i, 'datetimestamp'),
                    hasattr(i, 'xml'),
                    hasattr(i, 'stdname'),
                    hasattr(i, 'identification')]):

                if all([hasattr(i.identification, 'identtype'),
                        hasattr(i.identification, 'keywords'),
                        hasattr(i.identification, 'contact'),
                        hasattr(i.identification, 'contributor'),
                        hasattr(i.identification, 'creator'),
                        hasattr(i.identification, 'bbox')]):

                    if all([hasattr(i.identification.bbox, 'maxx'),
                            hasattr(i.identification.bbox, 'maxy'),
                            hasattr(i.identification.bbox, 'minx'),
                            hasattr(i.identification.bbox, 'miny')]):
                        correspond = True
            if not correspond:
                data.pop(j)
                nb_bad += 1
        if nb_bad != 0:
            Log.get_instance().insert_warning('VerificationMetadata', '%s : number incorrect records  ' % nb_bad)

    def _check_existing_previous_record(self, data):
        """

        check if record is already in database. delete in object data if already exist.

        Test:
        >>> verify = VerificationMetadata(1)
        >>> o = OrderedDict()
        >>> md = MD_Metadata()
        >>> o['test'] = md
        >>> num = verify.session.query(Metadata).delete()
        >>> num = verify.session.query(SDI).delete()
        >>> num = verify.session.query(Extraction).delete()
        >>> verify.session.commit()
        >>> num = verify.session.add(Metadata(id_metadata=1, identifier='test'))
        >>> num = verify.session.add(SDI(id_sdi=1))
        >>> verify.session.commit()
        >>> num = verify.session.add(Extraction(id_sdi=1, id_metadata=1,
        ...       date_extraction=verify.globalData.get_options().date))
        >>> verify.session.commit()
        >>> o['test2'] = md
        >>> len(o.values()) == 2
        True
        >>> verify._check_existing_previous_record(o)
        >>> len(o.values()) == 1
        True

        """
        ids = data.keys()
        option = self.globalData.get_options()
        nb_exist = 0
        for id_md in self.session.query(Metadata.identifier).filter(Metadata.id_metadata == Extraction.id_metadata,
                                                                    Extraction.id_sdi == SDI.id_sdi,
                                                                    SDI.id_sdi == self.sdi,
                                                                    Extraction.date_extraction == option.date):
            if id_md.identifier in ids:
                data.pop(id_md.identifier)
                nb_exist += 1
        if nb_exist > 0:
            Log.get_instance().insert_warning('VerificationMatadata', ' %s : records already exist' % nb_exist)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
