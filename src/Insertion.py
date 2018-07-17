# -*- coding: utf-8 -*-


from sqlalchemy import exc

# import utilities.
from datetime import datetime

from Log import *
from GlobalData import *
from owslib.iso import *

# import to test
from owslib.util import OrderedDict
from sqlalchemy import func
# import base

from sqlalchemy.orm import sessionmaker
from Declarative import *
from Source import *


class Insertion(object):
    """

    This class convert the set of values retrieved as object OrderedDict to Declaratives which are classes
    of simpler values (scalar) for storage in the Database, at this point metadata can be store in database thanks to
    the ORM's declaratives, it preserves all properties of the OrderedDict and all their relationships so that they can
    be reloaded if is needed.

    """

    def __init__(self, source):
        """
        Initialisation of connexion to database and list to records data.
        Example:
         >>> insertion = Insertion(Source())

        Test:
        >>> insertion = Insertion(Source())
        >>> print(type(insertion.session))
        <class 'sqlalchemy.orm.session.Session'>
        >>> insertion.id_keyword is not None
        True
        >>> insertion.id_metadata is not None
        True
        >>> insertion.id_responsibleparty is not None
        True
        >>> num = insertion.session.query(Responsibleparty).delete()
        >>> num = insertion.session.query(Metadata).delete()
        >>> num= insertion.session.query(Keyword).delete()
        >>> insertion.session.commit()
        >>> insertion2 = Insertion(Source())
        >>> insertion2.id_keyword == 0
        True
        >>> insertion2.id_metadata == 0
        True
        >>> insertion2.id_responsibleparty == 0
        True

        """
        self._connect()
        self._init_global_id()
        self.source = source

    def _connect(self):
        """
        Initiate connection to posgresql

        :return: the current session of sqlalchemy
        """

        global_data = GlobalData.get_instance()
        engine = create_engine('postgresql://' + global_data.get_data_base_parameter('user')
                               + ':' + global_data.get_data_base_parameter('password') + '@' +
                               global_data.get_data_base_parameter('host') + ':' +
                               global_data.get_data_base_parameter('port') + '/'
                               + global_data.get_data_base_parameter('dbname'))
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        self.session = db_session()

    def _init_global_id(self):
        """

        Initialization of global id id_metadata, id_respondibleparty and id_keyword.
        Take ids in database postgres.

        """
        # get next  id_metadata
        query = self.session.query(func.max(Metadata.id_metadata))
        result = self.session.execute(query)
        try:
            self.id_metadata = result.fetchone()[0] + 1
        except:
            self.id_metadata = 0

        # get next  id_keyword
        query = self.session.query(func.max(Keyword.id_keyword))
        result = self.session.execute(query)

        try:
            self.id_keyword = result.fetchone()[0] + 1
        except:
            self.id_keyword = 0

        # get next  id_responsibleparty
        query = self.session.query(func.max(Responsibleparty.id_responsibleparty))
        result = self.session.execute(query)
        try:
            self.id_responsibleparty = result.fetchone()[0] + 1
        except:
            self.id_responsibleparty = 0

    def save_in_db(self, data):
        """
        save ordereddict in database. Begin to convert this data to déclarative.
        Then save déclaratives produce in database
        :param data: datas to records. It's ordereddict. Value of this dictionary must me type of MD_Metadata.

        Test:
        >>> source = Source(123456789,"nameTest",0,0,30,"TestUrl","testUrl_csw")
        >>> insert = Insertion(source)
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identifier = 'test_id'
        >>> meta.datetimestamp = datetime.datetime.today().strftime('%Y-%m-%d')
        >>> meta.xml = "test xml"
        >>> meta.stdname = "IDO  1915"
        >>> meta.identification = MD_DataIdentification()
        >>> meta.identification.identtype = "test identype"
        >>> tabKeyword = [{'keywords': ['Usage des sols'], 'type': None, 'thesaurus': {'date': '2008-06-01',
        ...                 'datetype': 'publication', 'title': 'GEMET - INSPIRE themes, version 1.0'}},
        ...                {'keywords': ['Risque / Zonages Risque naturel'], 'type': 'theme',
        ...                 'thesaurus': {'date': '2009-12-03', 'datetype': 'publication',
        ...                 'title': 'Arborescence COVADIS'}},
        ...                 {'keywords': ['RESSOURCES - NUISANCES/NUISANCE/source de nuisance (inclut risques industriels et naturels)'],
        ...                 'type': 'theme', 'thesaurus': {'date': '2009', 'datetype': 'publication',
        ...                 'title': u'Th\xc3\xa9saurus URBAMET'}}, {'keywords': [u'Donn\xe9es vecteur'],
        ...                 'type': 'stratum', 'thesaurus': {'date': None, 'datetype': None, 'title': None}}]
        >>> meta.identification.keywords = tabKeyword
        >>> meta.identification.bbox = EX_GeographicBoundingBox()
        >>> meta.identification.bbox.maxx =5
        >>> meta.identification.bbox.maxy = 10
        >>> meta.identification.bbox.minx = 0
        >>> meta.identification.bbox.miny = 0
        >>> meta.identification.contact = []
        >>> ci = CI_ResponsibleParty()
        >>> ci.city= 'Bordeaux'
        >>> ci.role = 'nothing'
        >>> meta.identification.contact.append(ci)
        >>> meta.identification.contributor = []
        >>> meta.identification.contributor.append(ci)
        >>> meta.identification.creator = []
        >>> meta.identification.creator.append(ci)
        >>> o['test_id'] = meta
        >>> num = insert.session.query(SDI).delete()
        >>> num = insert.session.query(Metadata).delete()
        >>> num = insert.session.query(Responsibleparty).delete()
        >>> num = insert.session.query(Keyword).delete()
        >>> num = insert.session.query(Dataidentification).delete()
        >>> num = insert.session.query(Contact).delete()
        >>> num = insert.session.query(Geographicboundingbox).delete()
        >>> num = insert.session.query(Extraction).delete()
        >>> insert.session.commit()
        >>> listconvert = insert.save_in_db(o)
        >>>
        """

        list_orm = self.convert_cswdata_to_orm(data)
        self.save_orm_in_db(list_orm,len(data.values()))

    def convert_cswdata_to_orm(self, data):
        """
            Convert cswdata ro declaratives of sqlalchemy.

        :param data: collection.orderedDict, the data to convert
        :return: List of declarative to save in BDD.

        Test:
        >>> source = Source(123456789, "nameTest", 0, 0, 30, "TestUrl", "testUrl_csw")
        >>> insert = Insertion(source)
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identifier = 'test_id'
        >>> meta.datetimestamp = datetime.datetime.today().strftime('%Y-%m-%d')
        >>> meta.xml = "test xml"
        >>> meta.stdname = "IDO  1915"
        >>> meta.identification = MD_DataIdentification()
        >>> meta.identification.identtype = "test identype"
        >>> tabKeyword = [{'keywords': ['Usage des sols'], 'type': None, 'thesaurus': {'date': '2008-06-01',
        ...                 'datetype': 'publication', 'title': 'GEMET - INSPIRE themes, version 1.0'}},
        ...                {'keywords': ['Risque / Zonages Risque naturel'], 'type': 'theme',
        ...                 'thesaurus': {'date': '2009-12-03', 'datetype': 'publication',
        ...                 'title': 'Arborescence COVADIS'}},
        ...                 {'keywords': ['RESSOURCES - NUISANCES/NUISANCE/source de nuisance (inclut risques industriels et naturels)'],
        ...                 'type': 'theme', 'thesaurus': {'date': '2009', 'datetype': 'publication',
        ...                 'title': u'Th\xc3\xa9saurus URBAMET'}}, {'keywords': [u'Donn\xe9es vecteur'],
        ...                 'type': 'stratum', 'thesaurus': {'date': None, 'datetype': None, 'title': None}}]
        >>> meta.identification.keywords = tabKeyword
        >>> meta.identification.bbox = EX_GeographicBoundingBox()
        >>> meta.identification.bbox.minx = 0
        >>> meta.identification.bbox.miny = 0
        >>> meta.identification.contact = []
        >>> ci = CI_ResponsibleParty()
        >>> ci.city= 'Bordeaux'
        >>> ci.role = 'nothing'
        >>> meta.identification.contact.append(ci)
        >>> meta.identification.contributor = []
        >>> meta.identification.contributor.append(ci)
        >>> meta.identification.creator = []
        >>> meta.identification.creator.append(ci)
        >>> o['test_id'] = meta
        >>> temp_id_Meta = insert.id_metadata
        >>> listconvert = insert.convert_cswdata_to_orm(o)
        >>> len(listconvert) == 15
        True
        >>> ( insert.id_metadata -temp_id_Meta) == 1
        True
        """
        list_orm = []
        list_meta = self._convert_to_metadata(data)
        if list_meta:
            list_orm.extend(list_meta)

        list_data_identif = self._convert_to_dataidentification(data)
        if list_data_identif:
            list_orm.extend(list_data_identif)

        list_keyword = self._convert_to_keyword(data)
        if list_keyword:
            list_orm.extend(list_keyword)

        list_geographicbb = self._convert_to_geographicboundingbox(data)
        if list_geographicbb:
            list_orm.extend(list_geographicbb)

        list_resp = self._convert_to_responsibleparty(data)
        if list_resp:
            list_orm.extend(list_resp)

        list_sdi = self._convert_to_sdi()
        if list_sdi:
            list_orm.extend(list_sdi)

        list_extraction = self._convert_to_extraction(data)
        if list_extraction:
            list_orm.extend(list_extraction)

        self.id_metadata += len(data.values())
        return list_orm

    def save_orm_in_db(self, listDeclaratives, number_to_records):
        """
        Save a list in database. the list must be Declaratives. Check sqlalchemy for more informations.

        :param listDeclaratives: list of object type declaratives to save in BDD
        Test:
        >>> insert = Insertion(Source(name_idg='test'))
        >>> list = []
        >>> sdi = SDI(id_sdi=1048, name='test', url='test_url', url_csw='test url_csw')
        >>> num_rows = insert.session.query(SDI).delete()
        >>> num_rows2 = insert.session.query(Responsibleparty).delete()
        >>> insert.session.commit()
        >>> responsibleparty= Responsibleparty(id_responsibleparty=1, city='test_city', role='test role',
        ...                                     organization='test organization')
        >>> list.append(sdi)
        >>> list.append(responsibleparty)
        >>> query = insert.session.query(SDI.id_sdi).filter(SDI.id_sdi == 1048)
        >>> res = insert.session.execute(query)
        >>> res.fetchone() == None
        True
        >>> insert.save_orm_in_db(list,2)
        >>> query = insert.session.query(SDI.id_sdi).filter(SDI.id_sdi == 1048)
        >>> res = insert.session.execute(query)
        >>> resultat = res.fetchone()[0]
        >>> resultat == 1048
        True
        >>> query = insert.session.query(Responsibleparty.id_responsibleparty).filter(
        ...                              Responsibleparty.id_responsibleparty == 1)
        >>> res = insert.session.execute(query)
        >>> resultat = res.fetchone()[0]
        >>> resultat == 1
        True
        >>> list = [sdi]
        >>> insert.save_orm_in_db(list,2)
        """
        nb_records = number_to_records

        if listDeclaratives > 0:
            try:
                self.session.bulk_save_objects(listDeclaratives)
                self.session.commit()
                Log.get_instance().insert_info('Insertion', '%s next values from %s inserted into database' %
                                               (nb_records, self.source.name_idg))
            except exc.SQLAlchemyError as e:
                Log.get_instance().insert_error('Insertion',
                                               '%s next values from %s  don\'t inserted into database : %s' %
                                               (nb_records, self.source.num_idg, e))

    def _convert_to_metadata(self, data):
        """
        convert OrderedDict to collections.Declaratives.Metadata.
        :param data: type of collectionOrderedDict in oswlib. the data to convert.
        :return: list of Declarative.Metadata

        Test:
        >>> insert = Insertion(Source())
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identifier = 'test_id'
        >>> meta.datetimestamp = datetime.datetime.today().strftime('%Y-%m-%d')
        >>> meta.xml = "test xml"
        >>> meta.stdname = "IDO  1915"
        >>> o['test_id'] = meta
        >>> list = insert._convert_to_metadata(o)
        >>> len(list) == 1
        True

        """
        index = 0
        list_to_return = []
        for input in data.values():
            metadata = Metadata(id_metadata=self.id_metadata + index, identifier=input.identifier,
                                datestamp=input.datestamp, xml_text=input.xml, stdname=input.stdname, xml=input.xml)
            list_to_return.append(metadata)
            index += 1
        return list_to_return

    def _convert_to_dataidentification(self, data):
        """
        convert OrderedDict to collections.Declaratives.DataIdentification
        :param data: type of collectionOrderedDict in oswlib. the data to convert.
        :return:list of Declarative.DataIdentification

        >>> insert = Insertion(Source())
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identification = MD_DataIdentification()
        >>> meta.identification.identtype = "test identype"
        >>> o['test_id'] = meta
        >>> list = insert._convert_to_dataidentification(o)
        >>> len(list) == 1
        True
        """
        index = 0
        list_to_return = []
        for input in data.values():
            dataindentification = Dataidentification(id_dataidentification=self.id_metadata + index,
                                                     identtype=input.identification.identtype)
            list_to_return.append(dataindentification)
            index += 1
        return list_to_return

    def _convert_to_keyword(self, data):
        """
        convert OrderedDict to collections.Declaratives.Keyword
        :param data:type of collectionOrderedDict in oswlib. the data to convert.
        :return: list of Declarative.Keyword
        Test:
        >>> tabKeyword = [{'keywords': ['Usage des sols'], 'type': None, 'thesaurus': {'date': '2008-06-01',
        ...    'datetype': 'publication', 'title': 'GEMET - INSPIRE themes, version 1.0'}},
        ...    {'keywords': ['Risque / Zonages Risque naturel'], 'type': 'theme', 'thesaurus': {'date': '2009-12-03',
        ...     'datetype': 'publication', 'title': 'Arborescence COVADIS'}},
        ...    {'keywords': ['RESSOURCES - NUISANCES/NUISANCE/source de nuisance (inclut risques industriels et naturels)'],
        ...    'type': 'theme', 'thesaurus': {'date': '2009', 'datetype': 'publication',
        ...    'title': u'Th\xc3\xa9saurus URBAMET'}}, {'keywords': [u'Donn\xe9es vecteur'], 'type': 'stratum',
        ...    'thesaurus': {'date': None, 'datetype': None, 'title': None}}]
        >>> insert = Insertion(Source())
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identification = MD_DataIdentification()
        >>> meta.identification.keywords = tabKeyword
        >>> o['test_id'] = meta
        >>> list = insert._convert_to_keyword(o)
        >>> len(list) == 4
        True

        """
        index_keywords = 0;
        list_keywords = [input.identification.keywords for input in data.values()]
        list_to_return = []
        id_keyword = 0

        for index, liste in enumerate(list_keywords):

            for dico in liste:

                for keyword in dico['keywords']:
                    id_k = self.id_keyword + index_keywords
                    id_data = self.id_metadata + index
                    shorttype = None
                    shortkeywords = None
                    thesaurus_title = None
                    if 'type' in dico.keys():
                        if dico['type'] is not None:
                            shorttype = dico['type'][:50]

                    if keyword is not None:
                        shortkeywords = keyword[:250]

                    if 'thesaurus' in dico.keys():

                        if 'title' in dico['thesaurus'].keys():
                            thesaurus_title = dico['thesaurus']['title']

                    key = Keyword(id_keyword=id_k, keywords=shortkeywords, thesaurus_title=thesaurus_title,
                                  type=shorttype, id_dataidentification=id_data)
                    list_to_return.append(key)
                    index_keywords += 1

        self.id_keyword += index_keywords

        return list_to_return

    def _convert_to_geographicboundingbox(self, data):
        """
        convert OrderedDict to collections.Declaratives.GeographicBoundingBox
        :param data: type of collectionOrderedDict in oswlib. the data to convert.
        :return: list of Declarative.GeographicboundingBox


        >>> insert = Insertion(Source())
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identification = MD_DataIdentification()
        >>> meta.identification.bbox = EX_GeographicBoundingBox()
        >>> meta.identification.bbox.maxx = 5
        >>> meta.identification.bbox.maxy = 10
        >>> meta.identification.bbox.minx = 0
        >>> meta.identification.bbox.miny = 0
        >>> o['test_id'] = meta
        >>> list = insert._convert_to_geographicboundingbox(o)
        >>> len(list) == 1
        True
        """
        index = 0
        list_to_return = []
        for input in data.values():
            geo = Geographicboundingbox(id_geographicboundingbox=index + self.id_metadata,
                                        maxx=input.identification.bbox.maxx,
                                        maxy=input.identification.bbox.maxy,
                                        minx=input.identification.bbox.minx,
                                        miny=input.identification.bbox.miny)
            list_to_return.append(geo)
            index += 1

        return list_to_return

    def _convert_to_responsibleparty(self, data):
        """
        convert OrderedDict to collections.Declaratives.ResponsibleParty
        :param data: type of collectionOrderedDict in oswlib (the data to convert).
        :return: list of Declarative.ResponsibleParty


        >>> insert = Insertion(Source())
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identification = MD_DataIdentification()
        >>> meta.identification.contact = []
        >>> ci = CI_ResponsibleParty()
        >>> ci.city= 'Bordeaux'
        >>> ci.role = 'nothing'
        >>> meta.identification.contact.append(ci)
        >>> meta.identification.contributor = []
        >>> meta.identification.contributor.append(ci)
        >>> meta.identification.creator = []
        >>> meta.identification.creator.append(ci)
        >>> o['test_id'] = meta
        >>> list = insert._convert_to_responsibleparty(o)
        >>> len(list) == 6
        True
        """
        index = 0
        list_to_return = []
        for input in data.values():
            for elt in self.extract_contacts(input,index):
                list_to_return.append(elt)

            for elt in self.extract_contributors(input,index):
                list_to_return.append(elt)

            for elt in self.extract_creators(input,index):
                list_to_return.append(elt)

            index += 1

        return list_to_return

    def extract_contacts(self,input,index):
        """
        Get contacts of Collections.MD_Metadata.
        ( see documentation of OSWLIB for more information to this type)
        and convert to Collections.Declaratives.contact

        :param input:  MD_Metadata
        :return : list of declaratives.Contact
        """
        list_to_return = []
        for contact in input.identification.contact:
            responsibleparty = Responsibleparty(id_responsibleparty=self.id_responsibleparty, city=contact.city,
                                                role=contact.role, organization=contact.organization)
            c = Contact(id_dataidentification=self.id_metadata + index,
                        id_responsibleparty=self.id_responsibleparty, type_contact='contact')
            self.id_responsibleparty += 1
            list_to_return.append(responsibleparty)
            list_to_return.append(c)
        return list_to_return

    def extract_creators(self,input,index):
        """
        Get creator of Collections.MD_Metadata.
        ( see documentation of OSWLIB for more information to this type)
        and convert to Collections.Declaratives.creator

        :param input:  MD_Metadata
        :return list of declaratives.creator

        """
        list_to_return = []
        for creator in input.identification.creator:
            responsibleparty = Responsibleparty(id_responsibleparty=self.id_responsibleparty, city=creator.city,
                                                role=creator.role, organization=creator.organization)
            contact = Contact(id_dataidentification=self.id_metadata + index,
                              id_responsibleparty=self.id_responsibleparty, type_contact='creator')
            self.id_responsibleparty += 1
            list_to_return.append(responsibleparty)
            list_to_return.append(contact)
        return list_to_return

    def extract_contributors(self,input,index):
        """
        Get contributors of Collections.MD_Metadata.
        ( see documentation of OSWLIB for more information to this type)
        and convert to Collections.Declaratives.contributor

        :param input: MD_Metadata.
        :return list of declaratives.contributor

        """
        list_to_return = []
        for contributor in input.identification.contributor:
            responsibleparty = Responsibleparty(id_responsibleparty=self.id_responsibleparty, city=contributor.city,
                                                role=contributor.role, organization=contributor.organization)
            c = Contact(id_dataidentification=self.id_metadata + index,
                        id_responsibleparty=self.id_responsibleparty, type_contact='contributor')
            self.id_responsibleparty += 1
            list_to_return.append(responsibleparty)
            list_to_return.append(c)
        return list_to_return

    def _convert_to_extraction(self, data):
        """
        convert OrderedDict to collections.Declaratives.Extraction
        :param data: type of collectionOrderedDict in oswlib. the data to convert.
        :return: list of Declarative.Extraction

        Test:
        >>> insert = Insertion(Source())
        >>> o = OrderedDict()
        >>> meta = MD_Metadata()
        >>> meta.identification = MD_DataIdentification()
        >>> o['test_id'] = meta
        >>> list = insert._convert_to_extraction(o)
        >>> len(list) == 1
        True
        """
        global_data = GlobalData.get_instance()
        options = global_data.get_options()
        index = 0
        list_to_return = []

        for input in data.values():
            extract = Extraction(id_sdi=self.source.num_idg, id_metadata=self.id_metadata + index,
                                 date_extraction=options.date)
            list_to_return.append(extract)
            index += 1
        return list_to_return

    def _convert_to_sdi(self):
        """

        convert OrderedDict to collections.Declaratives.SDI
        Warning to test this function source.num correspond to primarykey in table sdi.
        :return: list of Declarative.SDI

        Test:

        >>> source = Source(123456789, "nameTest", 0, 0, 30, "TestUrl", "testUrl_csw")
        >>> insert = Insertion(source)
        >>> num = insert.session.query(SDI).delete()
        >>> insert.session.commit()
        >>> list = insert._convert_to_sdi()
        >>> len(list) == 1
        True
        >>> insert.session.add(list[0])
        >>> insert.session.commit()
        >>> list = insert._convert_to_sdi()
        >>> len(list) == 0
        True
        >>> num = insert.session.query(SDI).delete()
        >>> insert.session.commit()

        """
        if self.source is not None:

            if self.session.query(SDI).filter(SDI.id_sdi == self.source.num_idg).count() == 0:
                return [SDI(id_sdi=self.source.num_idg, name=self.source.name_idg, url=self.source.url_idg,
                            url_csw=self.source.url_csw)]

        return []


if __name__ == "__main__":
    import doctest
    doctest.testmod()
