# -*- coding: utf-8 -*-


from ExtractionGeocatalogueController import *
from Insertion import *
from VerificationMetadata import *
from GlobalData import *

def start():
    globalData = GlobalData.get_instance()
    sources = globalData.get_geocatalog_sources()
    for source in sources:
        e = ExtractionGeocatalogue(source)
        listRecords = e.extract_generator_data()
        v = VerificationMetadata(source.num_idg)
        insert = Insertion(source)

        for records in listRecords:
            v.check(records)
            insert.save_in_db(records)


pass

if __name__ == "__main__":
    """
    
    This class extracts geographic information from the ServiceWeb for each instance(geocatalog)of the Source class
    and for that it utilises Extraction's class to traverse the genetator (list of instances of Source). Which returns a
    generator object that contains the set of values for every geocatalog. These Metadata are verified by the method 
    check of the VerificationMetadata's class to ensure it fullfills the requirements necessary for analysis. 
    Once it is verified, metadata is saved in the csw_harvester database with the method saveIndB of the class Insertion
    . 
 
    """
    start()
