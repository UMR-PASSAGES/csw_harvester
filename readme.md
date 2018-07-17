# CSW Harvester

## Synopsis

This is a python script that harvests metadata from CSW web services and saves some information from these metadata in a postgreSQL database.
This script was created by [Mathias Rouan](https://github.com/mrouan) and [Julie Pierson](https://github.com/juliepierson), and completely refactored by Laurent Bouquin, Jeniffer Ortiz Lozano, Julien Massonneau et Abdelwahid Hadj Zoubir.

## Motivation

This script is used to analyze Spatial Data Infrastructures for the GEOBS research project : https://www-iuem.univ-brest.fr/pops/projects/geobs.

## Dependencies

To run the program, you must first install the following dependencies:

- Python 2.7 : The script was programmed with python 2.7, it doesn't work with python 3, https://www.python.org/

- PostgreSQL : The world's most advanced open source database, https://www.postgresql.org/

- SQLAlchemy : The Python SQL Toolkit and Object Relational Mapper, https://www.sqlalchemy.org/

- OWSlib : Python package for client programming with Open Geospatial Consortium (OGC) web service (hence OWS) interface standards, https://github.com/geopython/OWSLib

To run profiling test, you need to install: 

- Snakeviz : Graphical viewer, https://jiffyclub.github.io/snakeviz/

To run coverage test, you need to install: 

- Coverage.py : Tool for measuring code coverage of Python programs, https://coverage.readthedocs.io/en/coverage-4.5.1/

## How to setup and run the program

### Database

The PostgreSQL database must first be created. A database dump is provided with **database/csw_harvester.sql**.	

    psql -f csw_harvester.sql -U postgres


Here is the physical data model of the database:
![Physical Data Model](/database/MPD_csw_harvester.png)

For the program to interact with the database, you will need to specify the following fields in the file **config_database.cfg**:
* dbname
* host
* password
* port
* user
* schema

### Sources file

The CSW list is read from a CSV file, the file structure is described below:
    
    IDG number, name of the IDG, beginning of the recording, end of the recording, step of the recording, IDG URL, CSW URL

 An example is provided with **sources_test.csv**. 
 For each CSW, you can set a start in each step (for example, if set at 30, records will be extracted 30 by 30). Lines can be commented with '#'.

### Program

You can then run the program `python Main.py`
You can specify the following options :

* -f OUTPUTSCHEMA, --outputschema=OUTPUTSCHEMA
    * the outputschema for CSW ; default = http://www.isotc211.org/2005/gmd
* -s SOURCES, --sources=SOURCES
    * the CSV sources files ; default = ../sources/source_test.csv
* -l LOG_FILE, --log-file=LOG_FILE
    * the log files ; default : ../csw-harvester.log
* -d DATE, --date=DATE
    * the extraction date ; default : the current date

The date option is used to force the extraction date stored in the database.

## Tests

* For unit tests, we use the doctest module, to run them, just run the desired file (except Main.py) with python like this:

        python GlobalData.py
    If the test is valid, nothing is returned, otherwise doctest returns an error message with the name of the functions that failed.

* For coverage tests, move to test folder and run:
        
        python Coverage.py
   Coverage test, launch all unit tests

* For profile test, move to the test folder and run:

        python Profiling.py

## Documentation
* For generate documentation you can use pydoc
        
        pydoc -w ../src/ 
    nb: you must launch this comand in src folder
## License

This project is published under the General Public License v3.
