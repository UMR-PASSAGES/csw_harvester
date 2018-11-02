"""

Declarative allows to have a virtual object of the database on top of the csw-harvester database
in order to facilitate the manage of this during the harvesting.

"""
from sqlalchemy import Column, ForeignKey, Integer, String, Text, Date, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()


class SDI(Base):
    __tablename__ = 'sdi'
    #__table_args__ = {'schema' : 'schema1'}
    id_sdi = Column(Integer, primary_key=True)
    name = Column(String(250))
    url = Column(String(250))
    url_csw = Column(String(250))


class Geographicboundingbox(Base):
    __tablename__ = "geographicboundingbox"
    id_geographicboundingbox = Column(Integer, primary_key=True)
    maxx = Column(Text)
    maxy = Column(Text)
    minx = Column(Text)
    miny = Column(Text)


class Responsibleparty(Base):
    __tablename__ = "responsibleparty"
    id_responsibleparty = Column(BigInteger, primary_key=True)
    city = Column(String(150))
    role = Column(String(150))
    organization = Column(String(150))


class Dataidentification(Base):
    __tablename__= "dataidentification"
    id_dataidentification = Column(BigInteger, primary_key=True, nullable=False)
    identtype = Column(String(150))


class Contact(Base):
    __tablename__ = "contact"
    id_dataidentification = Column(Integer, ForeignKey("dataidentification.id_dataidentification"), primary_key=True)
    id_responsibleparty = Column(BigInteger, ForeignKey("responsibleparty.id_responsibleparty"), primary_key=True)
    type_contact = Column(String(150))

class Keyword(Base):
    __tablename__ = "keyword"
    id_keyword = Column(Integer, primary_key=True)
    keywords = Column(String(250))
    thesaurus_title = Column(String(250))
    type = Column(String(100))
    id_dataidentification = Column(BigInteger, ForeignKey("dataidentification.id_dataidentification"), nullable=False)


class Metadata(Base):
    __tablename__ = "metadata"
    id_metadata = Column(Integer, primary_key=True)
    identifier = Column(String(2000))
    datestamp = Column(Date)
    xml_text = Column(Text)
    stdname = Column(String(100))
    xml = Column(Text , nullable=True)

class Extraction(Base):
    __tablename__ = "extraction"
    id_sdi = Column(Integer, ForeignKey("sdi.id_sdi"), primary_key=True)
    id_metadata = Column(Integer, ForeignKey("metadata.id_metadata"), primary_key=True)
    date_extraction = Column(Date , primary_key=True)
