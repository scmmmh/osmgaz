# -*- coding: utf-8 -*-
"""

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
from sqlalchemy import Column, Integer, Numeric, Unicode, UnicodeText, create_engine, text
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class Polygon(Base):
    
    __tablename__ = 'planet_osm_polygon'
    
    gid = Column(Integer, primary_key=True)
    osm_id = Column(Integer)
    name = Column(Unicode)
    z_order = Column(Integer)
    way_area = Column(Numeric)
    way = Column(Geometry(srid=900913))
    tags = Column(HSTORE)
    classification = Column(Unicode(255))


class Line(Base):
    
    __tablename__ = 'planet_osm_line'
    
    gid = Column(Integer, primary_key=True)
    osm_id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    z_order = Column(Integer)
    way_area = Column(Numeric)
    way = Column(Geometry(srid=900913))
    tags = Column(HSTORE)
    classification = Column(Unicode(255))


class Point(Base):
    
    __tablename__ = 'planet_osm_point'
    
    gid = Column(Integer, primary_key=True)
    osm_id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    z_order = Column(Integer)
    way = Column(Geometry(srid=900913))
    tags = Column(HSTORE)
    classification = Column(Unicode(255))


class NameSalienceCache(Base):
    
    __tablename__ = 'name_salience_cache'
    
    id = Column(Integer, primary_key=True)
    category = Column(Unicode(255))
    toponym_id = Column(Integer)
    container_id = Column(Integer)
    salience = Column(Numeric)


class TypeSalienceCache(Base):
    
    __tablename__ = 'type_salience_cache'
    
    id = Column(Integer, primary_key=True)
    toponym_type = Column(Unicode(255))
    container_id = Column(Integer)
    salience = Column(Numeric)


class FlickrSalienceCache(Base):
    
    __tablename__ = 'flickr_salience_cache'
    
    id = Column(Integer, primary_key=True)
    toponym_id = Column(Integer)
    salience = Column(Numeric)


class LookupCache(Base):
    
    __tablename__ = 'lookup_cache'
    
    id = Column(Integer, primary_key=True)
    point = Column(Unicode(255))
    data = Column(UnicodeText)


def setup_db(args):
    """Alter the existing OSM database and create the cache tables."""
    engine = create_engine(args.sqla_url)
    ALTER_STATEMENTS = ['ALTER TABLE planet_osm_polygon ADD COLUMN gid SERIAL',
                        'ALTER TABLE planet_osm_polygon ADD COLUMN classification VARCHAR(255)',
                        'ALTER TABLE planet_osm_line ADD COLUMN gid SERIAL',
                        'ALTER TABLE planet_osm_line ADD COLUMN classification VARCHAR(255)',
                        'ALTER TABLE planet_osm_point ADD COLUMN gid SERIAL',
                        'ALTER TABLE planet_osm_point ADD COLUMN classification VARCHAR(255)']
    for statement in ALTER_STATEMENTS:
        try:
            print('Running: %s' % statement)
            engine.execute(text(statement))
        except Exception as e:
            print(e)
    print('Creating cache tables')
    Base.metadata.create_all(engine)
