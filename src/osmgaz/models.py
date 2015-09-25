# -*- coding: utf-8 -*-
u"""

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
from sqlalchemy import Column, Integer, Numeric, Unicode
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class Polygon(Base):
    
    __tablename__ = 'planet_osm_polygon'
    
    osm_id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    z_order = Column(Integer)
    way_area = Column(Numeric)
    way = Column(Geometry(srid=900913))
    tags = Column(HSTORE)
