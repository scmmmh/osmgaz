# -*- coding: utf-8 -*-
u"""

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement
from pyproj import Proj

from .models import Polygon
from .classifier import classify
from .filters import ContainmentFilter

class Gazetteer(object):
    """Generic Gazetteer object that creates the database connection.
    """
    
    def __init__(self, sqlalchemy_url):
        engine = create_engine(sqlalchemy_url)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.proj = Proj('+init=EPSG:3857')
    

class ContainmentGazetteer(Gazetteer):
    """Handles containment queries.
    """

    def __init__(self, sqlalchemy_url):
        Gazetteer.__init__(self, sqlalchemy_url)
        self.filter = ContainmentFilter(self)
        
    def query(self, query):
        """Runs the given query against the database and returns those
        toponyms that can be classifed using the classifier module.
        """
        toponyms = []
        for toponym in query:
            classification = classify(toponym)
            if classification:
                toponyms.append((toponym, classification))
        return toponyms
        
    def __call__(self, point):
        """Retrieves the containment hierarchy for the point (WGS84 lon/lat).
        Returns the filtered list of toponyms using the ContainmentFilter.
        """
        coords = self.proj(*point)
        toponyms = self.query(self.session.query(Polygon).filter(and_(Polygon.name != '',
                                                                      Polygon.way.ST_Contains(WKTElement('POINT(%f %f)' % coords,
                                                                                                         srid=900913)))))
        toponyms.sort(key=lambda i: float(i[0].tags['way_area']))
        return self.filter(toponyms)
