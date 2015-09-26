# -*- coding: utf-8 -*-
u"""

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement
from pyproj import Proj

from .models import Polygon, Line, Point
from .classifier import ToponymClassifier

class Gazetteer(object):
    """Generic Gazetteer object that creates the database connection.
    """
    
    def __init__(self, sqlalchemy_url):
        engine = create_engine(sqlalchemy_url)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.proj = Proj('+init=EPSG:3857')
        self.classifier = ToponymClassifier()
    
    def query(self, query):
        """Runs the given query against the database and returns those
        toponyms that can be classifed using the classifier module.
        """
        toponyms = []
        for toponym in query:
            classification = self.classifier(toponym)
            if classification:
                toponyms.append((toponym, classification))
        return toponyms
        

class ContainmentGazetteer(Gazetteer):
    """Handles containment queries.
    """

    def __init__(self, sqlalchemy_url):
        Gazetteer.__init__(self, sqlalchemy_url)
        
    def __call__(self, point):
        """Retrieves the full containment hierarchy for the point (WGS84 lon/lat).
        """
        coords = self.proj(*point)
        toponyms = self.query(self.session.query(Polygon).filter(and_(Polygon.name != '',
                                                                      Polygon.way.ST_Contains(WKTElement('POINT(%f %f)' % coords,
                                                                                                         srid=900913)))))
        toponyms.sort(key=lambda i: float(i[0].tags['way_area']))
        return toponyms


class ProximalGazetteer(Gazetteer):
    """Handles proximal queries.
    """
    
    def __call__(self, point, containment):
        coords = self.proj(*point)
        for dist in [400, 3000]:
            toponyms = []
            for toponym, classification in self.query(self.session.query(Polygon).filter(and_(Polygon.name != '',
                                                                                              Polygon.way.ST_DWithin(WKTElement('POINT(%f %f)' % coords,
                                                                                                                                srid=900913),
                                                                                                                     dist)))):
                toponyms.append((toponym, classification))
            for toponym, classification in self.query(self.session.query(Line).filter(and_(Line.name != '',
                                                                                           Line.way.ST_DWithin(WKTElement('POINT(%f %f)' % coords,
                                                                                                                          srid=900913),
                                                                                                               dist)))):
                toponyms.append((toponym, classification))
            for toponym, classification in self.query(self.session.query(Point).filter(and_(Point.name != '',
                                                                                            Point.way.ST_DWithin(WKTElement('POINT(%f %f)' % coords,
                                                                                                                            srid=900913),
                                                                                                                 dist)))):
                toponyms.append((toponym, classification))
            if toponyms:
                return toponyms
        return []
