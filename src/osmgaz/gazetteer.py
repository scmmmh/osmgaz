# -*- coding: utf-8 -*-
"""

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
import logging

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement
from pyproj import Proj

from .classifier import ToponymClassifier
from .filters import type_match
from .models import Polygon, Line, Point

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
        classified = 0
        for toponym in query:
            if toponym.classification is None:
                classification = self.classifier(toponym)
                if classification:
                    classified = classified + 1
                    toponym.classification = '::'.join(classification['type'])
                    toponyms.append((toponym, classification))
            else:
                toponyms.append((toponym, {'type': toponym.classification.split('::')}))
        if classified > 0:
            self.session.commit()
        return toponyms
        

class ContainmentGazetteer(Gazetteer):
    """Handles containment queries.
    """

    def __init__(self, sqlalchemy_url):
        Gazetteer.__init__(self, sqlalchemy_url)
        
    def __call__(self, point):
        """Retrieves the full containment hierarchy for the point (WGS84 lon/lat).
        """
        logging.info('Retrieving containment toponyms for %.5f,%.5f' % point)
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
        logging.info('Retrieving proximal toponyms for %.5f,%.5f' % point)
        coords = self.proj(*point)
        toponyms = []
        for dist in [400, 1000, 2000, 3000]:
            logging.debug('Querying within %im' % dist)
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
            if dist == 400:
                for _, type_ in toponyms:
                    if type_match(type_['type'], ['ARTIFICIAL FEATURE', 'BUILDING']):
                        return toponyms
            else:
                if len(toponyms) > 10:
                    return toponyms
        return toponyms
