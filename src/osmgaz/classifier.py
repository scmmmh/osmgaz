# -*- coding: utf-8 -*-
"""
The classifier applies rules to the OSM tags to determine geography type hierarchies,
which are returned as a list. An empty list means nothing matched

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
import json
import logging

from copy import deepcopy
from geoalchemy2 import shape
from httplib2 import Http
from pkg_resources import resource_stream
from pyproj import Proj
from rdflib import Graph, URIRef
from rdflib.namespace import RDFS
from shapely import geometry
from sqlalchemy import create_engine, and_, not_
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlencode

from .filters import type_match
from .models import (Point, Line, Polygon, NameSalienceCache, TypeSalienceCache, FlickrSalienceCache)


class UrbanRuralClassifier(object):
    """Urban/Rural classification based on whether there is a building within
    400m. 
    """
    
    def __init__(self):
        self.proj = Proj('+init=EPSG:3857')
    
    def __call__(self, point, toponyms):
        point = geometry.Point(*self.proj(*point))
        for toponym, type_ in toponyms:
            if point.distance(shape.to_shape(toponym.way)) <= 400 and type_match(type_['type'], ['ARTIFICIAL FEATURE', 'BUILDING']):
                return 'URBAN'
        return 'RURAL'


class ToponymClassifier(object):
    """Classifies toponyms based on the rules defined in the ontology.
    """
    
    def __init__(self):
        self.load_rules()
        self.unknown = []
    
    def load_rules(self):
        """Load the classification rules from the ontology.
        """
        def parse_tree(parent, ontology, path):
            for child, _, _ in ontology.triples((None, RDFS.subClassOf, parent)):
                osm_type = path
                for _, _, label in ontology.triples((child, RDFS.label, None)):
                    osm_type = path + [str(label).upper()]
                    break
                parse_tree(child, ontology, osm_type)
                try:
                    extra_settings = None
                    for _, _, definition in ontology.triples((child, URIRef('http://work.room3b.eu/ontology/osm_types#extra_settings'), None)):
                        extra_settings = json.loads(definition)
                    for _, _, definition in ontology.triples((child, RDFS.isDefinedBy, None)):
                        rules = json.loads(definition)
                        if isinstance(rules, dict):
                            rule = {'rules': rules, 'type': osm_type}
                            if extra_settings:
                                rule.update(extra_settings)
                            self.rules.append(rule)
                        elif isinstance(rules, list):
                            for rule in rules:
                                rule = {'rules': rule,
                                        'type': osm_type}
                                if extra_settings:
                                    rule.update(extra_settings)
                                self.rules.append(rule)
                except:
                    print(definition)
        logging.debug('Loading classification rules')
        self.rules = []
        ontology = Graph()
        ontology.load(resource_stream('osmgaz', 'rules.rdf'))
        parse_tree(URIRef('http://work.room3b.eu/ontology/osm_types#OSM'), ontology, [])
        self.rules.sort(key=lambda r: (len(r['rules']), len(r['type'])), reverse=True)
        
        # Add static ignore rules
        for rule in [{'public_transport': 'pay_scale_area'},
                     {'boundary': 'vice_county'},
                     {'boundary': 'political'},
                     {'boundary': 'administrative', 'admin_level': '5'},
                     {'boundary': 'police'},
                     {'boundary': 'statistical'},
                     {'boundary': 'protected_area'},
                     {'boundary': 'toll'},
                     {'route': 'train'},
                     {'route': 'foot'},
                     {'route': 'bus'},
                     {'route': 'railway'},
                     {'route': 'bicycle'},
                     {'route': 'hiking'},
                     {'route': 'mtb'},
                     {'route': 'ferry'},
                     {'route': 'tram'},
                     {'route': 'road'},
                     {'power': 'line'},
                     {'wpt_symbol': 'Waypoint'},
                     {'wpt_symbol': 'Crossing'},
                     {'amenity': 'dog_agility_obstacle'},
                     {'building': 'entrance'},
                     {'entrance': 'residence'},
                     {'amenity': 'vending_machine'},
                     {'amenity': 'charging_station'},
                     {'amenity': 'postbox'},
                     {'pipeline': 'inspection_chamber'},
                     {'place': 'subdivision'},
                     {'public_transport': 'stop_position'}]:
            self.rules.insert(0, {'rules': rule})
    
    def get_unknown(self):
        """Return the list of all unknown toponym types.
        """
        result = self.unknown
        self.unknown = []
        return result
    
    def log_unknown(self, tags, match_tags=None):
        """Helper function that filters out some non-relevant tags and then only logs those
        tags as unknown that are distinct from that set.
        """
        for kw in ['name', 'phone', 'wikipedia', 'route_name', 'route_pref_color', 'way_area', 'building:part',
                   'website', 'wheelchair', 'postal_code', 'license_notice', 'description', 'operator', 'alt_name',
                   'email', 'old_name', 'date', 'opening_hours', 'genus', 'inscription', 'url', 'height', 'ref',
                   'direction', 'is_in', 'species', 'wpt_description', 'wpt_symbol', 'ele', 'capacity', 'occupier',
                   'polling_station', 'layer']:
            if kw in tags:
                del tags[kw]
        for tag in list(tags):
            for kw in ['addr:', 'name:', 'building:', 'roof:', 'disused:', 'ref:', 'is_in:', 'contact:', 'date:',
                       'genus:', 'seamark:']:
                if tag.startswith(kw):
                    del tags[tag]
            if match_tags and tag in match_tags:
                del tags[tag]
        if len(tags) > 0:
            if tags not in self.unknown:
                if match_tags:
                    self.unknown.append((tags, match_tags))
                    logging.debug(json.dumps(tags) + ' - ' + json.dumps(match_tags))
                else:
                    self.unknown.append(tags)
                    logging.debug(json.dumps(tags))
    
    def __call__(self, toponym):
        """Apply the classification rules to the given toponym.
        """
        logging.debug('Classifying %s' % toponym.name)
        for rule in self.rules:
            matches = True
            for key, value in rule['rules'].items():
                if key not in toponym.tags or toponym.tags[key] != value:
                    matches = False
            if matches:
                if 'type' in rule:
                    if('warn' in rule and rule['warn']):
                        self.log_unknown(deepcopy(toponym.tags), deepcopy(rule['rules']))
                    return {'type': deepcopy(rule['type'])}
                else:
                    return None
        self.log_unknown(deepcopy(toponym.tags))
        return None


class NameSalienceCalculator(object):
    """Calculates the uniqueness of the given name within the container.
    """
    
    def __init__(self, session):
        self.session = session

    def __call__(self, toponym, classification, containers):
        logging.debug('Calculating name salience for %s in %s' % (toponym.name, containers[0][0].name))
        cache = self.session.query(NameSalienceCache).filter(and_(NameSalienceCache.category == type(toponym).__name__,
                                                                  NameSalienceCache.toponym_id == toponym.gid,
                                                                  NameSalienceCache.container_id == containers[0][0].gid)).first()
        if cache:
            return cache.salience
        if type_match(classification['type'], ['ARTIFICIAL FEATURE', 'TRANSPORT', 'PUBLIC']):
            count = self.session.query(Point).filter(and_(Point.name == toponym.name,
                                                          Point.way.ST_DWithin(containers[0][0].way, 400))).count()
            count = count + self.session.query(Line).filter(and_(Line.name == toponym.name,
                                                                 Line.way.ST_DWithin(containers[0][0].way, 400))).count()
            count = count + self.session.query(Polygon).filter(and_(Polygon.name == toponym.name,
                                                                    Polygon.way.ST_DWithin(containers[0][0].way, 400))).count()
        else:
            count = self.session.query(Point).filter(and_(Point.name == toponym.name,
                                                          not_(Point.classification.startswith('ARTIFICIAL FEATURE::TRANSPORT::PUBLIC')),
                                                          Point.way.ST_DWithin(containers[0][0].way, 400))).count()
            count = count + self.session.query(Line).filter(and_(Line.name == toponym.name,
                                                                 not_(Line.classification.startswith('ARTIFICIAL FEATURE::TRANSPORT::PUBLIC')),
                                                                 Line.way.ST_DWithin(containers[0][0].way, 400))).count()
            count = count + self.session.query(Polygon).filter(and_(Polygon.name == toponym.name,
                                                                    not_(Polygon.classification.startswith('ARTIFICIAL FEATURE::TRANSPORT::PUBLIC')),
                                                                    Polygon.way.ST_DWithin(containers[0][0].way, 400))).count()
        salience = 0
        if count > 0:
            salience = 1.0 / count
        self.session.add(NameSalienceCache(category=type(toponym).__name__,
                                           toponym_id=toponym.gid,
                                           container_id=containers[0][0].gid,
                                           salience=salience))
        self.session.commit()
        return salience


class TypeSalienceCalculator(object):
    """Calculates the uniqueness of the given toponym type within the container.
    """
    
    def __init__(self, session):
        self.session = session
        self.classifier = ToponymClassifier()

    def __call__(self, type_, containers):
        type_ = '::'.join(type_['type'])
        logging.debug('Calculating type salience for %s in %s' % (type_, containers[0][0].name))
        cache = self.session.query(TypeSalienceCache).filter(and_(TypeSalienceCache.toponym_type == type_,
                                                                  TypeSalienceCache.container_id == containers[0][0].gid)).first()
        if cache:
            return cache.salience
        count = self.session.query(Point).filter(and_(Point.classification.startswith(type_),
                                                      Point.way.ST_DWithin(containers[0][0].way, 400))).count()
        count = count + self.session.query(Line).filter(and_(Line.classification.startswith(type_),
                                                             Line.way.ST_DWithin(containers[0][0].way, 400))).count()
        count = count + self.session.query(Polygon).filter(and_(Polygon.classification.startswith(type_),
                                                                Polygon.way.ST_DWithin(containers[0][0].way, 400))).count()
        salience = 0
        if count > 0:
            salience = 1.0 / count
        self.session.add(TypeSalienceCache(toponym_type=type_,
                                           container_id=containers[0][0].gid,
                                           salience=salience))
        self.session.commit()
        return salience


class FlickrSalienceCalculator(object):
    """Calculates the salience of a set of toponyms based on Flickr photograph use.
    """
    
    def __init__(self, session):
        self.session = session
        self.classifier = ToponymClassifier()
        self.proj = Proj('+init=EPSG:3857')

    def __call__(self, toponym, urban_rural):
        logging.debug('Calculating flickr salience for %s' % toponym.name)
        cache = self.session.query(FlickrSalienceCache).filter(FlickrSalienceCache.toponym_id == toponym.gid).first()
        if cache is not None:
            return int(cache.salience)
        else:
            try:
                centroid = shape.to_shape(toponym.way).centroid
                centroid = self.proj(centroid.x, centroid.y, inverse=True)
                http = Http()
                if urban_rural == 'URBAN':
                    distance = '0.4'
                else:
                    distance = '3'
                _, data = http.request('https://api.flickr.com/services/rest/?%s' % urlencode({'api_key': 'f9394a32075e4ee39605c618e65dce4a',
                                                                                               'method': 'flickr.photos.search',
                                                                                               'text': '"%s"' % toponym.name,
                                                                                               'lat': '%f' % centroid[1],
                                                                                               'lon': '%f' % centroid[0],
                                                                                               'radius': distance,
                                                                                               'format': 'json',
                                                                                               'nojsoncallback': '1'}))
                data = json.loads(data.decode('utf-8'))
                self.session.add(FlickrSalienceCache(toponym_id=toponym.gid,
                                                     salience=int(data['photos']['total'])))
                self.session.commit()
                return int(data['photos']['total'])
            except:
                return 0
