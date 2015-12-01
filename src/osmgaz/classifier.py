# -*- coding: utf-8 -*-
u"""
The classifier applies rules to the OSM tags to determine geography type hierarchies,
which are returned as a list. An empty list means nothing matched

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
import json
import logging

from copy import deepcopy
from pkg_resources import resource_stream
from rdflib import Graph, URIRef
from rdflib.namespace import RDFS
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from .models import Point, Line, Polygon, NameSalienceCache, TypeSalienceCache

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
        self.rules.extend([{'rules': {'public_transport': 'pay_scale_area'}},
                           {'rules': {'boundary': 'vice_county'}},
                           {'rules': {'boundary': 'administrative', 'admin_level': '5'}},
                           {'rules': {'route': 'train'}},
                           {'rules': {'route': 'foot'}},
                           {'rules': {'route': 'bus'}},
                           {'rules': {'route': 'railway'}},
                           {'rules': {'route': 'bicycle'}},
                           {'rules': {'route': 'hiking'}},
                           {'rules': {'route': 'mtb'}},
                           {'rules': {'route': 'ferry'}},
                           {'rules': {'power': 'line'}},
                           {'rules': {'wpt_symbol': 'Waypoint'}},
                           {'rules': {'wpt_symbol': 'Crossing'}}])
    
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
                   'website', 'wheelchair', 'postal_code', 'license_notice', 'description', 'operator']:
            if kw in tags:
                del tags[kw]
        for tag in list(tags):
            if tag.startswith('addr:') or tag.startswith('name:') or tag.startswith('building:') or tag.startswith('roof:') or tag.startswith('disused:') or tag.startswith('ref:') or tag.startswith('is_in:'):
                del tags[tag]
            if match_tags and tag in match_tags:
                del tags[tag]
        if len(tags) > 0:
            if tags not in self.unknown:
                if match_tags:
                    self.unknown.append((tags, match_tags))
                else:
                    self.unknown.append(tags)
    
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
    
    def __init__(self, sqlalchemy_url):
        engine = create_engine(sqlalchemy_url)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def __call__(self, toponym, containers):
        logging.debug('Calculating name salience for %s in %s' % (toponym.name, containers[0][0].name))
        cache = self.session.query(NameSalienceCache).filter(and_(NameSalienceCache.category == type(toponym).__name__,
                                                                  NameSalienceCache.toponym_id == toponym.gid,
                                                                  NameSalienceCache.container_id == containers[0][0].gid)).first()
        if cache:
            return cache.salience
        count = self.session.query(Point).filter(and_(Point.name == toponym.name,
                                                      Point.way.ST_DWithin(containers[0][0].way, 400))).count()
        count = count + self.session.query(Line).filter(and_(Line.name == toponym.name,
                                                             Line.way.ST_DWithin(containers[0][0].way, 400))).count()
        count = count + self.session.query(Polygon).filter(and_(Polygon.name == toponym.name,
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
    
    def __init__(self, sqlalchemy_url):
        engine = create_engine(sqlalchemy_url)
        Session = sessionmaker(bind=engine)
        self.session = Session()
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
