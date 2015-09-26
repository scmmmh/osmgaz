# -*- coding: utf-8 -*-
u"""
The classifier applies rules to the OSM tags to determine geography type hierarchies,
which are returned as a list. An empty list means nothing matched

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
import json

from copy import deepcopy
from rdflib import Graph, URIRef
from rdflib.namespace import RDFS

class ToponymClassifier(object):
    
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
        self.rules = []
        ontology = Graph()
        ontology.load('src/osmgaz/rules.rdf')
        parse_tree(URIRef('http://work.room3b.eu/ontology/osm_types#OSM'), ontology, [])
        self.rules.sort(key=lambda r: (len(r['rules']), len(r['type'])), reverse=True)
        
        # Add static ignore rules
        self.rules.extend([{'rules': {'public_transport': 'pay_scale_area'}},
                           {'rules': {'boundary': 'vice_county'}},
                           {'rules': {'boundary': 'administrative', 'admin_level': '5'}},
                           {'rules': {'route': 'train'}},
                           {'rules': {'route': 'foot'}},
                           {'rules': {'route': 'bus'}}])
    
    def get_unknown(self):
        result = self.unknown
        self.unknown = []
        return result
    
    def __call__(self, toponym):
        for rule in self.rules:
            matches = True
            for key, value in rule['rules'].items():
                if key not in toponym.tags or toponym.tags[key] != value:
                    matches = False
            if matches:
                if 'type' in rule:
                    #if('warn' in rule and rule['warn']):
                    #    self.unknown.append(toponym.tags)
                    return {'type': deepcopy(rule['type'])}
                else:
                    return None
        self.unknown.append(toponym.tags)
        return None
