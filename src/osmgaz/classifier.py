# -*- coding: utf-8 -*-
u"""
The classifier applies rules to the OSM tags to determine geography type hierarchies,
which are returned as a list. An empty list means nothing matched

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
from copy import deepcopy

# Administrative Mapping Rules
rules = [{'rules': {'boundary': 'administrative',
                    'admin_level': '2'},
          'type': ['AREA', 'ADMINISTRATIVE', '2']},
         {'rules': {'boundary': 'administrative',
                    'admin_level': '4'},
          'type': ['AREA', 'ADMINISTRATIVE', '4']},
         {'rules': {'boundary': 'administrative',
                    'admin_level': '6'},
          'type': ['AREA', 'ADMINISTRATIVE', '6']},
         {'rules': {'boundary': 'administrative',
                    'admin_level': '8'},
          'type': ['AREA', 'ADMINISTRATIVE', '8']},
         {'rules': {'boundary': 'administrative',
                    'admin_level': '10'},
          'type': ['AREA', 'ADMINISTRATIVE', '10']},
         {'rules': {'boundary': 'ceremonial'},
          'type': ['AREA', 'CEREMONIAL']}]

# Park Mapping Rules
rules.extend([{'rules': {'boundary': 'national_park'},
               'type': ['AREA', 'PARK', 'NATIONAL PARK']},
              {'rules': {'landuse': 'recreation_ground'},
               'type': ['AREA', 'PARK', 'RECREATION GROUND']},
              {'rules': {'leisure': 'nature_reserve'},
               'type': ['AREA', 'NATURE RESERVE']},
              {'rules': {'leisure': 'park'},
               'type': ['AREA', 'PARK']}])

# Natural Areas Mapping Rules
rules.extend([{'rules': {'landuse': 'forest',
                         'wood': 'coniferous'},
               'type': ['AREA', 'FOREST', 'CONIFEROUS']},
              {'rules': {'landuse': 'forest'},
               'type': ['AREA', 'FOREST']},
              {'rules': {'waterway': 'stream'},
               'types': ['AREA', 'WATER', 'FLOWING WATER', 'STREAM']},
              {'rules': {'natural': 'water'},
               'type': ['AREA', 'WATER'],
               'warn': True}])

# Building Rules
rules.extend([{'rules': {'building': 'yes', 'amenity': 'pub'},
               'type': ['BUILDING', 'COMMERCIAL', 'FOOD AND DRINK', 'PUB']},
              {'rules': {'building': 'yes', 'amenity': 'supermarket'},
               'type': ['BUILDING', 'COMMERCIAL', 'FOOD AND DRINK', 'SUPERMARKET']},
              {'rules': {'building': 'yes', 'shop': 'supermarket'},
               'type': ['BUILDING', 'COMMERCIAL', 'FOOD AND DRINK', 'SUPERMARKET']},
              {'rules': {'building': 'yes', 'tourism': 'hotel'},
               'type': ['BUILDING', 'COMMERCIAL', 'HOTEL']},
              {'rules': {'building': 'apartments'},
               'type': ['BUILDING', 'RESIDENTIAL', 'APARTMENTS']},
              {'rules': {'building': 'greenhouse'},
               'type': ['BUILDING', 'AGRICULTURAL', 'GREENHOUSE']},
              {'rules': {'landuse': 'farmyard'},
               'type': ['BUILDING', 'AGRICULTURAL', 'FARM']},
              {'rules': {'building': 'yes'}, # Generic Building
               'type': ['BUILDING', 'UNCLASSIFIED'],
               'warn': True}])

# Object Rules
rules.extend([{'rules': {'building': 'ship', 'historic': 'ship'},
               'type': ['OBJECT', 'ARTIFICIAL', 'SHIP', 'HISTORIC']}])

# Road Rules
rules.extend([{'rules': {'highway': 'trunk'},
               'type': ['TRANSPORT', 'ROAD', 'TRUNK']},
              {'rules': {'highway': 'primary'},
               'type': ['TRANSPORT', 'ROAD', 'PRIMARY']},
              {'rules': {'highway': 'secondary'},
               'type': ['TRANSPORT', 'ROAD', 'SECONDARY']},
              {'rules': {'highway': 'tertiary'},
               'type': ['TRANSPORT', 'ROAD', 'TERTIARY']},
              {'rules': {'highway': 'service'},
               'type': ['TRANSPORT', 'ROAD', 'SERVICE']},
              {'rules': {'highway': 'residential'},
               'type': ['TRANSPORT', 'ROAD', 'RESIDENTIAL']},
              {'rules': {'highway': 'unclassified'},
               'type': ['TRANSPORT', 'ROAD', 'UNCLASSIFIED']},
              {'rules': {'railway': 'rail', 'electrified': 'no'},
               'types': ['TRANSPORT', 'RAIL', 'NOT ELECTRIFIED']},
              {'rules': {'highway': 'path'},
               'types': ['TRANSPORT', 'PATH']},
              {'rules': {'highway': 'pedestrian'},
               'types': ['TRANSPORT', 'PATH', 'FOOT']},
              {'rules': {'highway': 'footway'},
               'types': ['TRANSPORT', 'PATH', 'FOOT']},
              {'rules': {'natural': 'water',
                         'water': 'canal'},
               'type': ['AREA', 'WATER', 'WATER WAY', 'CANAL']},
              {'rules': {'waterway': 'canal'},
               'type': ['AREA', 'WATER', 'WATER WAY', 'CANAL']},
              {'rules': {'waterway': 'dock'},
               'type': ['AREA', 'WATER', 'WATER WAY', 'DOCK']},
              ])

# Ignore these toponyms
rules.extend([{'rules': {'public_transport': 'pay_scale_area'}},
              {'rules': {'boundary': 'vice_county'}},
              {'rules': {'boundary': 'administrative', 'admin_level': '5'}},
              {'rules': {'route': 'train'}},
              {'rules': {'route': 'foot'}},
              {'rules': {'route': 'bus'}}])

UNKNOWN = []

def classify(toponym):
    for rule in rules:
        matches = True
        for key, value in rule['rules'].items():
            if key not in toponym.tags or toponym.tags[key] != value:
                matches = False
        if matches:
            if 'type' in rule:
                if('warn' in rule and rule['warn']):
                    UNKNOWN.append(toponym.tags)
                return {'type': deepcopy(rule['type'])}
            else:
                return None
    UNKNOWN.append(toponym.tags)
    return None

def output_unknown():
    for tags in UNKNOWN:
        print(tags)
    UNKNOWN.clear()
