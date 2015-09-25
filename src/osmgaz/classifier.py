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
               'type': ['AREA', 'NATURE RESERVE']}])

# Natural Areas Mapping Rules
rules.extend([{'rules': {'landuse': 'forest',
                         'wood': 'coniferous'},
               'type': ['AREA', 'FOREST', 'CONIFEROUS']}])

# Ignore these toponyms
rules.extend([{'rules': {'public_transport': 'pay_scale_area'}},
              {'rules': {'boundary': 'vice_county'}},
              {'rules': {'boundary': 'administrative', 'admin_level': '5'}}
])

def classify(toponym):
    for rule in rules:
        matches = True
        for key, value in rule['rules'].items():
            if key not in toponym.tags or toponym.tags[key] != value:
                matches = False
        if matches:
            if 'type' in rule:
                return {'type': deepcopy(rule['type'])}
            else:
                return None
    print(toponym.tags)
    return None
