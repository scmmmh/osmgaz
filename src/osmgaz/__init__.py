import base64
import json
import logging
import math

from argparse import ArgumentParser
from copy import deepcopy
from geoalchemy2 import shape
from shapely import wkt, geometry
from shapely.ops import linemerge
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from . import preprocess
from .gazetteer import ContainmentGazetteer, ProximalGazetteer
from .filters import ContainmentFilter, ProximalFilter, type_match
from .classifier import NameSalienceCalculator, TypeSalienceCalculator
from .models import LookupCache, Polygon, Line, Point


class OSMGaz(object):
    """Main interface object, handles the full gazetteer pipeline.
    """

    def __init__(self, sqlalchemy_uri):
        engine = create_engine(sqlalchemy_uri)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.containment_gaz = ContainmentGazetteer(sqlalchemy_uri)
        self.containment_filter = ContainmentFilter(self.containment_gaz)
        self.proximal_gaz = ProximalGazetteer(sqlalchemy_uri)
        self.proximal_filter = ProximalFilter(self.proximal_gaz)
        self.name_salience_calculator = NameSalienceCalculator(sqlalchemy_uri)
        self.type_salience_calculator = TypeSalienceCalculator(sqlalchemy_uri)

    def load(self, point):
        cache = self.session.query(LookupCache).filter(LookupCache.point == '%.5f::%.5f' % point).first()
        if cache:
            data = json.loads(cache.data)
            return data
        return None
    
    def save(self, point, data):
        data = deepcopy(data)
        for toponym in data['osm_containment']:
            if 'osm_salience' in toponym:
                if 'name' in toponym['osm_salience']:
                    toponym['osm_salience']['name'] = float(toponym['osm_salience']['name'])
                if 'type' in toponym['osm_salience']:
                    toponym['osm_salience']['type'] = float(toponym['osm_salience']['type'])
        for toponym in data['osm_proximal']:
            if 'osm_salience' in toponym:
                if 'name' in toponym['osm_salience']:
                    toponym['osm_salience']['name'] = float(toponym['osm_salience']['name'])
                if 'type' in toponym['osm_salience']:
                    toponym['osm_salience']['type'] = float(toponym['osm_salience']['type'])
        self.session.add(LookupCache(point='%.5f::%.5f' % point,
                                     data=json.dumps(data)))
        self.session.commit()
    
    def merge_lines(self, toponyms):
        """Merge all line toponyms with the same name together."""
        result = []
        processed = []
        for idx, (toponym1, classification) in enumerate(toponyms):
            if isinstance(toponym1, Polygon) or isinstance(toponym1, Point):
                result.append((toponym1, classification))
                continue
            if toponym1.osm_id in processed:
                continue
            geometries = [shape.to_shape(toponym1.way)]
            for toponym2, _ in toponyms[idx + 1:]:
                if isinstance(toponym2, Polygon) or isinstance(toponym2, Point):
                    continue
                if toponym1.name == toponym2.name:
                    geometries.append(shape.to_shape(toponym2.way))
                    processed.append(toponym2.osm_id)
            if len(geometries) > 1:
                merged_toponym = Line(gid=toponym1.gid,
                                      osm_id=toponym1.osm_id,
                                      name=toponym1.name,
                                      way=shape.from_shape(linemerge(geometries), 900913),
                                      tags=toponym1.tags)
                result.append((merged_toponym, classification))
            else:
                result.append((toponym1, classification))
        return result

    def add_intersections(self, toponyms):
        """Add intersections between roads."""
        processed = []
        junctions = []
        for idx, (toponym1, classification1) in enumerate(toponyms):
            if isinstance(toponym1, Polygon) or isinstance(toponym1, Point) or not type_match(classification1['type'], ['ARTIFICIAL FEATURE', 'TRANSPORT', 'ROAD']):
                continue
            for toponym2, classification2 in toponyms[idx + 1:]:
                if (toponym1.osm_id, toponym2.osm_id) in processed or (toponym2.osm_id, toponym1.osm_id) in processed or not type_match(classification2['type'], ['ARTIFICIAL FEATURE', 'TRANSPORT', 'ROAD']):
                    continue
                geom1 = shape.to_shape(toponym1.way)
                geom2 = shape.to_shape(toponym2.way)
                if geom1.intersects(geom2):
                    processed.append((toponym1.osm_id, toponym2.osm_id))
                    geom = geom1.intersection(geom2)
                    if isinstance(geom, geometry.MultiPoint):
                        for part in geom:
                            junctions.append((Point(gid=-toponym1.gid,
                                                    osm_id=-toponym1.osm_id,
                                                    name='%s and %s' % (toponym1.name, toponym2.name),
                                                    way=shape.from_shape(part, 900913)),
                                              {'type': ['ARTIFICIAL FEATURE', 'TRANSPORT', 'ROAD', 'JUNCTION']}))
                    else:
                        junctions.append((Point(gid=-toponym1.gid,
                                                osm_id=-toponym1.osm_id,
                                                name='%s and %s' % (toponym1.name, toponym2.name),
                                                way=shape.from_shape(geom, 900913)),
                                          {'type': ['ARTIFICIAL FEATURE', 'TRANSPORT', 'ROAD', 'JUNCTION']}))
        toponyms.extend(junctions)
        return toponyms

    def __call__(self, point):
        """Run the gazetteer pipeline for a single point. Returns a dictionary with
        containment and proximal toponyms. The containment toponyms are sorted by
        containment hierarchy. The proximal toponyms are in a random order.
        """
        def format_topo(toponym, classification, name_salience=None, type_salience=None):
            data = {'dc_title': toponym.name,
                    'osm_geometry': wkt.dumps(shape.to_shape(toponym.way)),
                    'dc_type': classification['type']}
            if name_salience is not None or type_salience is not None:
                data['osm_salience'] = {}
                if name_salience is not None:
                    data['osm_salience']['name'] = name_salience
                if type_salience is not None:
                    data['osm_salience']['type'] = type_salience
            return data
        cache = self.load(point)
        if cache:
            return cache
        else:
            logging.info('Processing containment toponyms')
            containment = self.containment_gaz(point)
            filtered_containment = self.containment_filter(containment)
            logging.info('Processing proximal toponyms')
            proximal = self.proximal_gaz(point, filtered_containment)
            filtered_proximal = self.proximal_filter(proximal, point, containment)
            filtered_proximal = self.merge_lines(filtered_proximal)
            filtered_proximal = self.add_intersections(filtered_proximal)
            data = {'osm_containment': [format_topo(t, c) for (t, c) in filtered_containment],
                    'osm_proximal': [format_topo(t,
                                                 c,
                                                 self.name_salience_calculator(t, filtered_containment) if not type_match(c['type'], ['ARTIFICIAL FEATURE', 'TRANSPORT', 'ROAD', 'JUNCTION']) else 1,
                                                 self.type_salience_calculator(c, filtered_containment) if not type_match(c['type'], ['ARTIFICIAL FEATURE', 'TRANSPORT', 'ROAD', 'JUNCTION']) else 0)
                                     for (t, c) in filtered_proximal]}
            self.save(point, data)
            return data


def test(args):
    points = [
              (-2.63629, 53.39797), # Dakota Park
              (-1.88313, 53.38129), # Peak District
              (-3.43924, 51.88286), # Brecon Beacons
              (-3.17516, 51.50650), # Roath Park
              (-2.99141, 53.40111), # Liverpool
              (-2.04045, 53.34058), # Lyme Park
              (-2.47429, 53.3827),  # Lymm
              ]
    gaz = OSMGaz(args.sqla_url)
    for point in points:
        print(point)
        data = gaz(point)
        print(', '.join([t['dc_title'] for t in data['osm_containment']]))
        print('\n'.join(['%s - %s (%.4f %.4f)' % (t['dc_title'], t['dc_type'], t['osm_salience']['name'], t['osm_salience']['type']) for t in data['osm_proximal']]))


def main():
    parser = ArgumentParser()
    parser.add_argument('action', choices=['pre-process', 'test'])
    parser.add_argument('sqla_url')
    parser.add_argument('--full', default=False, action='store_true')
    args = parser.parse_args()
    if args.action == 'test':
        test(args)
    elif args.action == 'pre-process':
        preprocess.run(args)
