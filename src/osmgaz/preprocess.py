# -*- coding: utf-8 -*-
"""
Contains the pre-processing functionality.

.. moduleauthor:: Mark Hall <mark.hall@edgehill.ac.uk>
"""
import json
import logging
import math

from geoalchemy2 import shape
from sqlalchemy import and_

from osmgaz.classifier import NameSalienceCalculator, TypeSalienceCalculator
from osmgaz.filters import ContainmentFilter
from osmgaz.gazetteer import ContainmentGazetteer
from osmgaz.models import Point, Line, Polygon


def classify(session, obj, classifier, full):
    """Classify all entries of type obj using the classifier."""
    if full:
        query = session.query(obj).filter(obj.name != '').order_by(obj.gid)
    else:
        query = session.query(obj).filter(and_(obj.name != '',
                                               obj.classification == None)).order_by(obj.gid)
    for start in range(0, math.ceil(query.count() / 1000)):
        for toponym in query.offset(start * 1000).limit(1000):
            classification = classifier(toponym)
            if classification:
                classification = '::'.join(classification)
                if classification != toponym.classification:
                    toponym.classification = '::'.join(classification)
        session.commit()
        logging.info('Classified %i %s' % ((start + 1) * 1000, obj.__name__))


def salience(session, obj, gaz, filtr, name_salience, type_salience, full):
    """Pre-calculate the name and type salience for the location"""
    if full:
        query = session.query(obj).filter(obj.name != '').order_by(obj.gid)
    else:
        query = session.query(obj).filter(and_(obj.name != '',
                                               obj.classification != None)).order_by(obj.gid)
    for start in range(0, math.ceil(query.count() / 1000)):
        for toponym in query.offset(start * 1000).limit(1000):
            geom = shape.to_shape(toponym.way)
            containment = gaz(gaz.proj(geom.centroid.x, geom.centroid.y, inverse=True))
            containment = [(t, c) for (t, c) in containment if t.name != toponym.name]
            containment = filtr(containment[:-1])
            if containment:
                name_salience(toponym, containment)
                type_salience({'type': toponym.classification.split('::')}, containment)
        logging.info('Salience calculated for %i %s' % ((start + 1) * 1000, obj.__name__))


def run(args):
    """Pre-processes the complete data-set"""
    logging.root.setLevel(logging.INFO)
    gaz = ContainmentGazetteer(args.sqla_url)
    containment_filter = ContainmentFilter(gaz)
    session = gaz.session
    classifier = gaz.classifier
    name_salience = NameSalienceCalculator(args.sqla_url)
    type_salience = TypeSalienceCalculator(args.sqla_url)
    for obj in [Polygon, Line, Point]:
        logging.info('Classifying all %s' % (obj.__name__))
        classify(session, obj, classifier, args.full)
    with open('unknown.txt', 'w') as out_f:
        for tags in classifier.get_unknown():
            out_f.write('%s\n' % json.dumps(tags))
    for obj in [Polygon, Line, Point]:
        logging.info('Salience classification for all %s' % (obj.__name__))
        salience(session,
                 obj,
                 gaz,
                 containment_filter,
                 name_salience,
                 type_salience,
                 args.full)

