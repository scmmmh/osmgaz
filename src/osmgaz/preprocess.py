# -*- coding: utf-8 -*-
"""
Contains the pre-processing functionality.

.. moduleauthor:: Mark Hall <mark.hall@edgehill.ac.uk>
"""
import json
import logging
import math

from sqlalchemy import and_

from osmgaz.gazetteer import ContainmentGazetteer
from osmgaz.models import Point, Line, Polygon


def classify(session, obj, classifier):
    """Classify all entries of type obj using the classifier."""
    query = session.query(obj).filter(and_(obj.name != '', obj.classification == None)).order_by(obj.gid)
    for start in range(0, math.ceil(query.count() / 1000)):
        for toponym in query.offset(start * 1000).limit(1000):
            classification = classifier(toponym)
            if classification:
                toponym.classification = '::'.join(classification)
        session.commit()
        logging.info('Classified %i %s' % ((start + 1) * 1000, obj.__name__))


def run(args):
    """Pre-processes the complete data-set"""
    logging.root.setLevel(logging.INFO)
    gaz = ContainmentGazetteer(args.sqla_url)
    session = gaz.session
    classifier = gaz.classifier
    for obj in [Polygon, Line, Point]:
        logging.info('Classifying all %s' % (obj.__name__))
        classify(session, obj, classifier)
    with open('unknown.txt', 'w') as out_f:
        for tags in classifier.get_unknown():
            out_f.write('%s\n' % json.dumps(tags))
