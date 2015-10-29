from sqlalchemy import and_
from geoalchemy2.shape import to_shape
from shapely import wkb

from .gazetteer import ContainmentGazetteer, ProximalGazetteer
from .filters import ContainmentFilter, ProximalFilter
from .classifier import NameSalienceCalculator, TypeSalienceCalculator
from .models import Point, Line, Polygon

class OSMGaz(object):
    """Main interface object, handles the full gazetteer pipeline.
    """

    def __init__(self, sqlalchemy_uri):
        self.containment_gaz = ContainmentGazetteer(sqlalchemy_uri)
        self.containment_filter = ContainmentFilter(self.containment_gaz)
        self.proximal_gaz = ProximalGazetteer(sqlalchemy_uri)
        self.proximal_filter = ProximalFilter(self.proximal_gaz)
        self.name_salience_calculator = NameSalienceCalculator(sqlalchemy_uri)
        self.type_salience_calculator = TypeSalienceCalculator(sqlalchemy_uri)

    def __call__(self, point):
        """Run the gazetteer pipeline for a single point. Returns a dictionary with
        containment and proximal toponyms. The containment toponyms are sorted by
        containment hierarchy. The proximal toponyms are in a random order.
        """
        def format_topo(toponym, classification, name_salience=None, type_salience=None):
            data = {'name': toponym.name,
                    'geometry': wkb.dumps(to_shape(toponym.way)),
                    'type': classification['type']}
            if name_salience is not None or type_salience is not None:
                data['salience'] = {}
                if name_salience is not None:
                    data['salience']['name'] = name_salience
                if type_salience is not None:
                    data['salience']['type'] = type_salience
            return data
        containment = self.containment_gaz(point)
        filtered_containment = self.containment_filter(containment)
        proximal = self.proximal_gaz(point, filtered_containment)
        filtered_proximal = self.proximal_filter(proximal, point, containment)
        
        return {'containment': [format_topo(t, c) for (t, c) in filtered_containment],
                'proximal': [format_topo(t, c, self.name_salience_calculator(t, filtered_containment), self.type_salience_calculator(c, filtered_containment))
                             for (t, c) in filtered_proximal]}


def main():
    #with open('unknown.txt', 'w') as _:
    #    pass
    points = [
              (-2.63629, 53.39797), # Dakota Park
              (-1.88313, 53.38129), # Peak District
              (-3.43924, 51.88286), # Brecon Beacons
              (-3.17516, 51.50650), # Roath Park
              (-2.99141, 53.40111), # Liverpool
              (-2.04045, 53.34058), # Lyme Park
              (-2.47429, 53.3827),  # Lymm
              ]
    gaz = OSMGaz('postgresql+psycopg2://osm:osmPWD@localhost:4321/osm')
    for point in points:
        print(point)
        data = gaz(point)
        print(', '.join([t['name'] for t in data['containment']]))
        print('\n'.join(['%s - %s (%.4f %.4f)' % (t['name'], t['type'], t['salience']['name'], t['salience']['type']) for t in data['proximal']]))

        # Find any unclassified toponyms
        """for pnt in name_salience_calculator.session.query(Point).filter(and_(Point.name != '',
                                                                               Point.classification == None,
                                                                               Point.way.ST_DWithin(filtered_containment[0][0].way, 400))):
            classification = type_salience_calculator.classifier(pnt)
            if classification:
                pnt.classification = '::'.join(classification['type'])
        for line in name_salience_calculator.session.query(Line).filter(and_(Line.name != '',
                                                                             Line.classification == None,
                                                                             Line.way.ST_DWithin(filtered_containment[0][0].way, 400))):
            classification = type_salience_calculator.classifier(line)
            if classification:
                line.classification = '::'.join(classification['type'])
        for polygon in name_salience_calculator.session.query(Polygon).filter(and_(Polygon.name != '',
                                                                                   Polygon.classification == None,
                                                                                   Polygon.way.ST_DWithin(filtered_containment[0][0].way, 400))):
            classification = type_salience_calculator.classifier(polygon)
            if classification:
                polygon.classification = '::'.join(classification['type'])
        name_salience_calculator.session.commit()"""
        #####
    with open('unknown.txt', 'a') as out_f:
        for tags in gaz.containment_gaz.classifier.get_unknown():
            out_f.write('%s\n' % repr(tags))
        for tags in gaz.proximal_gaz.classifier.get_unknown():
            out_f.write('%s\n' % repr(tags))

"""
Data must always be reprojected to EPSG:3857 (which in OSM terms is 900913)

Hierarchy Processing

#. Load all polygons that contain the point and have a name (UK needs UK polygon added)
#. Run each toponym through the classifier
#. Filter toponyms based on the classifications
   Idea with filtering is to keep adding places that improve the spatial accuracy of the location

Proximal Processing

#. Load all nodes/ways/polygons that are within a distance (increase distance if nothing found) from the point and have
   a name and are inside or within a specific distance from the smallest containment polygon
#. Run each toponym through the classifier
#. Filter unclassified toponyms
#. Run each toponym through the salience calculator (needs the most-specific hierarchy toponym)
#. Run each toponym through the "longevity" calculator (to factor in that some places are better references because they
   are more permanent)
#. Same principle, keep adding toponyms that improve the spatial accuracy of the location (Use the salience & longevity
   & preposition applicability values to drive the selection)
   This might need some additional weighting, such as "add a road first" a.k.a. preference for prepositions. Alternatively
   it might be workable if we calculate positional accuracy using just distance-metric and then add toponyms until that is
   maximised or hits a threshold. We could also possibly use the longevity metric as a stopping criterion -> if longevity
   of the caption overall would go down by adding the highest-ranked toponym, then stop adding them.
"""