from .classifier import output_unknown
from .gazetteer import ContainmentGazetteer, ProximalGazetteer
from .filters import ContainmentFilter

def main():
    points = [(-2.63629, 53.39797), # Dakota Park
              (-1.88313, 53.38129), # Peak District
              (-3.43924, 51.88286), # Brecon Beacons
              (-3.17516, 51.50650), # Roath Park
              (-2.99141, 53.40111), # Liverpool
              (-2.04045, 53.34058), # Lyme Park
              (-2.47429, 53.3827),  # Lymm
              ]
    containment_gaz = ContainmentGazetteer('postgresql+psycopg2://osm:osmPWD@localhost:6543/osm')
    containment_filter = ContainmentFilter(containment_gaz)
    proximal_gaz = ProximalGazetteer('postgresql+psycopg2://osm:osmPWD@localhost:6543/osm')
    for point in points:
        print(point)
        containment = containment_gaz(point)
        filtered_containment = containment_filter(containment)
        print(', '.join([t.name for t, _ in filtered_containment]))
        proximal = proximal_gaz(point, {'full': containment, 'filtered': filtered_containment})
        print(', '.join([t.name for t, _ in proximal]))
        #for toponym, classification in proximal:
        #    print(toponym.name, classification, toponym.tags)
    output_unknown()

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