# -*- coding: utf-8 -*-
u"""
The filters module implements the functionality for filtering lists of toponyms.

.. moduleauthor:: Mark Hall <mark.hall@mail.room3b.eu>
"""
from sqlalchemy import and_

from .models import Polygon

def type_match(haystack, needle):
    """Check whether the needle list is fully contained within the haystack
    list, starting from the front."""
    if len(needle) > len(haystack):
        return False
    for idx in range(0, len(needle)):
        if haystack[idx] != needle[idx]:
            return False
    return True

class ContainmentFilter(object):
    """The ContainmentFilter implements filtering for containment toponym lists.
    """
    
    def __init__(self, containment_gazetteer):
        self.containment_gaz = containment_gazetteer
        self.cache = {}
    
    def filter_duplicates(self, hierarchy):
        """This filters toponyms with the same name, if one of the two is a CEREMONIAL area."""
        filtered = []
        for idx1, (toponym1, classification1) in enumerate(hierarchy):
            duplicate = False
            for idx2, (toponym2, classification2) in enumerate(hierarchy):
                if idx1 != idx2:
                    if toponym1.name == toponym2.name:
                        if type_match(classification1['type'], ['AREA', 'ADMINISTRATIVE']) and type_match(classification2['type'], ['AREA', 'CEREMONIAL']):
                            duplicate = True
            if not duplicate:
                filtered.append((toponym1, classification1))
        return filtered

    def filter_increments(self, hierarchy):
        """This filters toponyms where the smaller of the two toponyms has more than 25% of the
        larger toponym's area. Ensures that there is actual spatial value added by the toponyms.
        """ 
        filtered = []
        prev_size = 0
        for toponym, classification in hierarchy:
            if len(filtered) == 0 or prev_size / float(toponym.tags['way_area']) <= 0.25:
                filtered.append((toponym, classification))
                prev_size = float(toponym.tags['way_area'])
        if hierarchy[-1][0].tags['admin_level'] != filtered[-1][0].tags['admin_level']:
            filtered.append(hierarchy[-1])
        return filtered

    def filter_unique(self, toponyms, figure_idx, ground_idx):
        """This filters the toponym(s) between figure_idx and ground_idx, if the toponym
        at figure_idx is unique within the ground_idx toponym. Removes pointless complexity
        from the list of containment toponyms.
        """
        query = self.containment_gaz.session.query(Polygon).filter(and_(Polygon.name == toponyms[figure_idx][0].name,
                                                                        Polygon.way.ST_Intersects(toponyms[ground_idx][0].way)))
        unique = True
        for toponym, classification in self.containment_gaz.query(query):
            if toponym.osm_id != toponyms[figure_idx][0].osm_id:
                if classification and classification['type'][:2] == toponyms[figure_idx][1]['type'][:2]:
                    unique = False
        if unique == True:
            return toponyms[:figure_idx + 1] + toponyms[ground_idx:]
        else:
            return toponyms
    
    def __call__(self, toponyms):
        """Runs the filtering pipline. Toponyms must be ordered by their size, smallest first.
        First applies the filter_duplicates and filter_increments. Then applies the filter_unique
        with the following conditions:
          * if there are more than 3 toponyms (first filter high-level, then low-level, then high-level)
          * if there are exactly three toponyms and the first one is at least level 8 ADMINISTRATIVE middle one is CEREMONIAL
          * if the first toponym is a NATIONAL PARK
        """
        filtered = self.filter_duplicates(toponyms)
        filtered = self.filter_increments(filtered)
        if len(filtered) > 3:
            filtered = self.filter_unique(filtered, -3, -1)
            if len(filtered) > 3:
                filtered = self.filter_unique(filtered, 0, 2)
                if len(filtered) > 3:
                    filtered = self.filter_unique(filtered, -3, -1)
        if len(filtered) == 3:
            if type_match(filtered[0][1]['type'], ['AREA', 'ADMINISTRATIVE', '8']) and type_match(filtered[1][1]['type'], ['AREA', 'CEREMONIAL']):
                filtered = self.filter_unique(filtered, 0, 2)
        if type_match(filtered[0][1]['type'], ['AREA', 'NATIONAL PARK']):
            filtered = self.filter_unique(filtered, 0, 2)
        return filtered

