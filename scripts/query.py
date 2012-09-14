#!/usr/bin/python

import sys
from music21.musicNet import *

print 'Starting up...'
db = Database()
query = Query(db)
n1 = query.setStartNode(nodeType='Note')
sw = query.addRelationship(relationType='NoteSimultaneousWithNote', start=n1)
fb1 = query.addRelationship(relationType='NoteToNote', end=n1)
fb2 = query.addRelationship(relationType='NoteToNote', end=sw.end)
query.addComparisonFilter(sw.simpleHarmonicInterval, '=', 7)
query.addComparisonFilter(sw.sameOffset, '=', True)
query.addComparisonFilter(fb1.interval, '!=', 0)
query.addComparisonFilter(fb2.interval, '!=', 0)
query.setLimit(10)

print 'Getting results...'
results = query.getResults()
print query.pattern
for result in results:
    print 'Assembling score...'
    score = query.music21Score(result)
    score.show()
    raw_input('Press Return for next score.')
    