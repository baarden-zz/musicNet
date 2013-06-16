#!/usr/bin/python

import sys
from music21.musicNet import *

print 'Starting up...'
db = Database()
values = db.listRelationshipProperties()
print [x for x in values if x[0]=='Instrument' and x[1]=='partName']
sys.exit()

query = Query(db)
n1 = query.setStartNode(nodeType='Note')
sw = query.addRelationship(relationType='NoteSimultaneousWithNote', start=n1)
fb1 = query.addRelationship(relationType='NoteToNote', end=n1)
fb2 = query.addRelationship(relationType='NoteToNote', end=sw.end)
query.addComparisonFilter(sw.simpleHarmonicInterval, '=', 7)
query.addComparisonFilter(sw.sameOffset, '=', True)
query.addComparisonFilter(fb1.interval, '<>', 0)
query.addComparisonFilter(fb2.interval, '<>', 0)

print 'Getting results...'
row = 0
print query
while True:
    print '==='
    results, metadata = query.results(row, 10)
    if len(results) == 0:
        break
    for result in results:
        print result
        row += 1

#        print 'Assembling score...'
#        score = query.music21Score(result)
#        score.show()
#        raw_input('Press Return for next score.')
    