#!/usr/bin/python

import sys
import time
import pdb
import cProfile
from music21.musicNet import *
from music21 import *

#uri = 'http://3bf8cea50.hosted.neo4j.org:7018/db/data/'
db = Database() #auth_username='dfa639169', auth_password='9253bc680')
#db.printStructure()
#sys.exit()

sys.stderr.write('Parsing score...\n')
path = 'bach/bwv84.5.mxl'
#path = 'bach/goldbergVariations_bwv988.mxl'
s = corpus.parse(path)
#print 'adding moments...'
#print time.ctime()
addMomentsToScore(s)
##cProfile.run("db.add(s, index=path)", "profile.txt")
#print time.ctime()
db.addScore(s, verbose=True)
sys.exit()

works = corpus.getComposer('bach')
for work in works[:10]:
    loc = work.find('corpus')
    path = work[loc+7:]
    print '\n' + work
    s = corpus.parse(path)
    addMomentsToScore(s)
    db.addScore(s, index=path, verbose=True)
