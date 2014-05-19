#!/usr/bin/python

import os
import time
import subprocess
import multiprocessing
import pickle
import redis
import music21
import music21.musicNet
import tempfile
import py2neo

'''
Requires a server with Lilypond >=2.17 and ImageMagick installed, 
as well as a functioning MusicNet server.
'''

class GeneratePreviews(multiprocessing.Process):
    '''
    .. doctest::
       hide
    >>> import multiprocessing
    >>> inQueue = multiprocessing.JoinableQueue()
    >>> outQueue = multiprocessing.JoinableQueue()
    >>> from music21.musicNet.musicNetServer import *
    >>> previewGen = GeneratePreviews(inQueue, outQueue)
    >>> from music21 import *
    >>> bwv84_5 = corpus.parse('bach/bwv84.5.mxl')
    >>> previewGen.makePreview(bwv84_5)
    'preview...png'
    '''
    
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
        while self.redis.lpop('inQueue'):
            pass

    def run(self, uri='http://localhost:7474/db/data/', **kwargs):
        # hack to get py2neo to play nice with multiprocessing
        py2neo.packages.httpstream.http.ConnectionPool._puddles = {}
        #1.4: py2neo.rest._thread_local = threading.local()
        #
        db = music21.musicNet.Database()
        while True:
            val = self.redis.lpop('inQueue')
            if val == None:
                time.sleep(0.2)
                continue
            scoreDict = pickle.loads(val)
            start = time.clock()
            q = music21.musicNet.Query(db)
            score = q.music21Score(scoreDict['result'], scoreDict['metadata'])
            start = time.clock()            
            path = self.makePreview(score)
            imageDict = { 'index': scoreDict['index'], 'path': path, 'token': scoreDict['token'] }
            self.redis.rpush('outQueue', pickle.dumps(imageDict))
    
    def makePreview(self, score):
        fh = tempfile.NamedTemporaryFile(prefix='preview', dir=tempfile.gettempdir(), delete=False)
        filename = fh.name
        fh.close()
        # Create a minimal score with no header or footer
        score.metadata = None
        conv = music21.lily.translate.LilypondConverter()
        conv.loadFromMusic21Object(score)
        header = [x for x in conv.context.contents if isinstance(x, music21.lily.lilyObjects.LyLilypondHeader)][0]
        header.lilypondHeaderBody = 'tagline = ""'
        conv.runThroughLily(backend='eps -dresolution=200', format='png', fileName=filename)
        filename += '.png'
        subprocess.call(['convert', '-trim', filename, filename])
        return os.path.basename(filename)

def newWorker():
    worker = GeneratePreviews()
    #worker.daemon = True
    print 'Starting new worker.'
    worker.start()
    return worker

if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('-w', '--workers', dest='workers', default=multiprocessing.cpu_count(),
                      help='-w|--workers : number of multiprocessing workers (default: 1 per CPU)')
    (options, args) = parser.parse_args()

    previewWorkers = []
    workerCount = int(options.workers)
    for i in range(workerCount):
        previewWorkers.append(newWorker())
    print 'Running.'
    while True:
        time.sleep(1)
        for i in range(workerCount):
            if not previewWorkers[i].is_alive():
                previewWorkers[i].terminate()
                previewWorkers[i] = newWorker()
