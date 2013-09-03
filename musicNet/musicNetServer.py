#!/usr/bin/python
#-------------------------------------------------------------------------------
# Name:         musicNetServer.py
# Purpose:      a Python app providing a RESTful interface 
#               to the musicNet classes
#
# Authors:      Bret Aarden
# Version:      0.2
# Date:         22Dec2012
#
# License:      MPL 2.0
#-------------------------------------------------------------------------------


'''
This is an app that provides a RESTful JSON interface to the music21.musicNet objects.
Any client with the ability to send/receive JSON can use a remote
server running this app to execute queries and return the results. It's dependent on
the Python Bottle module. (Doctests are dependent on the webtest module.)

Services with a result containing multiple rows return each row as a separate JSONItem:
rather than returning one JSON document containing an array, each line
is a separate JSON document containing one row. Any service may return
a JSONItem dictionary with an 'error'/error message key/value pair, so
clients should check for this possibility.

Only query-related services are exposed by this interface. Actions that manipulate 
the database can only be done on the server using the musicNet objects directly.

The app is built with the Bottle framework and will start using its default options
(localhost:8080).

Services
========
'''

import os
import time
import json
import tempfile
import multiprocessing
import Queue
import threading
import bottle
import music21
import music21.musicNet
import py2neo


## {{{ http://code.activestate.com/recipes/577564/ (r2) 
class Silence:

    def __init__(self, stdout=os.devnull, stderr=os.devnull, mode='w'):
        self.outfiles = stdout, stderr
        self.combine = (stdout == stderr)
        self.mode = mode
 
    def __enter__(self):
        import sys
        self.sys = sys
        # save previous stdout/stderr
        self.saved_streams = saved_streams = sys.__stdout__, sys.__stderr__
        self.fds = fds = [s.fileno() for s in saved_streams]
        self.saved_fds = map(os.dup, fds)
        # flush any pending output
        for s in saved_streams: s.flush()
 
        # open surrogate files
        if self.combine: 
            null_streams = [open(self.outfiles[0], self.mode, 0)] * 2
            if self.outfiles[0] != os.devnull:
                # disable buffering so output is merged immediately
                sys.stdout, sys.stderr = map(os.fdopen, fds, ['w']*2, [0]*2)
        else: null_streams = [open(f, self.mode, 0) for f in self.outfiles]
        self.null_fds = null_fds = [s.fileno() for s in null_streams]
        self.null_streams = null_streams
 
        # overwrite file objects and low-level file descriptors
        map(os.dup2, null_fds, fds)
 
    def __exit__(self, *args):
        sys = self.sys
        # flush any pending output
        for s in self.saved_streams: s.flush()
        # restore original streams and file descriptors
        map(os.dup2, self.saved_fds, self.fds)
        sys.stdout, sys.stderr = self.saved_streams
        # clean up
        for s in self.null_streams: s.close()
        return False
## end of http://code.activestate.com/recipes/577564/ }}}


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
    
    def __init__(self, inQueue, outQueue):
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self.inQueue = inQueue
        self.outQueue = outQueue

    def run(self, uri='http://localhost:7474/db/data/', **kwargs):
        # stupid hack to get py2neo to play nice with multiprocessing
        import py2neo.rest
        py2neo.rest._thread_local = threading.local()
        #
        db = music21.musicNet.Database()
        while True:
            try:
                scoreDict = self.inQueue.get_nowait()
            except Queue.Empty:
                time.sleep(0.25)
                continue
            q = music21.musicNet.Query(db)
            score = q.music21Score(scoreDict['result'], scoreDict['metadata'])
            path = self.makePreview(score)
            imageDict = { 'index': scoreDict['index'], 'path': path, 'token': scoreDict['token'] }
            self.inQueue.task_done()
            self.outQueue.put(imageDict)
    
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
        with Silence():
            conv.runThroughLily(backend='eps -dresolution=200', format='png', fileName=filename)
        from subprocess import call
        filename += '.png'
        call(['convert', '-trim', filename, filename])
        return os.path.basename(filename)

#-------------------------------------------------------------------------------
# musicNetServer
#-------------------------------------------------------------------------------

bottle.debug(True)
app = bottle.Bottle()
app.catchall = False
app.db = music21.musicNet.Database()
app.tokens = {}
app.images = {}

m = multiprocessing.Manager()
inQueue = m.Queue()
outQueue = m.Queue()
previewWorkers = []
for i in range(multiprocessing.cpu_count()):
    worker = GeneratePreviews(inQueue, outQueue)
    worker.daemon = True
    previewWorkers.append(worker)

'''
listScores
----------
'''

@app.get('/listscores')
def listScores():
    '''
    Server address::
 
        /listscores?start=0&limit=100
    
    Request parameters::
    
        start - The first row of scores to return (default=0).
        limit - The number of scores to return (default=100).
    
    Response: a JSONItem dictionary for each score in the database.
    
    Data structure::

        { movementName: name_of_score_file, _names: [ contributor, ... ], index: original_path_of_score_file }
        { movementName: name_of_score_file, _names: [ contributor, ... ], index: original_path_of_score_file }
        ...
    
    We can test the functionallity of the app without actually starting it by using the :class:`webtest`
    framework:
    
    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listscores')
    >>> import json
    >>> sorted(json.loads(r.body).items())
    [(u'_names', [None]), (u'corpusFilepath', u'bach/bwv84.5.mxl'), (u'movementName', u'bwv84.5.mxl')]
    '''
    query = bottle.request.query
    start = int(query.start or 0)
    limit = int(query.limit or 100)
    rows = app.db.listScores(start, limit)
    for row in rows:
        yield json.dumps(row) + '\n'

'''
listNodeTypes
-------------
'''     
@app.get('/listnodetypes')
def listNodeTypes():
    '''    
    Server address::
 
        /listnodetypes
    
    Request parameters: none.
    
    Response: a JSON array of node types in the database.
    
    Data structure::

        [ 'score', 'metadata', 'note', ... ]
        
    Example:

    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listnodetypes')
    >>> import json
    >>> json.loads(r.body)
    [u'StaffGroup', u'SystemLayout', u'Beam', u'Score', u'Rest', u'Note', u'Instrument', u'Moment', u'Part', u'Measure', u'Barline', u'Expression', u'Metadata']
    '''
    types = list(app.db.listNodeTypes())
    return json.dumps(types) + '\n'

@app.get('/listnodeproperties')
def listNodeProperties():
    '''
    Server address::
 
        /listnodeproperties
    
    Request parameters: none.
    
    Response: 
    
        A JSON array for each node property type in the database. The first element is the node
        type, and the second element is the name of the property.
        
    Data structure::
    
        ['StaffGroup', 'offset']
        ['SystemLayout', 'distance']
        ['SystemLayout', 'quarterLength']
        ...
    
    Example:

    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listnodeproperties')
    >>> import json
    >>> json.loads(r.body.splitlines()[0])
    [u'StaffGroup', u'offset']
    '''
    rows = app.db.listNodeProperties()
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/listnodepropertyvalues')
def listNodePropertyValues():
    '''
    Server address::
 
        /listnodepropertyvalues
    
    Request parameters: none.
    
    Response: 
    
        A JSON array for each node property type in the database. The first element is the node
        type, the second element is the name of the property, and the third is a list of values for that
        property found in the database.
        
    Data structure::
    
        ['Instrument', 'partName', ['Alto', 'Bass', 'Soprano', 'Tenor']]
        ['Note', '_stemDirection', ['down', 'up']]
        ...
    
    Example:

    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listnodepropertyvalues')
    >>> import json
    >>> json.loads(r.body.splitlines()[0])
    [u'StaffGroup', u'offset', [0.0,]]
    '''
    rows = app.db.listNodePropertyValues()
    for row in rows:
        yield json.dumps(row) + '\n'
        
@app.get('/listrelationshiptypes')
def listRelationshipTypes():
    '''
    Server address::
 
        /listrelationshiptypes
    
    Request parameters: none.
    
    Response: 
    
        A JSONItem dictionary for each kind of relationship in the database, including
        the type of node at the start and end of the relationship.
    
    Data structure::
    
        {'start': u'Note', 'end': u'Measure', 'type': u'NoteInMeasure'}
        {'start': u'Part', 'end': u'Score', 'type': u'PartInScore'}
        ...
    
    Example:

    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listrelationshiptypes')
    >>> import json
    >>> json.loads(r.body.splitlines()[0])
    {u'start': u'Note', u'end': u'Measure', u'type': u'NoteInMeasure'}
    '''
    rows = app.db.listRelationshipTypes()
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/listrelationshipproperties')
def listRelationshipProperties():
    '''
    Server address::
 
        /listrelationshipproperties
    
    Request parameters: none.
    
    Response: 
    
        A 2-element JSONItem array for each kind of Relationship property in the database:
        the first item is the Relationship type, and the second is the name of the property.
    
    Data structure::
    
        ['NoteToNoteByBeat', 'interval']
        ['NoteToNote', 'interval']
        ...
        
    Example:

    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listrelationshiptypes')
    >>> import json
    >>> sorted(json.loads(r.body.splitlines()[0]).items())
    [(u'end', u'Measure'), (u'start', u'Note'), (u'type', u'NoteInMeasure')]
    '''
    rows = app.db.listRelationshipProperties()
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/listrelationshippropertyvalues')
def listRelationshipPropertyValues():
    '''
    Server address::
 
        /listrelationshippropertyvalues
    
    Request parameters: none.
    
    Response: 
    
        A JSON array for each relationship property type in the database. The first element is the relationship
        type, the second element is the name of the property, and the third is a list of values for that
        property found in the database.
        
    Data structure::
    
        ['NoteToNoteByBeat', 'interval', [-12, -7, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 7, 12]]
        ['NoteToNote', 'interval', [-12, -7, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 7, 12]]
        ...
    
    Example:

    >>> from webtest import TestApp
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = TestApp(mns.app)
    >>> r = webapp.get('/listrelationshippropertyvalues')
    >>> import json
    >>> json.loads(r.body.splitlines()[0])
    [u'NoteToNoteByBeat', u'interval', [-12, -7, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 7, 12]]
    '''
    rows = app.db.listRelationshipPropertyValues()
    for row in rows:
        yield json.dumps(row) + '\n'
        
@app.get('/images/<filename:re:.*\.png>')
def sendImage(filename):
    tempdir = tempfile.gettempdir()
    return bottle.static_file(filename, root=tempdir, mimetype='image/png')

@app.post('/submitquery')
def prepareQuery():
    '''
    Server address::
 
        /submitquery
    
    This service processes a JSON query and returns an integer token that can be used
    to obtain results using 'getresults' and 'getimages'. Unlike
    the other services, it uses the POST method and expects the body of the request
    to be a JSON document containing the query. Tokens are good for a
    half hour before they expire. Identical queries from the same IP address will
    return the same token.
    
    The JSON query is expected to be an object with specific key/value pairs:
    
      nodes
        An array of objects, one per node, each identifying its 
        'type' and its 'name' in the query.
      relationships
        An array of objects, one per relationship, each identifying its 'type', 
        the name of its 'start' node, the name of its 'end' node, and its own 'name' 
        in the query.
      startNode
        The name of the starting node for the query.
      startRelationship
        The name of the starting relationship for the query. There must be 
        exactly one startNode or startRelationship per query. Results are
        ordered by the ID of the start object.
      relationshipProperties
        An array of objects, one per relationship property that is referenced
        in the query, each identifying the name of the 'relationship', the
        relationship 'property', and its 'name' in the query.
      nodeProperties
        An array of objects, one per node property that is referenced
        in the query, each identifying the name of the 'node', the
        node 'property', and its 'name' in the query.
      comparisonFilters
        An array of objects, one per comparison filter. Each object should specify
        the comparison 'operator' (one of `=', '<>', '<', '>', '<=',` or '>='),
        the comparison prefix ('pre' and 'preType') and comparison postfix
        ('post' and 'postType'). The pre/post type can be either a 'property' (a property
        name) or a 'value' (a number, string, or boolean). 
      returns
        An array of property names that will be returned by the query. 
      makePreviews
        A boolean indicating whether previews will be created. Queries that
        specify returns are not compatible with the 'makePreviews' option.
        Preview URLs can be obtained from the getimage service.
    
    For instance::
    
        { 'nodes': [ { 'type': 'Note', 'name': 'n1' },
                     { 'type': 'Note', 'name': 'n2' } ],
          'relationships': [ { 'start': 'n1', 'type': 'NoteSimultaneousWithNote', 'end': 'n2', 'name': 'nswn' } ],
          'startNode': 'n1',
          'relationshipProperties': [ { 'relationship': 'nswn', 'property': 'sameOffset', 'name': 'nswnSo' },
                                      { 'relationship': 'nswn', 'property': 'simpleHarmonicInterval', 'name': 'nswnShi' } ],
          'comparisonFilters': [ { 'preType': 'property', 'pre': 'nswnSo', 'operator': '=', 'postType': 'value', 'post': 'True' },
                                 { 'preType': 'property', 'pre': 'nswnShi', 'operator': '=', 'postType': 'value', 'post': 7 } ],
          'returns': [],
          'makePreviews': 'True'
        }
    
    Data structure::
    
        {'token': -4258737148674360350}
        
    Example:

    >>> import webtest
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = webtest.TestApp(mns.app)
    >>> req = { 'nodes': [ { 'type': 'Note', 'name': 'n1' } ], 'startNode': 'n1' }
    >>> import json
    >>> r = webapp.post_json('/submitquery', req)
    >>> json.loads(r.body)
    {u'token': ...}
    '''
    req = bottle.request.json
    q = music21.musicNet.Query(app.db)
    nodes = {}
    scoreNode = None
    relations = {}
    properties = {}
    notes = []
    measures = {}
    parts = {}
    instruments = {}
    score = {}
    columns = []
    if not req.get('nodes', False):
        return { 'error': 'query has no nodes' }
    for n in req['nodes']:
        node = q.addNode(n['type'], n['name'])
        nodes[n['name']] = node
        if (n['type'] == 'Note'):
            notes.append(node)
            columns.append(n['name'])
        elif (n['type'] == 'Score'):
            scoreNode = node
    for r in req.get('relationships', []):
        start = nodes[r['start']]
        end = nodes[r['end']]
        optional = getattr(r, 'optional', None)
        relation = q.addRelationship(relationType=r['type'], start=start, end=end, 
                                     name=r['name'], optional=optional)
        relations[r['name']] = relation
        if (r['type'] == 'NoteInMeasure'):
            measures[start] = end
        elif (r['type'] == 'MeasureInPart'):
            parts[start] = end
        elif (r['type'] == 'InstrumentInPart'):
            instruments[end] = start 
        elif (r['type'] == 'PartInScore'):
            score[start] = end
    if 'startNode' in req:
        q.setStartNode(nodes[req['startNode']])
    elif 'startRelationship' in req:
        q.setStartRelationship(relations[req['startRelationship']])
    else:
        return { 'error': 'query has no start' }
    if 'nodeProperties' in req:
        for p in req['nodeProperties']:
            node = nodes[p['node']]
            properties[p['name']] = getattr(node, p['property'])
    if 'relationshipProperties' in req:
        for p in req['relationshipProperties']:
            relation = relations[p['relationship']]
            properties[p['name']] = getattr(relation, p['property'])
    if 'comparisonFilters' in req:
        for f in req['comparisonFilters']:
            addComparisonFilter(q, f, properties)
    if 'returns' in req and req['returns']:
        returns = [properties[r['property']] for r in req['returns']]
        columns.extend(returns);
        q.addReturns(*returns)
    #else:
    #    columns.extend(properties.values())
    for n in notes:
        if n not in measures.keys():
            m = q.addNode('Measure', 'measure_' + n.name)
            r = q.addRelationship(relationType='NoteInMeasure', start=n, end=m, name=n.name+'In'+m.name, optional=True)
            measures[n] = m
    for m in measures.values():
        if m not in parts.keys():
            p = q.addNode('Part', 'part_' + m.name)
            r = q.addRelationship(relationType='MeasureInPart', start=m, end=p, name=m.name+'In'+p.name, optional=True)
            parts[m] = p
    for p in parts.values():
        if p not in instruments.keys():
            i = q.addNode('Instrument', 'instrument_' + p.name)
            r = q.addRelationship(relationType='InstrumentInPart', start=i, end=p, name=i.name+'In'+p.name, optional=True)
    if scoreNode == None:
        scoreNode = q.addNode('Score', 'Score')
    if len(score) == 0:
        p = parts.itervalues().next()
        r = q.addRelationship(relationType='PartInScore', start=p, end=scoreNode, name=p.name+'In'+scoreNode.name)
    # ensure that combinations of parts are unique
    partIds = [n['name'] for n in req['nodes'] if n['type'] == 'Part']
    for i in range(len(partIds) - 1):
        q.addComparisonFilter(nodes[partIds[i]].ID, '<', nodes[partIds[i+1]].ID)
    previews = req.get('makePreviews', False)
    #if previews and 'returns' in req:
    #    return { 'error': '"makePreviews" and "results" cannot be combined in the same query' }
    pattern = q._assemblePattern()
    ipAddr = bottle.request.remote_addr or "None"
    token = hash(ipAddr + pattern)
    app.tokens[token] = [pattern, columns, previews, time.time()]
    expireTokens()
    return { 'token': token }
    
@app.get('/getresults')
def results():
    '''
    Server address::
     
        /getresults?token=nnn&row=0&limit=10
    
    Request parameters::

        token - The number returned by the submitquery service
        row   - The first row of the query to return (default=0)
        limit - The number of query results to return (default=10)
    
    Response: 
    
        A series of JSONItem dictionaries: first the metadata for the
        results (the list of column names), then the data rows.
        A 'type' key indicates the kind of row, and the information
        is indexed by the 'data' key. Data rows also have a 'index'
        key indicating the line number of the result.
        
        A typical strategy is to send requests to this service,
        incrementing the row by the limit each time, until it
        returns an empty response.
    
    Data structure::
    
        {'type': 'metadata', 'data': ['nswn', 'p2', 'n1', 'p1', 'ntn1', 'n2', 'ntn2', 'p1', 'm1']}
        {'type': 'data', 'index': 0, 'data': [7, 'D4', 'F#4', 'B4', -5, 'B3', -3, 'Soprano', 'Tenor', 5]}
        {'type': 'data', 'index': 1, 'data': [19, 'B3', 'C#5', 'D5', -1, 'F#3', -5, 'Soprano', 'Bass', 6]}
        ...
        
    Example:
        
    >>> import webtest
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = webtest.TestApp(mns.app)
    >>> req = { 'nodes': [ { 'type': 'Note', 'name': 'n1' } ], 'startNode': 'n1' }
    >>> r = webapp.post_json('/submitquery', req)
    >>> import json
    >>> token = json.loads(r.body)['token']
    >>> r = webapp.get('/getresults?token=%s&start=0&limit=1' % token)
    >>> sorted(json.loads(r.body.splitlines()[0]).items())
    [(u'data', [u'n1']), (u'type', u'metadata')]
    '''
    args = bottle.request.query
    token = int(args.token)
    minRow = int(args.row or 0)
    limit = int(args.limit or 10)
    if token not in app.tokens:
        item = { 'error': 'That token is invalid or has expired.' }
        yield json.dumps(item) + '\n'
        raise StopIteration
    pattern, columns, previews, timestamp = app.tokens[token]
    q = music21.musicNet.Query(app.db)
    data, metadata = q.results(minRow, limit, pattern)
    rLookup = {}
    for i in range(len(metadata)):
        rLookup[metadata[i]] = i
    for idx in range(len(data)):
        queryIdx = idx + minRow
        # remove any None values from the output
        row = data[idx]
        objects = []
        objects_meta = []
        nonobjects = []
        nonobjects_meta = []
        for i in range(len(row)):
            item = row[i]
            if isinstance(item, py2neo.rest.Resource):
                objects.append(item)
                objects_meta.append(metadata[i])
            elif not item is None:
                nonobjects.append(item)
                nonobjects_meta.append(metadata[i])
        # get extended results and limit to requested values
        result = q.getResultProperties(objects)
        objects = [objectValueMap(x[1]) for x in result]
        items = objects + nonobjects
        headers = objects_meta + nonobjects_meta
        output = []
        meta_output = columns[:]
        for r in meta_output:
            output.append(items[headers.index(r)])
        addScoreInfo(result, output, meta_output, idx)
        if previews:
            output_objects = []
            output_meta = []
            for i in range(len(result)):
                if objects_meta[i] in columns:
                    output_objects.append(result[i][0])
                    output_meta.append(objects_meta[i])
            inQueue.put({ 'index': queryIdx, 'result': output_objects, 'metadata': output_meta, 'token': token })
        if idx == 0:
            item = { 'type': 'metadata', 'data': meta_output }
            yield json.dumps(item) + '\n'
        item = { 'type': 'data', 'index': queryIdx, 'data': output }
        yield json.dumps(item) + '\n'
    expireTokens()

@app.get('/getimages')
def getImage():
    '''
    Server address::
 
        /getimages?token=nnn&block=false

    Request parameters::

        token - The number returned by the submitquery service
    
    Response: 
    
        Zero or more JSONItem objects. The 'path' key/value pair
        contains the path that an HTTP request can use to obtain the
        preview PNG. An 'index' key/value indicates the corresponding query
        result row for this image. A 'type' key indicating that this is a
        'preview' is included in case it's useful.
        
        An empty response is _not_ an indication that all previews have been
        generated. It is up to the client to keep track of how many
        previews are expected. Sending requests to this service more than
        2-4 times a second is probably not useful.
        Previews are generated after a call to the result service.
    
    Data structure::
    
        {'url': '/images/previewJHUqde.png', 'index': 0, 'type': 'preview'}
        {'url': '/images/previewriWX1Z.png', 'index': 1, 'type': 'preview'}
        {'url': '/images/previewf0PvMb.png', 'index': 2, 'type': 'preview'}
        ...
        
    Example:

    >>> import webtest
    >>> import music21.musicNet.musicNetServer as mns
    >>> webapp = webtest.TestApp(mns.app)
    >>> req = { 'nodes': [ { 'type': 'Note', 'name': 'n2' } ], 'startNode': 'n2', 'makePreviews': True }
    >>> r = webapp.post_json('/submitquery', req)
    >>> import json
    >>> token = json.loads(r.body)['token']
    >>> r = webapp.get('/getresults?token=%s&start=0&limit=1' % token)
    >>> r = webapp.get('/getimages?token=%s' % token)                           # doctest: +SKIP
    >>> sorted(json.loads(r.body).items())                                      # doctest: +SKIP
    [(u'index', 0), (u'type', u'preview'), (u'url', u'/images/preview...png')]  # doctest: +SKIP
    '''
    token = int(bottle.request.query.token)
    if token not in app.tokens:
        item = { 'error': 'That token is invalid or has expired.' }
        yield json.dumps(item) + '\n'
        raise StopIteration
    makePreviews = app.tokens[token][1]
    if not makePreviews:
        item = { 'error': 'makePreviews was not set in the query submission.' }
        yield json.dumps(item) + '\n'
        raise StopIteration
    if bottle.request.query.block:
        imageDict = outQueue.get()
        outQueue.task_done()
        addImage(imageDict)
    while True:
        try:
            imageDict = outQueue.get_nowait()
        except Queue.Empty:
            break
        outQueue.task_done()
        addImage(imageDict)
    images = app.images.pop(token, [])
    while images:
        imageDict = images.pop(0)
        item = { 'type': 'preview', 'index': imageDict['index'], 'url': '/images/' + imageDict['path'] }
        yield json.dumps(item) + '\n'
    expireTokens()
    
def addImage(imageDict):
    imgToken = imageDict['token']
    try:
        app.images[imgToken].append(imageDict)
    except KeyError:
        app.images[imgToken] = [imageDict]

def addComparisonFilter(query, comparison, properties):
    pre = comparison['pre']
    if comparison['preType'] == 'property':
        pre = properties[pre]
    post = comparison['post']
    if comparison['postType'] == 'property':
        post = properties[post]
    query.addComparisonFilter(pre, comparison['operator'], post)

def addScoreInfo(results, row, metadata, idx):
    if idx == 0:
        metadata.extend(['score', 'parts', 'measures'])
    instruments = []
    mms = []
    filepath = ''
    for el in results:
        kind = el[1]['type']
        if (kind == 'Instrument'):
            instruments.append(el[1]['partName'])
        elif (kind == 'Measure'):
            mms.append(el[1]['number'])
        elif (kind == 'Score'):
            filepath = el[1]['corpusFilepath']
    measureTxt = '%d' % mms[0]
    if len(mms) > 1:
        measureTxt = '%d-%d' % (min(mms), max(mms))
    row.extend([filepath, ','.join(instruments), measureTxt])

def objectValueMap(data):
    kind = data['type']
    if kind == 'Note':
        return data['pitch']
    if kind == 'NoteSimultaneousWithNote':
        return data['harmonicInterval']
    if kind == 'NoteToNote' or kind == 'NoteToNoteByBeat':
        return data['interval']
    else:
        return '-'

def expireTokens():
    # Remove tokens that are older than 30 minutes.
    for token in app.tokens:
        if app.tokens[token][2] - time.time() > 1800:
            del app.tokens[token]


if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('-a', '--address', dest='address', default='127.0.0.1',
                      help='-a|--address : the IP address of the server')
    (options, args) = parser.parse_args()
    
    print "Loading relationship types..."
    app.db.listRelationshipTypes()
    print "Loading node property values..."
    app.db.listNodePropertyValues()
    print "Loading relationship property values..."
    app.db.listRelationshipPropertyValues()
    
    for worker in previewWorkers:
        worker.start()
    bottle.run(app, host=options.address, port=8080) #reloader=True
    
    
# This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. 
# If a copy of the MPL was not distributed with this file, You can obtain one at 
# http://mozilla.org/MPL/2.0/.

