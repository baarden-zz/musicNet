#!/usr/bin/python
#-------------------------------------------------------------------------------
# Name:         musicNetBottle.py
# Purpose:      a Bottle app providing a RESTful interface 
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
server running this app to execute queries and return the results. It requires that
the Python Bottle module be installed.

Services with a result containing multiple rows return each row as a separate JSONItem:
rather than returning one JSON document containing an array, each line
is a separate JSON document containing one item. Any service may return
a JSONItem dictionary with an 'error'/error message key/value pair, so
clients should check for this possibility.

Only query-related services are exposed by this interface. Actions that manipulate 
the database can only be done on the server using the musicNet objects directly.

The app is built with the Bottle framework and will start using its default options.

Here is a list of the available services:
'''

import os
import time
import json
import tempfile
import Queue
import threading
import bottle
import py2neo
import music21
import music21.musicNet

ENDOFQUEUE = -1


class GeneratePreviews(threading.Thread):
    ''' A threading class for producing previews in the background.
    '''
    
    def __init__(self, queue, out_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.outQueue = out_queue

    def run(self):
        while True:
            try:
                scoreDict = self.queue.get_nowait()
            except Queue.Empty:
                time.sleep(0.25)
                continue
            path = self.makePreview(scoreDict['score'])
            imageDict = { 'idx': scoreDict['line'], 'path': path, 'token': scoreDict['token'] }
            self.queue.task_done()
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
        conv.createPNG(filename)
        return os.path.basename(filename + '.png')


bottle.debug(True)
app = bottle.Bottle()
app.catchall = False
app.db = music21.musicNet.Database()
app.tokens = {}
app.images = {}

inQueue = Queue.Queue()
outQueue = Queue.Queue()
previewGen = GeneratePreviews(inQueue, outQueue)
previewGen.daemon = True
previewGen.start()


@app.get('/listscores')
def listScores():
    '''/listscores?start=0&limit=100
    
    Request parameters:
    
        start - The first row of scores to return (default=0).
        limit - The number of scores to return (default=100).
    
    Response: a JSONItem dictionary for each score in the database.
    
    Data structure:

        { movementName: name_of_score_file, _names: [ contributor, ... ], index: original_path_of_score_file }
        { movementName: name_of_score_file, _names: [ contributor, ... ], index: original_path_of_score_file }
        ...
    '''
    query = bottle.request.query
    start = int(query.start or 0)
    limit = int(query.limit or 100)
    rows = app.db.listScores(start, limit)
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/listnodetypes')
def listNodeTypes():
    '''/listnodetypes
    
    Request parameters: none.
    
    Response: a JSON array of node types in the database.
    
    Data structure:

        [ 'score', 'metadata', 'note', ... ]
    '''
    types = list(app.db.listNodeTypes())
    return json.dumps(types) + '\n'

@app.get('/listnodeproperties')
def listNodeProperties():
    '''/listnodeproperties
    
    Request parameters: none.
    
    Response: 
    
        A 2-element JSONItem array for each kind of note property in the database 
    
    Data structure:
    
        ['StaffGroup', 'offset']
        ['SystemLayout', 'distance']
        ['SystemLayout', 'quarterLength']
        ...
    '''
    rows = app.db.listNodeProperties()
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/listrelationshiptypes')
def listRelationshipTypes():
    '''/listrelationshiptypes
    
    Request parameters: none.
    
    Response: 
    
        A JSONItem dictionary for each kind of relationship property in the database 
    
    Data structure:
    
        {'start': 'Note', 'end': 'Measure', 'type': 'NoteInMeasure'}
        {'start': 'Part', 'end': 'Score', 'type': 'PartInScore'}
        ...
    '''
    rows = app.db.listRelationshipTypes()
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/listrelationshipproperties')
def listRelationshipProperties():
    '''/listrelationshipproperties
    
    Request parameters: none.
    
    Response: 
    
        A 2-element JSONItem array for each kind of Relationship in the database 
    
    Data structure:
    
        ['NoteToNoteByBeat', 'interval']
        ['NoteToNote', 'interval']
        ...
    '''
    rows = app.db.listRelationshipProperties()
    for row in rows:
        yield json.dumps(row) + '\n'

@app.get('/images/<filename:re:.*\.png>#')
def send_image(filename):
    tempdir = tempfile.gettempdir()
    return bottle.static_file(filename, root=tempdir, mimetype='image/png')

@app.post('/submitquery')
def prepareQuery():
    '''/submitquery
    
    This service processes a JSON query and returns an integer token that can be used
    to obtain results using 'getresults' and 'getimages'. Unlike
    the other services, it uses the POST method and expects the body of the request
    to be a JSON document containing the query. Tokens are good for a
    half hour before they expire.
    
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
    
    For instance,
    
        { 'nodes': [ { 'type': 'Note', 'name': 'n1' },
                     { 'type': 'Note', 'name': 'n2' } ]
          'relationships': [ { 'start': 'n1', 'type': 'NoteSimultaneousWithNote', 'end': 'n2', 'name': 'nswn' } ],
          'startNode': 'n1',
          'relationshipProperties': [ { 'relationship': 'nswn', 'property': 'sameOffset', 'name': 'nswnSo' },
                                      { 'relationship': 'nswn', 'property': 'simpleHarmonicInterval', 'name': 'nswnShi' } ],
          'comparisonFilters': [ { 'preType': 'property', 'pre': 'nswnSo', 'operator': '=', 'postType': 'value', 'post': 'True' },
                                 { 'preType': 'property', 'pre': 'nswnShi', 'operator': '=', 'postType': 'value', 'post': 7 } ],
          'returns': [],
          'makePreviews': 'True'
        }
        
    Data structure:
    
        {'token': -4258737148674360350}
    '''
    req = bottle.request.json
    q = music21.musicNet.Query(app.db)
    nodes = {}
    relations = {}
    properties = {}
    if not req.get('nodes', False):
        return { 'error': 'query has no nodes' }
    for n in req['nodes']:
        node = q.addNode(n['type'], n['name'])
        nodes[n['name']] = node
    for r in req.get('relationships', []):
        start = nodes[r['start']]
        end = nodes[r['end']]
        optional = getattr(r, 'optional', None)
        relation = q.addRelationship(relationType=r['type'], start=start, end=end, 
                                     name=r['name'], optional=optional)
        relations[r['name']] = relation
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
    if 'returns' in req:
        for r in req['returns']:
            q.addReturns(properties[r['property']])
    previews = req.get('makePreviews', False)
    if previews and q.returns:
        return { 'error': '"makePreviews" and "results" cannot be combined in the same query' }
    pattern = q._assemblePattern()
    token = hash(pattern)
    app.tokens[token] = [pattern, previews, time.time()]
    expireTokens()
    return { 'token': token }
    
@app.get('/getresults')
def results():
    '''/getresults?token=nnn&row=0&limit=10
    
    Request parameters:

        token - The number returned by the submitquery service
        row   - The first row of the query to return (default=0)
        limit - The number of query results to return (default=10)
    
    Response: 
    
        A series of JSONItem dictionaries: first the metadata for the
        results (the list of column names), then the data rows.
        A 'type' key indicates the kind of row, and the information
        is indexed by the 'data' key. Data rows also have a 'line'
        key indicating the line number of the result.
        
        A typical strategy is to send requests to this service,
        incrementing the row by the limit each time, until it
        returns an empty response.
    
    Data structure:
    
        {'type': 'metadata', 'data': ['nswn', 'p2', 'n1', 'p1', 'ntn1', 'n2', 'ntn2', 'p1', 'm1']}
        {'type': 'data', 'line': 0, 'data': [7, 'D4', 'F#4', 'B4', -5, 'B3', -3, 'Soprano', 'Tenor', 5]}
        {'type': 'data', 'line': 1, 'data': [19, 'B3', 'C#5', 'D5', -1, 'F#3', -5, 'Soprano', 'Bass', 6]}
        ...
    '''
    args = bottle.request.query
    token = int(args.token)
    minRow = int(args.row or 0)
    limit = int(args.limit or 10)
    if token not in app.tokens:
        item = { 'error': 'That token is invalid or has expired.' }
        yield json.dumps(item) + '\n'
        raise StopIteration
    pattern, previews, timestamp = app.tokens[token]
    q = music21.musicNet.Query(app.db)
    attempt = 0
    while True:
        try:
            data, metadata = q.results(minRow, limit, pattern)
            break
        except py2neo.rest.SocketError:
            attempt += 1
            if attempt > 3:
                item = { 'error': 'Unable to contact Neo4j server. Please contact the site administrator.' }
                yield json.dumps(item) + '\n'
                raise StopIteration
            time.sleep(1)
    for idx in range(len(data)):
        row = data[idx]
        queryIdx = idx + minRow
        if previews:
            score = q.music21Score(row, metadata)
            inQueue.put({ 'line': queryIdx, 'score': score, 'token': token })
            addScoreInfo(score, row, metadata, idx)
        else:
            for i in range(len(row)):
                if not isinstance(row[i], (str, unicode, float, int)):
                    row[i] = str(row[i])
        if idx == 0:  # wait until after previews have added data to return metadata
            item = { 'type': 'metadata', 'data': metadata }
            yield json.dumps(item) + '\n'
        item = { 'type': 'data', 'line': queryIdx, 'data': row }
        yield json.dumps(item) + '\n'
    expireTokens()

@app.get('/getimages')
def getImage():
    '''/getimages?token=nnn
    
    Request parameters:

        token - The number returned by the submitquery service
    
    Response: 
    
        Zero or more JSONItem objects. The 'url' key/value pair
        contains the path that an HTTP request can use to obtain the
        preview PNG. A 'line' key/value indicates the corresponding query
        result row for this image. A 'type' key indicating that this is a
        'preview' is included in case it's useful.
        
        An empty response is _not_ an indication that all previews have been
        generated. It is up to the client to keep track of how many
        previews are expected. Sending requests to this service more than
        2-4 times a second is probably not useful.
        Previews are generated after a call to the result service.
    
    Data structure:
    
        {'url': '/images/previewJHUqde.png', 'line': 0, 'type': 'preview'}
        {'url': '/images/previewriWX1Z.png', 'line': 1, 'type': 'preview'}
        {'url': '/images/previewf0PvMb.png', 'line': 2, 'type': 'preview'}
        ...
    '''
    token = int(bottle.request.query.token)
    if token not in app.tokens:
        item = { 'error': 'That token is invalid or has expired.' }
        yield json.dumps(item) + '\n'
        raise StopIteration
    previews = app.tokens[token][1]
    if not previews:
        item = { 'error': 'makePreviews was not set in the query submission.' }
        yield json.dumps(item) + '\n'
        raise StopIteration
    while True:
        try:
            imageDict = outQueue.get_nowait()
        except Queue.Empty:
            break
        outQueue.task_done()
        imgToken = imageDict['token']
        try:
            app.images[imgToken].append(imageDict)
        except KeyError:
            app.images[imgToken] = [imageDict]
    images = app.images.pop(token, [])
    while images:
        imageDict = images.pop(0)
        item = { 'type': 'preview', 'line': imageDict['idx'], 'url': '/images/' + imageDict['path'] }
        yield json.dumps(item) + '\n'
    expireTokens()

def addComparisonFilter(query, comparison, properties):
    pre = comparison['pre']
    if comparison['preType'] == 'property':
        pre = properties[pre]
    post = comparison['post']
    if comparison['postType'] == 'property':
        post = properties[post]
    query.addComparisonFilter(pre, comparison['operator'], post)

def addScoreInfo(score, row, metadata, idx):
    parts = score.getElementsByClass(music21.stream.Part)
    measures = parts[0].getElementsByClass(music21.stream.Measure)
    for i in range(len(row)):
        row[i] = objectValueMap(row[i])
    for i in range(len(parts)):
        if i == 0:
            metadata.append('p%d' % (i+1))
        instrument = parts[i].getElementsByClass(music21.instrument.Instrument)[0]
        row.append(instrument.partName)
    for i in range(len(measures)):
        if i == 0:
            metadata.append('m%d' % (i+1))
        row.append(measures[i].number)

def objectValueMap(obj):
    data = music21.musicNet._getPy2neoMetadata(obj)['data']
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
    ''' Remove tokens that are older than 30 minutes.
    '''
    for token in app.tokens:
        if app.tokens[token][2] - time.time() > 1800:
            del app.tokens[token]


bottle.run(app, host='localhost', port=8080, reloader=True)
