#!/usr/bin/python
#-------------------------------------------------------------------------------
# Name:         musicNet.py
# Purpose:      classes for connecting music21 to and searching Neo4j
#
# Authors:      Bret Aarden
# Version:      0.2
# Date:         29Jul2012
#
# License:      MPL 2.0
#-------------------------------------------------------------------------------

'''
.. testsetup:: *

    import music21.musicNet
    music21.musicNet._prepDoctests()

The musicNet module provides objects for searching music data in a network database
(specifically the Neo4j database), and for adding music21 data to the database. 
Queries are constructed by creating relationships between objects (notes, measures, parts, etc.).
This hides much of the mechanics of searching through music data, but
the database format is relatively fixed, unlike music21.

The :class:`Database` object provides an interface with the database to add a music21 
:class:`~music21.stream.Score` and get information about the database contents, 
such as the available scores, relationships, and properties.

The :class:`Query` object provides an interface for building and executing queries of the database.
It also has a :meth:`Query.music21Score` method for generating a music21
Score from a result.

There are also database :class:`Entity` objects that help with building the queries, including
:class:`Node`, :class:`Relationship`, :class:`Property`, and :class:`Filter` objects.

:class:`Moment` objects can be added to music21 objects using the module-level 
:meth:`addMomentsToScore` method. These objects have a similar function to
:class:`~music21.voiceLeading.VerticalSlice` objects. When present in a score, they
add cross-:class:`~music21.stream.Part` relationships to Notes in the database.

Configuration
-------------

This module requires that the `py2neo <http://py2neo.org/>`_ Python module be installed,
and that you have access to a `Neo4j <http://neo4j.org/>`_ database (version 1.8 or newer).

The database must be configured to use automatic indexing. (Well, technically it doesn't, 
but it will make open-ended searches a lot faster.) This involves editing the
`conf/neo4j.properties` file in the database folder. The following lines should be 
uncommented (the leading hash mark should be deleted) and edited as shown::

    node_auto_indexing=true
    node_keys_indexable=type
    relationship_auto_indexing=true
    relationship_keys_indexable=type

Neo4j operation is nearly plug-and-play. To start the database server from the command line,
just move to the database directory and enter::

    $ bin/neo4j start
    
After importing one or more files the relationship automatic index must still be manually
created with this command::

    $ bin/neo4j-shell -c 'index --create relationship_auto_index -t Relationship'
    
Contents
------------- 
'''

#TO DO:
#LONG TERM:
#Implement a programmatic replacement for Query.addCypherFilter(),
#  maybe by overriding Python operators, a la SQLAlchemy?
#Add inspect disk caching.
#Optimize speed and memory profiles. Goldberg Variations, argggh!

#Suggestions for music21:
#Change Measure.keySignature, .timeSignature, and .clef from None to weakRefs.
#Add direct means of manipulating Lilypond headers (removing tagline, etc.)

#Suggestions for py2neo
#Authorization on databases doesn't appear to work with .create().
#Tornado __del__ errors on exit should be fixed!

import os
import sys
import time
import random
import weakref
import unittest, doctest
import json
import sqlite3
import py2neo
import py2neo.neo4j
import music21
from music21 import *

def _prepDoctests():
    '''This function is run before starting doctests. 
    Results from the tests depend on the state of the database, and this function will
    purge the database and import a sample file.
    '''
    db = Database()
    db.wipeDatabase()
    bwv84_5 = music21.corpus.parse('bach/bwv84.5.mxl')
    addMomentsToScore(bwv84_5)
    db.addScore(bwv84_5)

def addMomentsToScore(score, forceAdd=False):
    '''Adds :class:`Moment` objects to a :class:`~music21.stream.Score`.
    Without them, the :meth:`Database.addScore` method will not 
    be able to add vertical note relationships to the database, such as
    `NoteSimultaneousWithNote` and `MomentInNote`.
    
    >>> from music21 import *
    >>> bwv84_5 = corpus.parse('bach/bwv84.5.mxl')
    >>> print len(bwv84_5)
    6
    >>> from music21.musicNet import *
    >>> addMomentsToScore(bwv84_5)
    >>> print len(bwv84_5)
    71
    '''
    for el in score:
        if el.__class__.__name__ == 'Moment':
            if forceAdd:
                break
            sys.stderr.write('This score already has Moments. ',
                             'Use the forceAdd=True argument to override.\n')
            return

    attackLookup = {}
    addNotesFromStream(attackLookup, score, 0)    
    attackOffsets = attackLookup.keys()
    attackOffsets.sort()
    
    import heapq
    releaseOffsets = []
    releaseLookup = {}
    for offset in attackOffsets:
        moment = Moment()
        # drop any sustained notes that have passed
        while (releaseOffsets and releaseOffsets[0] <= offset):
            del releaseLookup[releaseOffsets[0]]
            heapq.heappop(releaseOffsets) # always returns the lowest element
        # add any current sustained notes to the moment
        for r in releaseOffsets:
            for n in releaseLookup[r]:
                moment.addComponents(n, sameOffset=False)
        # add any new onsets to the moment
        # add a release reference for every note
        notes = attackLookup[offset]
        for note in notes:
            moment.addComponents(note, sameOffset=True)
            noteReleaseOffset = offset + note.quarterLength
            if (noteReleaseOffset not in releaseOffsets):
                heapq.heappush(releaseOffsets, noteReleaseOffset)
                releaseLookup[noteReleaseOffset] = weakref.WeakSet()
            releaseLookup[noteReleaseOffset].add(note)
        score.insert(offset, moment)

def addNotesFromStream(attackLookup, obj, offset):
    try:
        classes = obj._classes
    except AttributeError:
        return
    if classes == None:
        return
    offset += obj.offset
    if 'Stream' in classes:
        for el in obj:
            addNotesFromStream(attackLookup, el, offset)
        return
    if 'Note' not in classes:
        return
    # Add a reference to the Note indexed by offset
    try:
        attackLookup[offset].add(obj)
    except KeyError:
        attackLookup[offset] = weakref.WeakSet([obj])

def _signedModulo(val, mod):
    ''' This modulo function will return both negative and positive numbers.
    '''
    if val == 0:
        return 0
    sign = val / abs(val)
    while abs(val) >= mod:
        val -= mod * sign
    return val

def _cypherQuery(db, queryText, params=None):
    try:
        #version 1.6
        query = py2neo.neo4j.CypherQuery(db, queryText)
        if params:
            results = query.execute(**params) 
        else:
            results = query.execute()             
        return results.data, results.columns
    except AttributeError:
        #version 1.4
        results, metadata = py2neo.cypher.execute(db, queryText, params)
        return results, metadata.columns

def _getPy2neoMetadata(node):
    ''' Returns the hidden metadata of a py2neo object, 
    the location of which can change depending on the version of Neo4j.
    '''
    #1.4: return node._Resource__metadata['data'] # 1.4
    return node.__dict__['_properties']

def _convertFromString(val):
    if not isinstance(val, (str, unicode)):
        return val
    if val == 'None':
        val = None
    elif val == 'True':
        val = True
    elif val == 'False':
        val = False
    else:
        if val.find('.') >= 0:
            try:
                val = float(val)
            except ValueError: pass
        else:
            try:
                val = int(val)
            except ValueError: pass
    return val

# return the ID of a py2neo object
def _id(item):
    return item._id #__dict__['_id']

def _serverCall(func, *args):
    while True:
 #       try:
        r = func(*args)
#        except py2neo.rest.SocketError:
#            time.sleep(0.2)
#            continue
        return r    

def _fix535(results, metadata):
    orderColumn = -1
    for i in range(len(metadata)):
        if (metadata[i].find('INTERNAL_SORT') > -1):
            orderColumn = i
            metadata.pop(i)
            break
    if (orderColumn > -1):
        for i in range(len(results)):
            results[i].pop(orderColumn)

class NodeFarm():
    
    def __init__(self):
        sqlite3.register_converter("JSON", json.loads)
        self.sqldb = sqlite3.connect('', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.sqldb.row_factory = sqlite3.Row
        c = self.sqldb.cursor()
        c.execute('CREATE TABLE nodeLookup (hash INTEGER, parentHash INTEGER, vertex JSON, nodeRef INTEGER);')
        c.execute('CREATE TABLE edges (startNodeHash INTEGER, relationship TEXT, endNodeHash INTEGER, properties JSON);')
        c.execute('CREATE INDEX nodeLookup_hash_IDX on nodeLookup (hash);')
        self.sqldb.commit()
        self.writeBuffer = []
        
    def flushBuffer(self):
        if len(self.writeBuffer) == 0:
            return
        c = self.sqldb.cursor()
        for command in self.writeBuffer:
            c.execute(*command)
        self.sqldb.commit()
        self.writeBuffer = []
    
    def addNode(self, obj, parent, vertex=None):
        if len(self.writeBuffer) > 1000:
            self.flushBuffer()
        parentHash = parent
        if parent and not isinstance(parent, int):
            parentHash = hash(parent)
        if not vertex:
            vertex = {}
        if not 'type' in vertex:
            vertex['type'] = obj.__class__.__name__
        vertexJSON = json.dumps(vertex)
        values = { 'hash': hash(obj),
                   'parentHash': parentHash,
                   'vertex': vertexJSON }
        sql = 'INSERT INTO nodeLookup (hash, parentHash, vertex) VALUES (:hash, :parentHash, :vertex); '
        self.writeBuffer.append( (sql, values) )
        # return a copy of the node data with a real vertex
        valuesCopy = values.copy()
        valuesCopy['vertex'] = vertex
        return valuesCopy
    
    def updateNode(self, node, column, value):
        if len(self.writeBuffer) > 1000:
            self.flushBuffer()
        if column == 'vertex':
            value = json.dumps(value)
        values = { 'hashValue': node['hash'],
                   'value': value }
        sql = 'UPDATE nodeLookup SET %s = :value WHERE hash = :hashValue; ' % column
        self.writeBuffer.append( (sql, values) )
    
    def addEdge(self, start, relation, end, properties=None):
        if len(self.writeBuffer) > 1000:
            self.flushBuffer()
        startHash = start
        if not isinstance(start, int):
            startHash = hash(start)
        endHash = end
        if not isinstance(end, int):
            endHash = hash(end)
        if not isinstance(startHash, int) or not isinstance(endHash, int):
            raise Exception('bad hash!')
        values = { 'start': startHash,
                   'relation': relation, 
                   'end': endHash }
        if properties:
            sql = 'INSERT INTO edges (startNodeHash, relationship, endNodeHash, properties) VALUES (:start, :relation, :end, :props); '
            values['props'] = json.dumps(properties)
        else:
            sql = 'INSERT INTO edges (startNodeHash, relationship, endNodeHash) VALUES (:start, :relation, :end); '
        self.writeBuffer.append( (sql, values) )
        return values

    def getNodeFromHash(self, hashVal):
        self.flushBuffer()
        c = self.sqldb.cursor()
        c.execute('SELECT * FROM nodeLookup WHERE hash = ?;', (hashVal,))
        return c.fetchone()
    
    def getNodeFromObject(self, obj):
        return self.getNodeFromHash(hash(obj))
    
    def getNodeBatch(self, startIdx, limit=100):
        self.flushBuffer()
        c = self.sqldb.cursor()
        c.execute('SELECT * FROM nodeLookup WHERE ROWID >= :startIdx LIMIT :limit;', locals())
        results = []
        while True:
            result = c.fetchone()
            if not result:
                break
            results.append(result)
        return results
    
    def getEdgeBatch(self, startIdx, limit=100):
        self.flushBuffer()
        c = self.sqldb.cursor()
        c.execute('SELECT * FROM edges WHERE ROWID >= :startIdx LIMIT :limit;', locals())
        return c.fetchall()    

#-------------------------------------------------------------------------------
class Database(object):
    '''An object that connects to a Neo4j database, imports music21 scores,
    and provides information about the contents of the database.
    
    By default it assumes the Neo4j database is available at the standard
    location on the current machine (`http://localhost:7474/db/data/`), but the
    `uri` argument can be used to specify a remote or non-standard location.
    (Note that the URI should end with a slash!) Any keyword arguments are
    passed on to a :class:`py2neo.neo4j.GraphDatabaseService` object.
    
    >>> db = Database()
    >>> print db.graph_db
    GraphDatabaseService('http://localhost:7474/db/data/')
    '''
    
    _DOC_ORDER = [ 'wipeDatabase', 'addScore', 'listScores', 'listNodeTypes', 'listNodeProperties', 
                   'listRelationshipTypes', 'listRelationshipProperties', 'addPropertyCallback' ]
    _DOC_ATTR = {
    'graph_db': 'The instance of a :class:`py2neo.neo4j.GraphDatabaseService` object connected to this object, which is in turn connected to a Neo4j server either at the default location on the present computer, or the one specified by the Database `uri` argument.',             
    }
    HIDEFROMDATABASE = 1

    def __init__(self, uri='http://localhost:7474/db/data/', **kwargs):
        try:
            self.graph_db = py2neo.neo4j.GraphDatabaseService(uri, **kwargs)
        except py2neo.rest.SocketError:
            sys.exit('Unable to connect to database.\n')
        self._db_kwargs = kwargs
        self._db_uri = uri
        self._callbacks = {}
        self._extractState = {}
        self._defaultCallbacks()
        self._m21SuperclassLookup = self._inspectMusic21ExpressionsArticulations()
        self._skipProperties = ('_activeSite', 'id', '_classes', 'groups', 'sites',
                               '_derivation', '_overriddenLily', '_definedContexts', '_activeSiteId', 
                               '_idLastDeepCopyOf', '_mutable', '_elements', '_cache', 'isFlat', 
                               'autosort', '_components', '_unlinkedDuration', 'isSorted', 
                               'flattenedRepresentationOf', '_reprHead', 'idLocal', 'autoSort',
                               'inherited', '_fullyQualifiedClasses', 'filePath', 'fileFormat', 
                               'fileNumber', 'spannedElements') 

    def wipeDatabase(self):
        '''Removes all relationships and nodes from the database.

        >>> db = Database()
        >>> db.wipeDatabase()
        >>> print db.graph_db.get_relationship_count()
        0
        >>> print db.graph_db.get_node_count()
        1
        >>> bwv84_5 = corpus.parse('bach/bwv84.5.mxl')  # doctest: hide
        >>> addMomentsToScore(bwv84_5)                  # doctest: hide
        >>> db.addScore(bwv84_5)                        # doctest: hide

        The node count can never go below 1 because Neo4j always keeps a reference node 
        in its network graph.
        '''
        q = Query(self)
        q.setStartRelationship()
        while (_serverCall(self.graph_db.get_relationship_count)):
            results, meta = q.results(limit=100)
            results = [x[0] for x in results]
            _serverCall(self.graph_db.delete, *results)
        q = Query(self)
        q.setStartNode()
        while (_serverCall(self.graph_db.get_node_count) > 1):
            results, meta = q.results(limit=100)
            results = [x[0] for x in results]
            _serverCall(self.graph_db.delete, *results)

    def addScore(self, score, verbose=False):
        '''Adds a music21 :class:`~music21.stream.Score` to the database.
        To see progress on the import, we can set the `verbose` argument to `True`.
        
        In order to be able to access vertical note relationships such as
        `NoteSimultaneousWithNote` and `MomentInNote`,
        we need to add :class:`Moment` objects to the score using the 
        module-level :meth:`addMomentsToScore` method before calling this method.
        
        >>> db = Database()
        >>> db.wipeDatabase() # doctest: hide
        >>> bwv84_5 = corpus.parse('bach/bwv84.5.mxl')
        >>> addMomentsToScore(bwv84_5)
        >>> db.addScore(bwv84_5)
        >>> print db.graph_db.get_node_count()
        457
        >>> print db.graph_db.get_relationship_count()
        1517
        '''
        self.nodeRefs = {}
        self.maxNodes = 0
        self.maxEdges = 0
        self.nodeFarm = NodeFarm()
        self._extractState = { 'verbose': verbose,
                              'nodeCnt': 0,
                              'relationCnt': 0,
                              'nodeLookup': {} } #vertex, parent, voice
        import copy
        if verbose:
            self.lastProgress = 0
            self._timeUpdate(report=False)
            self._extractState['partItemMax'] = sum([len(x) for x in score.parts])
            sys.stderr.write('Extracting music21 objects..........')
        self._extractNodes(score)        
        self._writeNodesToDatabase()        
        self._writeEdgesToDatabase(score)

    def listScores(self, start=0, limit=100):
        '''Returns a list of dict objects with information about the scores that have been added 
        to the database, with keys for `movementName` and `_names` (a list of 
        contributor/composer names).

        >>> db = Database()
        >>> expectedScore = [(u'_names', [None]), (u'corpusFilepath', u'bach/bwv84.5.mxl'), (u'movementName', u'bwv84.5.mxl')]
        >>> scores = db.listScores()
        >>> sorted(scores[0].items()) == expectedScore
        True
        '''
        q = Query(self)
        score = q.setStartNode(nodeType='Score')
        inScore = q.addRelationship(relationType='MetadataInScore', end=score, optional=True)
        meta = inScore.start
        inMetadata = q.addRelationship(relationType='ContributorInMetaData', end=meta, optional=True)
        contributor = inMetadata.start
        q.addReturns(meta.movementName, contributor._names, score.corpusFilepath)
        q.limitReturnToWhere = True
        results, meta = q.results(start, limit)
        columns = [x[x.find('.') + 1:] for x in meta]
        scores = {}
        for row in results:
            index = row[0] or row[2]
            if index not in scores:
                scores[index] = {}
            for i in range(len(row)):
                col = columns[i]
                if col not in scores[index]:
                    scores[index][col] = set()
                scores[index][col].add(row[i])
        for score in scores.itervalues():
            for col in score:
                if col == '_names':
                    score[col] = list(score[col])
                else:
                    score[col] = score[col].pop()
        return scores.values()

    def listNodeTypes(self):
        '''Returns a set of node types available in the database.
        
        >>> db = Database()
        >>> nTypes = db.listNodeTypes()
        >>> print 'Instrument' in nTypes
        True
        '''
        if hasattr(self, 'nTypes'):
            return self.nTypes
        self.listRelationshipTypes()
        return self.nTypes

    def listNodeProperties(self):
        '''Returns a list of node properties in the database, represented as tuples
        (node type, property name).
        
        For instance, to see what properties are available in score nodes:
        
        >>> db = Database()
        >>> props = db.listNodeProperties()
        >>> print sorted( [x for x in props if x[0]=='Score'] )
        [(u'Score', u'_atSoundingPitch'), (u'Score', u'_priority'), (u'Score', u'corpusFilepath'), (u'Score', u'hideObjectOnPrint'), (u'Score', u'offset')]
        '''
        if hasattr(self, 'nodeProperties'):
            return self.nodeProperties
        if not hasattr(self, 'nTypes'):
            self.listNodeTypes()
        self.nodeProperties = []
        self.nodePropertyValues = []
        q = Query(self)
        q.limitReturnToWhere = False
        for nodeType in self.nTypes:
            q.setStartNode(nodeType=nodeType)
            results, meta = q.results(limit=10000)
            results = [x[0] for x in results]
            nodes = _serverCall(self.graph_db.get_properties, *results)
            properties = {}
            for node in nodes:
                for prop in node:
                    if prop == 'type':
                        continue
                    try:
                        propSet = properties[prop]
                    except:
                        propSet = properties[prop] = set()
                    propSet.add(node[prop])
            for p in properties:
                self.nodeProperties.append((nodeType, p))
                values = list(properties[p])
                values.sort()
                self.nodePropertyValues.append((nodeType, p, tuple(values)))
        return self.nodeProperties
    
    def listNodePropertyValues(self):
        '''Returns a list of lists of values used by node properties in the database
        (node type, property name, (value1, value2, ...)).
        
        For instance, to see what part names are used by instruments in the database:
        
        >>> db = Database()
        >>> values = db.listNodePropertyValues()
        >>> print [x for x in values if x[0]=='Instrument' and x[1]=='partName']
        [(u'Instrument', u'partName', (u'Alto', u'Bass', u'Soprano', u'Tenor'))]
        '''
        if hasattr(self, 'nodePropertyValues'):
            return self.nodePropertyValues
        self.listNodeProperties()
        return self.nodePropertyValues                
    
    def listRelationshipTypes(self):
        '''Returns a list of relationship types in the database, represented as 
        dict objects with keys for `start`, `type`, and `end`. By convention,
        starts and ends in relationships read as a right-directed arrow::
        
            Start--Relationship-->End
            
        For instance, to see if any of the scores in the database 
        have `MetadataInScore` relationships (let's hope they do!), we could do this:
        
        >>> db = Database()
        >>> rTypes = db.listRelationshipTypes()
        >>> print [x['type'] for x in rTypes if x['type']=='MetadataInScore']
        [u'MetadataInScore']
        '''
        if hasattr(self, 'rTypes'):
            return self.rTypes
        self.rTypes = []
        self.nTypes = set()
        rTypes = set()
        relateTypes = []
        while not relateTypes:
            queryText = 'START r=relationship(*) RETURN DISTINCT TYPE(r);'
            relateTypes, metadata = _cypherQuery(self.graph_db, queryText)
        for relateType in relateTypes:
            q = Query(self)
            r = q.setStartRelationship(relationType=str(relateType[0]))
            q.addReturns(r.start.type, r.end.type)
            q.limitReturnToWhere = True;
            results, meta = q.results(limit=100)
            for n1, n2 in results:
                rTypes.add( (n1, relateType[0], n2) )
                self.nTypes.add(n1)
                self.nTypes.add(n2)
        for start, r, end in rTypes:
            self.rTypes.append( { 'start': start, 'type': r, 'end': end } )
        return self.rTypes
    
    def listRelationshipProperties(self):
        '''Returns a list of relationship properties in the database, represented as tuples
         (relationship type,  property name).
        
        For instance, to see what properties are available in `NoteSimultaneousWithNote`
        relationships:
        
        >>> db = Database()
        >>> rProps = db.listRelationshipProperties()
        >>> print sorted( [x for x in rProps if x[0]=='NoteSimultaneousWithNote'] )
        [(u'NoteSimultaneousWithNote', u'harmonicInterval'), (u'NoteSimultaneousWithNote', u'sameOffset'), (u'NoteSimultaneousWithNote', u'simpleHarmonicInterval')]
        '''
        if hasattr(self, 'relateProperties'):
            return self.relateProperties
        rTypes = set()
        for x in self.listRelationshipTypes():
            rTypes.add(x['type']) 
        self.relateProperties = []
        self.relatePropertyValues = []
        for rType in rTypes:
            q = Query(self)
            q.setStartRelationship(relationType=rType)
            results, meta = q.results(limit=10000)
            results = [x[0] for x in results]
            nodes = _serverCall(self.graph_db.get_properties, *results)
            properties = {}
            for relate in nodes:
                for prop in relate:
                    if prop == 'type':
                        continue
                    try:
                        propSet = properties[prop]
                    except:
                        propSet = properties[prop] = set()
                    propSet.add(relate[prop])
            for p in properties:
                self.relateProperties.append((rType, p))
                values = list(properties[p])
                values.sort()
                self.relatePropertyValues.append((rType, p, tuple(values)))
        return self.relateProperties

    def listRelationshipPropertyValues(self):
        '''Returns a list of lists of values used by relationship properties in the database
        (relationship type, property name, (value1, value2, ...)).
        
        For instance, to see what melodic intervals are used in the database:
        
        >>> db = Database()
        >>> values = db.listRelationshipPropertyValues()
        >>> print [x for x in values if x[0]=='NoteToNote' and x[1]=='interval']
        [(u'NoteToNote', u'interval', (-12, -7, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 7, 12))]
        '''
        if hasattr(self, 'relatePropertyValues'):
            return self.relatePropertyValues
        self.listRelationshipProperties()
        return self.relatePropertyValues                
    
    def addPropertyCallback(self, entity, callback):
        '''**For advanced use only.**
        
        All music21 objects have default handling when being imported. When
        default handling isn't enough, one or more callbacks can be added to
        preprocess the object, or prevent it from being added to the database.
        The `entity` argument is the class name the callback should be added to
        (as a string), and the `callback` argument is a function object to be
        called. Multiple callbacks can be added to the same class, and an object
        can have callbacks for multiple classes (for instance, a trill will have
        callbacks for both 'Trill' and 'Expression'). Built-in callbacks are set 
        in the internal :meth:`_defaultCallbacks` method.
        '''
        if entity not in self._callbacks:
            self._callbacks[entity] = []
        self._callbacks[entity].append(callback)

    def _defaultCallbacks(self):
        HIDEFROMDATABASE = self.HIDEFROMDATABASE
        
        # Score
        def addNumbersToParts(db, score, scoreVertex, emptyNode):
            number = 0
            for obj in score:
                if isinstance(obj, music21.stream.Part):
                    number = number + 1
                    partVertex = { 'number': number }
                    self.nodeFarm.addNode(obj, score, partVertex)
                    self.maxNodes = self.maxNodes + 1
        self.addPropertyCallback('Score', addNumbersToParts)
        
        # Contributor
        def addTextToContributor(db, contributor, vertex, metadataNode):
            contributor._names = unicode(contributor.name)
        self.addPropertyCallback('Contributor', addTextToContributor)
        
        # Part
        def resetExtractState(db, part, partVertex, scoreNode):
            db._extractState['history'] = { 'NoteToNote': {} }
            for key in ('clef', 'timeSignature', 'keySignatureSharps', 'keySignatureMode'):
                db._extractState[key] = None
        self.addPropertyCallback('Part', resetExtractState)
                
        # Measure
        def addSignaturesAndClefs(db, measure, vertex, partNode):
            if measure.clef:
                self._extractState['clef'] = measure.clef.classes[0]
            #vertex['barDuration'] = self._extractState['barDuration']
            vertex['clef'] = self._extractState['clef']
            if measure.timeSignature:
                self._extractState['timeSignature'] = measure.timeSignature.ratioString
            vertex['timeSignature'] = self._extractState['timeSignature']
            if measure.keySignature:
                self._extractState['keySignatureSharps'] = measure.keySignature.sharps
                self._extractState['keySignatureMode'] = measure.keySignature.mode
            vertex['keySignatureSharps'] = self._extractState['keySignatureSharps']
            vertex['keySignatureMode'] = self._extractState['keySignatureMode']
        self.addPropertyCallback('Measure', addSignaturesAndClefs)            
        
        # Chord
        def labelChordNotes(db, chordObj, chordVertex, measureNode):
            chordObj.sortAscending(inPlace=True)
            for i in range(len(chordObj)):
                noteObj = chordObj[i]
                noteVertex = { 'voice': len(chordObj) - i }
                db._addNode(noteObj, measureNode, noteVertex)
            return HIDEFROMDATABASE
        self.addPropertyCallback('Chord', labelChordNotes)
        
        # Voice
        def labelVoiceNotes(db, voice, voiceVertex, measureNode):
            for noteObj in voice:
                noteVertex = { 'voice': int(voice.id) }
                db._addNode(noteObj, measureNode, noteVertex)
            return HIDEFROMDATABASE
        self.addPropertyCallback('Voice', labelVoiceNotes)

        # Note
        def addNoteVoiceleading(db, noteObj, vertex, measureNode):
            def addVoiceleading(db, byBeat, noteVertex, measureNode):
                if noteObj.isRest:
                    return
                history = db._extractState['history']
                voice = noteVertex['voice']
                try:
                    prevNote, prevOffset = history['NoteToNote'][voice][byBeat]
                    voiceleadingHistory = True
                except KeyError:
                    voiceleadingHistory = False
                # Only calculate voiceleading for notes within the span of a measure.
                if (voiceleadingHistory and 
                        offset - prevOffset <= self._extractState['barDuration']):
                    mint = noteObj.midi - prevNote.midi
                    db._addEdge(prevNote, 'NoteToNote', noteObj, { 'interval': mint, 'byBeat': byBeat } )
                try:
                    saveLoc = history['NoteToNote'][voice]
                except KeyError:
                    saveLoc = history['NoteToNote'][voice] = {}
                saveLoc[byBeat] = [noteObj, offset]
            
            offset = measureNode['vertex']['offset'] + noteObj.offset
            if 'voice' not in vertex:
                vertex['voice'] = 1
            addVoiceleading(db, 'False', vertex, measureNode)
            if noteObj.offset % 1 == 0:
                addVoiceleading(db, 'True', vertex, measureNode)
            #del vertex['voice']
        self.addPropertyCallback('Note', addNoteVoiceleading)

        # Pitch
        def addPitchToNote(db, pitchObj, pitchVertex, noteNode):
            noteVertex = noteNode['vertex']
            noteVertex['pitch'] = pitchObj.nameWithOctave
            noteVertex['midi'] = pitchObj.midi
            noteVertex['microtone'] = pitchObj.microtone.cents
            self.nodeFarm.updateNode(noteNode, 'vertex', noteVertex)
            return HIDEFROMDATABASE
        self.addPropertyCallback('Pitch', addPitchToNote)

        # Duration
        def addDurationToParent(db, durationObj, vertex, parentNode):
            if (durationObj.quarterLength == 0):
                return HIDEFROMDATABASE
            parentVertex = parentNode['vertex']
            parentType = parentVertex['type']
            needsUpdate = False
            if parentType not in ('StaffGroup', 'Instrument', 'Metadata'):
                parentVertex['quarterLength'] = durationObj.quarterLength
                if (durationObj.tuplets):
                    tuplets = []
                    for tuplet in durationObj.tuplets:
                        tuplets.append('%d:%d' % (tuplet.tupletActual[0], tuplet.tupletNormal[0]))
                    parentVertex['tuplet'] = '*'.join(tuplets)
                needsUpdate = True
            if parentType == 'Note':
                parentVertex['isGrace'] = durationObj.isGrace
                if hasattr(durationObj, 'stealTimePrevious'):
                    for attr in ('stealTimePrevious', 'stealTimeFollowing', 'slash'):
                        parentVertex[attr] = getattr(durationObj, attr)
                needsUpdate = True
            if needsUpdate:
                self.nodeFarm.updateNode(parentNode, 'vertex', parentVertex)
            return HIDEFROMDATABASE
        self.addPropertyCallback('Duration', addDurationToParent)
        
        # MetronomeMark
        def simplifyText(db, mm, vertex, partNode):
            mm._tempoText = unicode(mm._tempoText)
        self.addPropertyCallback('MetronomeMark', simplifyText)
        
        # Expression, Articulation
        def useAbstractType(db, obj, vertex, noteNode):
            abstractions = ('Expression', 'Articulation')
            superclass = [x for x in obj.classes if x in abstractions][0]
            vertex['type'] = superclass
            vertex['name'] = obj.__class__.__name__
        self.addPropertyCallback('Expression', useAbstractType)
        self.addPropertyCallback('Articulation', useAbstractType)

        # Trill, Mordent, Turn, Schleifer
        def simplifyOrnamentInterval(db, ornament, vertex, noteNode):
            if not hasattr(ornament, 'size'):
                return
            if not isinstance(ornament.size, (str, unicode)):
                ornament.size = ornament.size.directedName
        self.addPropertyCallback('Trill', simplifyOrnamentInterval)
        self.addPropertyCallback('GeneralMordent', simplifyOrnamentInterval)
        self.addPropertyCallback('Turn', simplifyOrnamentInterval)
        self.addPropertyCallback('Schleifer', simplifyOrnamentInterval)

        # Beams
        def addBeams(db, beams, vertex, noteNode):
            for beam in beams.beamsList:
                db._addNode(beam, noteNode)
            return HIDEFROMDATABASE
        self.addPropertyCallback('Beams', addBeams)
        
        # Clef
        def addMidmeasureClefs(db, clefObj, vertex, measureNode):
            if clefObj.offset == 0:
                return HIDEFROMDATABASE
            vertex['type'] = 'MidmeasureClef'
            vertex['name'] = clefObj.__class__.__name__
        self.addPropertyCallback('Clef', addMidmeasureClefs)
        
        # Moment
        def addCrossPartRelationships(db, moment, vertex, scoreNode):
            simultaneous = list(moment.simultaneous)
            for noteObj in simultaneous:
                db._addEdge(noteObj, 'MomentInNote', moment, { 'startMoment': False })
            sameOffset = list(moment.sameOffset)
            for noteObj in sameOffset:
                db._addEdge(noteObj, 'MomentInNote', moment, { 'startMoment': True })
            notes = sameOffset + simultaneous
            simuls = {}
            for i in range(len(notes) - 1):
                note1 = notes[i]
                for j in range(i + 1, len(notes)):
                    note2 = notes[j]
                    if note1 in simuls:
                        if note2 in simuls[note1]: continue
                    else:
                        simuls[note1] = {}
                    if note2 in simuls and note1 in simuls[note2]:
                        continue
                    simuls[note1][note2] = True
                    cInt = abs(note1.midi - note2.midi)
                    sInt = abs(_signedModulo(note1.midi - note2.midi, 12))
                    properties = { 'harmonicInterval': cInt,
                                   'simpleHarmonicInterval': sInt,
                                   'sameOffset': 'False' }
                    if note1.offset == note2.offset:
                        properties['sameOffset'] = 'True'
                    db._addEdge(note1, 'NoteSimultaneousWithNote', note2, properties)
        self.addPropertyCallback('Moment', addCrossPartRelationships)
        
        # Spanner
        def addSpannerRelationship(db, span, vertex, parentNode):
            kind = span.__class__.__name__
            if len(span.getSpannedElements()) > 2:
                if kind == 'StaffGroup':
                    for i in range(len(span)):
                        part = span[i]
                        db._addEdge(part, 'PartInStaffGroup', span)
                    return
                elif kind not in ('Slur'):
                    raise TypeError('Handling of "%s" Spanners is not supported.' % kind)
            first = span.getFirst()
            last = span.getLast()
            span.name = kind
            # music21 1.0 handling of gracenote slurs is broken, therefore:
            if (first.measureNumber != None and last.measureNumber != None):
                db._addEdge(first, 'spannerTo', last, span)
            return HIDEFROMDATABASE
        self.addPropertyCallback('Spanner', addSpannerRelationship)

        # Optional objects
        # NoteEditorial
        def skipIfEmpty(db, obj, vertex=None, parentNode=None):
            objDict = obj.__dict__
            for key, val in objDict.iteritems():
                if key in self._skipProperties:
                    continue
                if key == 'position':
                    continue # There are empty objects with non-empty positions.
                if not val:
                    continue
                if hasattr(val, '__dict__'):
                    if skipIfEmpty(db, val):
                        continue
                return
            return HIDEFROMDATABASE
        self.addPropertyCallback('NoteEditorial', skipIfEmpty)
        
        # TimeSignature
        def getBarDuration(db, timeSig, vertex, measureNode):
            self._extractState['barDuration'] = timeSig.barDuration.quarterLength
            return HIDEFROMDATABASE
        self.addPropertyCallback('TimeSignature', getBarDuration)
        
        # Omitted objects
        # KeySignature, MiscTandam
        def skipThisObject(db, obj, vertex, parentNode):
            return HIDEFROMDATABASE
        self.addPropertyCallback('KeySignature', skipThisObject)
        self.addPropertyCallback('MiscTandam', skipThisObject)

    def _inspectMusic21ExpressionsArticulations(self):
        import inspect
        lookup = {}
        # search music21 modules:
        for module in (expressions, articulations):
            sName = module.__name__[8:].capitalize()
            classes = inspect.getmembers(module, inspect.isclass)
            for cName, ref in classes:
                lookup[cName] = sName
        return lookup

    def _timeUpdate(self, report=True):
        newtime = time.time()
        if report:
            sys.stderr.write('(%.1f seconds)\n' % (newtime - self.refTime))
        self.refTime = newtime

    def _progressReport(self, state, minIn, maxIn, minOut, maxOut):
        if not hasattr(self, 'lastProgress'):
            lastOut = self.lastProgress = 0
        lastOut = self.lastProgress
        rangeIn = maxIn - minIn
        rangeOut = maxOut - minOut
        progress = rangeOut * (state - minIn) / rangeIn + minOut
        progress = 5 * round(progress / 5)
        if progress >= lastOut + 5:
            increment = int(60.0 * (5.0 / rangeOut))
            sys.stderr.write('=' * increment)
            self.lastProgress = progress
    
    def _extractNodes(self, obj, parentNode=None):
        '''
        Put all the hierarchical nodes and their relationships into linear order.
        At this point relationships store references to the original music21 objects.
        '''
        state = self._extractState
        if state['verbose'] and parentNode and parentNode['vertex']['type'] in ('Part', 'PartStaff'):
            state['partItemCnt'] = state.get('partItemCnt', 0) + 1
            self._progressReport(state['partItemCnt'], 0, state['partItemMax'], 0, 5)
        objNode = self._addNode(obj, parentNode)
        if objNode == self.HIDEFROMDATABASE:
            return
        if hasattr(obj, 'classes') and 'Spanner' in obj.classes:
            return
        if obj.__class__.__name__ == 'Moment':
            return
        try:
            itemList = list(obj)
        except TypeError:
            return
        for item in itemList:
            if item == obj:
                continue
            self._extractNodes(item, objNode)

    def _addNode(self, node, parentData=None, vertex=None):
        kind = node.__class__.__name__
        if not vertex:
            vertex = {}
        vertex['type'] = kind
        if hasattr(node, 'offset'):
            vertex['offset'] = node.offset
        if self._runCallbacks(node, vertex, parentData) == self.HIDEFROMDATABASE:
            return self.HIDEFROMDATABASE
        parentHash = None
        if parentData:
            parentHash = parentData['hash']
        nodeData = self.nodeFarm.addNode(node, parentHash, vertex)
        self.maxNodes = self.maxNodes + 1
        if parentData:
            relation = nodeData['vertex']['type'] + 'In' + parentData['vertex']['type']
            self._addEdge(node, relation, parentData['hash'])
        if 'Spanner' in getattr(node, '_classes', []) or kind == 'Moment':
            return
        self._extractObject(node, nodeData)
        return nodeData

    def _addEdge(self, start, relationship, end, propertyNode=None):
        if isinstance(propertyNode, dict):
            properties = propertyNode
        elif isinstance(propertyNode, base.Music21Object): # in case of Spanners
            propertyData = self._extractObject(propertyNode)
            properties = propertyData['vertex']
        else:
            properties = {}
        properties['type'] = relationship
        self.nodeFarm.addEdge(start, relationship, end, properties)
        self.maxEdges = self.maxEdges + 1
           
    def _runCallbacks(self, node, nodeData, parentData):
        keys = self._callbacks.keys()
        if hasattr(node, 'classes'):
            kinds = [x for x in node.classes if x in keys]
        else:
            name = node.__class__.__name__
            try: 
                kinds = [name, self._m21SuperclassLookup[name]]
            except KeyError:
                kinds = [name]
        for kind in kinds:
            if kind not in self._callbacks:
                continue
            for callback in self._callbacks[kind]:
                rc = callback(self, node, nodeData, parentData)
                if rc != None:
                    return rc

    # get data from object; extract subnodes if necessary
    def _extractObject(self, obj, objData=None, parentNode=None):
        if isinstance(obj, Moment):
            return
        if not objData:
            parentHash = None
            if parentNode:
                parentHash = parentNode['hash']
            objData = self.nodeFarm.addNode(obj, parentHash)
            self.maxNodes = self.maxNodes + 1
        objectDict = obj.__dict__
        vertex = objData['vertex']
        for key, val in objectDict.iteritems():
            if key in self._skipProperties:
                continue
            if val == None and key in ('_duration'):
                continue
            elif isinstance(val, list):
                for item in val:
                    if hasattr(item, '__dict__'):
                        self._extractNodes(item, objData)
                continue
            elif isinstance(val, dict):
                for key, text in val.iteritems():
                    vertex[key] = text
                continue
            elif isinstance(val, music21.musicxml.base.MusicXMLElement):
                continue
            elif hasattr(val, '__dict__'):
                self._extractNodes(val, objData)
                continue
            if key == 'type':
                key = 'm21_' + key
            vertex[key] = val
        for key, val in vertex.items():
            if not isinstance(val, (int, float, long)):
                val = unicode(val)
            vertex[key] = val
        self.nodeFarm.updateNode(objData, 'vertex', vertex)
        return objData
    
    def _writeNodesToDatabase(self):
        '''
        When nodes are written to the database in order, 
        references to their database entries will be returned in the same order.
        Those references are saved as attributes in the original nodes.
        '''
        batchSize = 100
        verbose = self._extractState['verbose']
        if verbose:
            self._timeUpdate()
            sys.stderr.write('Writing nodes to database...........')
            cnt = 0
        idx = 0
        while True:
            subset = self.nodeFarm.getNodeBatch(idx, batchSize)
            batchLen = len(subset)
            if batchLen == 0:
                break
            vertices = [x['vertex'] for x in subset]
            results = _serverCall(self.graph_db.create, *vertices)
            # Store a nodeRef reference for each music21 object 
            # with the address of its corresponding Neo4j node.
            for i in range(len(results)):
                self.nodeRefs[subset[i]['hash']] = results[i]
            self._extractState['nodeCnt'] += len(results)
            idx += len(results)
            if verbose:
                cnt += len(results)
                self._progressReport(cnt, 0, self.maxNodes, 5, 25)
        
    def _writeEdgesToDatabase(self, score):
        '''
        Before relationships are written to the database, music21 object references are converted 
        to their corresponding database nodes.
        '''
        batchSize = 100
        verbose = self._extractState['verbose']
        if verbose:
            self._timeUpdate()
            sys.stderr.write('Writing relationships to database...')
        idx = 0
        while True:
            subset = self.nodeFarm.getEdgeBatch(idx, batchSize)
            batchLen = len(subset)
            if batchLen == 0:
                break
            edgeRefs = []
            for edge in subset:
                ref1 = self.nodeRefs[edge['startNodeHash']]
                ref2 = self.nodeRefs[edge['endNodeHash']]
                edgeRef = [ref1, edge['relationship'], ref2]
                if edge['properties']:
                    edgeRef.append(edge['properties'])
                edgeRefs.append(tuple(edgeRef))
            _serverCall(self.graph_db.create, *edgeRefs)
            #self._extractState['relationCnt'] += batchLen
            idx += batchSize
            if verbose:
                self._progressReport(idx, 0, self.maxEdges, 25, 100)
        if verbose:
            self._timeUpdate()

#-------------------------------------------------------------------------------
class Query(object):
    '''This object provides an interface for building and executing queries of the
    database. The first argument to the object must be an existing
    :class:`Database` object.
    
    All queries begin with a starting point, either a node or a relationship.
    This point is set with either the :meth:`setStartNode` or
    :meth:`setStartRelationship` method (but not both).  These nodes and
    relationships are represented by :class:`Node` and :class:`Relationship`
    objects, which are usually created automatically. Note that a starting point
    alone is a complete query structure!

    To get the results and metadata for a query, we use the :meth:`results`, 
    method.
    
    Optionally, we can add a number of restrictions to the search. The
    :meth:`addRelationship` method allows us to connect our start node/relationship to
    other nodes. Any number of nodes can be connected using relationships.
    Each node and relationship in the database has properties, and we use them to filter
    our search using the :meth:`addComparisonFilter` method. 
    
    Note that if you create potentially symmetrical queries spanning multiple parts 
    you may get duplicate results.
    
    By default, results are returned as references to entities in the database,
    which can be used by the :meth:`music21Score` method to return a music21
    score fragment for any result row. If we want specific data from the search,
    we can pass those database entities to the :meth:`getResultProperties`
    method to get all of their properties, we can use the :meth:`addReturns`
    method to specify the particular properties we're interested in.
    
    To understand what's going on behind the scenes, 
    it may be helpful to read the documentation on the Neo4j 
    `Cypher query language <http://docs.neo4j.org/chunked/stable/cypher-query-lang.html>`_.
    '''
    
    _DOC_ORDER = [ 'setStartNode', 'results', 'getResultProperties', 
                   'setStartRelationship', 'addRelationship', 'addComparisonFilter', 'addCypherFilter', 
                   'addReturns', 'setOrder',  'music21Score', 'setObjectCallback' ]
    _DOC_ATTR = {
    'db': 'Blah',
    'results': 'Blah',
    'metadata': 'Blah',
    'pattern': 'Blah',
    'nodes': 'Blah'
    }
    
    def __init__(self, db):
        self.db = db
        self._constructCallbacks = {}
        self.start = self.pattern = None
        self.match = []
        self.where = []
        self.limitReturnToWhere = False
        self.orders = []
        self.returns = []
        self.returnStr = ''
        self.nodes = set()
        self.limit = ''
        self.phrases = {}
        self._usedNames = []
        self._defaultCallbacks()
        self._inspectMusic21ExpressionsArticulations()
        
    def setStartNode(self, node=None, nodeType=None, name=None, nodeId=None):
        '''Sets a starting :class:`Node` for the Query, and returns that node. 
        Searches begin from this point and branch out to find patterns that fit the query. 
        If we already have a Node object, that can be used as the `node` argument. 
        
        Without a `node` argument a Node is created implicitly, 
        optionally using the `nodeType` argument to specify its type. (Without a type, the
        node will match any type.) The list of available relationship types can be obtained
        from the :meth:`Database.printStructure` method.
        
        A name attribute will be randomly generated unless a `name` argument
        is given. There is no particular reason to name objects unless we want to control 
        the query text exactly, however.
        
        Each node in a Neo4j database has a numeric ID. If a Node with an ID
        number is passed in to the `node` argument, or if a numeric `nodeId`
        argument is given, that ID will be used to target the search.
        
        >>> db = Database()
        >>> q = Query(db)
        >>> print q.setStartNode(nodeType='Metadata', name='Metadata1')
        Metadata1
        
        Calling this method again, or calling :meth:`setStartRelationship`, will
        reset the start point for the Query.
        '''
        self.pattern = None
        if node == None:
            node = Node(self, nodeType=nodeType, name=name)
        if node.id:
            self.start = 'start %s=node(%d)\n' % (node.name, node.id)
        else:
            self.start = 'start %s=node:node_auto_index("type:%s")\n' % (node.name, node.nodeType)
        self.order = 'order by ID(%s)\n' % node.name
        self.startId = 'ID(%s)' % node.name 
        return node

    def results(self, minRow=0, limit=20, pattern=None):
        '''
        Executes a query of the database using the current state of the Query object.
        Returns a tuple containing first the results, then the query metadata. 

        The results of the query is a list of lists.
        If the :meth:`addReturns` method was used to specify particular 
        properties, then each result is a list of those properties as text strings.
        
        If no return properties have been specified by the :meth:`addReturns`
        method, each result will be a list of :class:`py2neo.neo4j.Node` and
        :class:`py2neo.neo4j.Relationship` objects. In this case, a result list
        can be passed to :meth:`music21Score` to get a music21
        :class:`~music21.stream.Score` fragment containing the result. Or
        passing a result list to the :meth:`getResultProperties` method will add
        all the properties of the objects to the list.
        
        The metadata for the query is a list of the column names.

        >>> db = Database()
        >>> q = Query(db)
        >>> q.setStartNode(nodeType='Score', name='Score1')
        Score1
        >>> print q.results()
        ([[Node('http://localhost:7474/db/data/node/...')]], [u'Score1'])
        '''
        #import py2neo.cypher as cypher

        if not pattern:
            pattern = self._assemblePattern()
        params = { 'minRow': minRow, 'maxResults': limit }
        results, columns = _cypherQuery(self.db.graph_db, pattern, params)
        #_fix535(results, metadata) # Remove when issue 535 is fixed (introduced around Neo4j 1.8.1)
        return results, columns

    def getResultProperties(self, result):
        '''Takes a list of :class:`py2neo.neo4j.Node` and
        :class:`py2neo.neo4j.Relationship` objects (the default result format
        if the :meth:`addReturns` method wasn't called), and returns a new list
        in which each object has been replaced by a tuple: the original object, 
        then a dict of that object's database properties. 

        >>> db = Database()
        >>> q = Query(db)
        >>> score = q.setStartNode(nodeType='Score', name='Score1')
        >>> rows, meta = q.results()
        >>> nodeInfo = q.getResultProperties(rows[0])[0]
        >>> print sorted(nodeInfo[1].items())
        [(u'_atSoundingPitch', u'unknown'), (u'_priority', 0), (u'corpusFilepath', u'bach/bwv84.5.mxl'), (u'hideObjectOnPrint', False), (u'offset', 0.0), (u'type', u'Score')]
        '''
        props = _serverCall(self.db.graph_db.get_properties, *result)
        return zip(result, props)

    def setStartRelationship(self, relation=None, relationType=None, start=None, end=None, name=None):
        '''Sets a starting :class:`Relationship` for the Query, and returns that relationship. 
        Searches begin from this point and branch out to find patterns that fit the query.
        
        Without a `relation` argument a Relationship is created implicitly, 
        optionally using the `relationType` argument to specify its type. (Without a type, the
        relationship will match any type.) The list of available relationship types can be
        obtained from the :meth:`Database.printStructure` method.
        
        The `start` and `end` arguments take :class:`Node` objects, and allow the relationship 
        to be connected to other nodes in the query. If either is omitted, a new node object
        is created implicitly to fill it. By convention a relationship is understood to be 
        a right-directed arrow::
        
            Start--Relationship-->End

        A name attribute will be randomly generated unless 
        a `name` argument is given. There is no particular reason to name objects or create them
        explicitly, unless we want to control the query text exactly.

        >>> db = Database()
        >>> q = Query(db)
        >>> score = Node(q, nodeType='Score', name='Score1')
        >>> part = Node(q, 'Part', name='Part1')
        >>> pIS = q.setStartRelationship(relationType='PartInScore', name='PIS1', start=part, end=score)
        >>> print pIS
        (Part1)-[PIS1:PartInScore]->(Score1)
        
        Calling this method again, or calling :meth:`setStartNode`, will
        reset the start point for the Query.
        '''
        self.pattern = None
        if relation == None:
            relation = Relationship(self, relationType, start, end, name)
        if relation not in self.match and relationType is not None:
            self.addRelationship(relation)
        self.start = ('start %s=relationship:relationship_auto_index("type:%s")\n' 
                      % (relation.name, relation.relationType))
        self.order = 'order by ID(%s)\n' % relation.name
        self.startId = 'ID(%s)' % relation.name 
        return relation
    
    def addRelationship(self, relation=None, relationType=None, start=None, end=None, name=None, optional=False):
        '''Adds a relationship to the query, and returns the corresponding
        :class:`Relationship` object. Any number of relationships can be added to a query.

        Without a Relationship object passed to the `relation` argument, a
        Relationship is created implicitly, optionally using the `relationType`
        argument to specify its type. (Without a type, the relationship will
        match any type.) The list of available relationship types can be
        obtained from the :meth:`Database.printStructure` method.

        The `start` and `end` arguments take :class:`Node` objects, and allow the relationship 
        to be connected to other nodes in the query. If either is omitted, a new node object
        is created implicitly to fill it. By convention a relationship is understood to be 
        a right-directed arrow::
        
            Start--Relationship-->End

        A name attribute will be randomly generated unless 
        a `name` argument is given. There is no particular reason to name objects or create them
        explicitly, unless we want to control the query text exactly.

        Note that in the following example `pIS.start` is synonymous with `part`.

        >>> db = Database()
        >>> q = Query(db)
        >>> part = Node(q, 'Part', name='Part1')
        >>> pIS = q.setStartRelationship(relationType='PartInScore', name='PIS1', start=part)
        >>> measure = Node(q, 'Measure', name='Measure1')
        >>> mIP = q.addRelationship(relationType='MeasureInPart', name='MIP1', start=measure, end=pIS.start)
        >>> print mIP
        (Measure1)-[MIP1:MeasureInPart]->(Part1)
        
        Setting the `optional` argument to True will cause the query pattern to match even
        if this relationship doesn't exist in a particular instance.
        '''
        self.pattern = None
        if relation == None:
            relation = Relationship(self, relationType, start=start, end=end, name=name, optional=optional)
        self.match.append(relation)
        for node in (relation.start, relation.end):
            if node.nodeType == '*': continue
            self.nodes.add(node)
        return relation
    
    def addNode(self, nodeType=None, name=None, nodeId=None):
        self.pattern = None
        return Node(self, nodeType, name, nodeId)
    
    def addComparisonFilter(self, pre, operator, post):
        '''Adds a condition to the query that must be true in order for the query to match, and returns
        the corresponding :class:`Filter` object.
        
        The `pre` and `post` arguments should be either:
        
        * an attribute of a :class:`Node` or :class:`Relationship` (which will return a
          :class:`Property` object), or
        * some text or a number.
         
        The list of available properties can be obtained from the :meth:`Database.printStructure` 
        method. Of course, if any of those Nodes or Relationships aren't objects in the query,
        the query will fail to match. 
        
        The `operator` argument is a text string of the comparison we want to test
        between the two properties. The available comparison operators are
        `=, <>, <, >, <=,` and `>=`.

        >>> db = Database()
        >>> q = Query(db)
        >>> n1 = Node(q, 'Note', name='Note1')
        >>> print q.addComparisonFilter(n1.midi, '<', 43)
        Note1.midi < 43
                
        There is no need for us to build all these relationships between notes, measures, parts,
        and scores, unless we want to return information about them or add filters to them.
        In this case the Note object by itself would be enough 
        (using just the :meth:`setStartNode` method).
        '''
        self.pattern = None
        filt = Filter(self, pre, operator, post)
        if filt not in self.where:
            self.where.append(filt)
        return filt
        
    def addCypherFilter(self, text):
        '''
        **For advanced use. This method will be probably be deprecated in future versions.**
        
        Adds a condition written in 
        `Cypher <http://docs.neo4j.org/chunked/milestone/query-where.html>`_ to the query.
        This method is provided for complex queries that require filters more complicated
        than basic comparisons. 
        
        In referring to a entity being used in the query, the Cypher query text must use 
        the name of the entity, which can be obtained from its `name` attribute.
        
        >>> db = Database()
        >>> q = Query(db)
        >>> nSWN = q.setStartRelationship(relationType='NoteSimultaneousWithNote')
        >>> q.addCypherFilter('abs(%s.midi- %s.midi) %% 12 = 7' % (nSWN.start.name, nSWN.end.name))
        >>> results, meta = q.results(limit=100)
        >>> print len(results)
        70
        
        But we can create this filter more directly using the `simpleHarmonicInterval` property
        of `NoteSimultaneousWithNote` relationships:
        
        >>> f = q.addComparisonFilter(nSWN.simpleHarmonicInterval, '=', 7)        
        '''
        self.pattern = None
        self.where.append(text)

    def addReturns(self, *props):
        '''Adds a list of properties to be returned from the query. Each of these properties
        should be a :class:`Property` object returned by accessing an attribute of a
        :class:`Node` or :class:`Relationship` object. The list of available attributes can be
        obtained using the :meth:`Database.printStructure` method.
        
        If this method is set, the requested properties will be returned as text, numbers, 
        and so forth. If it is not set, the query will return a list of 
        :class:`py2neo.neo4j.Node` and :class:`py2neo.neo4j.Relationship` objects.
        
        This method can be called multiple times, and the items from each call will be appended
        to the list of returns.
        
        >>> db = Database()
        >>> q = Query(db)
        >>> nSWN = q.setStartRelationship(relationType='NoteSimultaneousWithNote')
        >>> f = q.addComparisonFilter(nSWN.simpleHarmonicInterval, '=', 7)
        >>> q.addReturns(nSWN.start.midi, nSWN.end.midi)
        >>> r, meta = q.results()
        >>> sorted([sorted(x) for x in r])[0]
        [42, 61]
        '''
        self.pattern = None
        for p in props:
            self.returns.append(p)

    def music21Score(self, resultList, metadata=None):
        '''
        Returns a music21 :class:`~music21.stream.Score` object, given a single
        query result (which is by default a list of :class:`py2neo.neo4j.Node`
        and :class:`py2neo.neo4j.Relationship` objects). This method will only
        work if the :meth:`addReturns` method has not been called.
        
        The Score will contain its Metadata, all the measures and parts containing the query results,
        and all the objects contained in those measures, but no more than that.  

        If the metadata from the query is included, the score will include
        information useful for matching query nodes with score objects. Objects
        in the score that correspond to nodes in the query will have a
        "queryName" attribute that contains the query column name.
        '''
        self.nodeLookup = {}
        result = [r for r in resultList if isinstance(r, py2neo.neo4j.Node) and _getPy2neoMetadata(r)['type'] == 'Note']
        # add hierarchical nodes and result properties if that hasn't happened already
        self._addHierarchicalNodes(result, metadata, True)
        result = self.getResultProperties(result)
        nodes = {}
        relations = []
        for itemTuple in result:
            if isinstance(itemTuple[0], py2neo.neo4j.Node):
                if _id(itemTuple[0]) not in nodes:
                    nodes[_id(itemTuple[0])] = itemTuple
            else:
                relations.append(itemTuple)
        score = music21.stream.Score()
        scoreNodeId = [x for x in nodes if nodes[x][1]['type'] == 'Score'][0]
        self._addMusic21Properties(score, nodes[scoreNodeId][1])
        measures = [x for x in nodes.values() if x[1]['type'] == 'Measure']
        self.scoreOffset = sorted([float(x[1]['offset']) for x in measures])[0]
        self._addHierarchicalMusic21Data(score, scoreNodeId, nodes, relations)
        return score

    def setObjectCallback(self, entity, callback):
        '''**For advanced use.**

        All node types have default handling when being exported to music21 objects.
        When default handling isn't enough, one or more callbacks can be added
        to preprocess the object, or prevent it from being added to the Score.
        The callback is responsible for adding the object to the score.
        The `entity` argument is the class name the callback should be added to 
        (as a string), and the `callback` argument is a function object to be called.
        Only one callback is allowed per class name, so each call to this method
        will replace the previous value set by the method for that entity.
        Built-in callbacks are set in the internal :meth:`_defaultCallbacks` method.
        '''
        self._constructCallbacks[entity] = callback
    
    def _defaultCallbacks(self):
        
        # TieInNote
        def setTie(self, tieDict, tie, note, r):
            note.tie = tie
            return tie
        self.setObjectCallback('TieInNote', setTie)
        
        # BeamInNote
        def addBeam(self, beamDict, beam, note, r):
            number = _convertFromString(beamDict.get('number', 'None'))
            direction = _convertFromString(beamDict.get('direction', 'None'))
            beamType = beamDict['m21_type']
            if number != None:
                note.beams.setByNumber(number, beamType, direction)
            else:
                note.beams.append(beamType, direction)
            return None
        self.setObjectCallback('BeamInNote', addBeam)
        
        # ContributorInMetadata
        def addContributorText(self, contributorDict, contributor, metadataObj, r):
            name = metadataObj.Text(contributorDict.pop('_names'))
            contributor._names.append(name)
            metadataObj.addContributor(contributor)
            return contributor
        self.setObjectCallback('ContributorInMetadata', addContributorText)
        
        # PartInScore
        def removePartNumber(self, partDict, part, score, r):
            partDict.pop('number', None)
            score.insert(part)
            return part
        self.setObjectCallback('PartInScore', removePartNumber)
        
        # MeasureInPart
        def addSignaturesAndClefs(self, measureDict, measure, part, r):
            firstMeasure = (_convertFromString(measureDict['offset']) == self.scoreOffset)
            if 'clef' in measureDict and (measureDict['clefIsNew'] == 'True' or firstMeasure):
                classLookup = self._listMusic21Classes()
                clef = classLookup[measureDict['clef']]()
                measure.insert(clef)
            if 'keySignatureSharps' in measureDict and (measureDict['keyIsNew'] == 'True' or firstMeasure):
                sharps = _convertFromString(measureDict['keySignatureSharps'])
                keySig = music21.key.KeySignature(sharps)
                if 'keySignatureMode' in measureDict:
                    keySig.mode = _convertFromString(measureDict['keySignatureMode'])
                measure.insert(keySig)
            if 'timeSignature' in measureDict and (measureDict['timeSignatureIsNew'] == 'True' or firstMeasure):
                timeSig = meter.TimeSignature(measureDict['timeSignature'])
                measure.insert(timeSig)
            for key in ('clef', 'keySignatureSharps', 'keySignatureMode', 'timeSignature'):
                if key in measureDict:
                    del measureDict[key]
            part.insert(measure)
            return measure
        self.setObjectCallback('MeasureInPart', addSignaturesAndClefs)
        
        # MidmeasureClefInMeasure
        def addMidmeasureClef(self, clefDict, clef, measure, r):
            classLookup = self._listMusic21Classes()
            clef = classLookup[clefDict['name']]()
            measure.insert(clef)
            return clef
        self.setObjectCallback('MidmeasureClefInMeasure', addMidmeasureClef)
        
        # NoteInMeasure
        def setPitchAndDuration(self, noteDict, note, measure, r):
            # .nameWithOctave is not read/write in music21 1.0.
            import re
            if ('pitch' in noteDict):
                name = re.sub('[0-9]*', '', noteDict['pitch'])
                note.midi = int(noteDict['midi'])
                note.name = name
                del noteDict['pitch']
                del noteDict['midi']
            if ('tuplet' in noteDict):
                tuplets = noteDict['tuplet'].split('*')
                for tupletString in tuplets:
                    actual, normal = tupletString.split(':')
                    tuplet = duration.Tuplet(actual, normal)
                    note.appendTuplet(tuplet)
                del noteDict['tuplet']
            if ('isGrace' in noteDict):
                note.duration.isGrace = _convertFromString(noteDict['isGrace'])
                if note.duration.isGrace:
                    for attr in ('stealTimePrevious', 'stealTimeFollowing', 'slash'):
                        setattr(note.duration, attr, noteDict[attr])
                del noteDict['isGrace']
            measure.insert(note)
            return note
        self.setObjectCallback('NoteInMeasure', setPitchAndDuration)
            
        # ArticulationInNote, ExpressionInNote
        def replaceWithSpecificClass(self, classDict, classObj, noteObj, r):
            classLookup = self._listMusic21Classes()
            childType = classDict['name']
            obj = classLookup[childType]()
            classAttribute = obj.__module__[8:] 
            getattr(noteObj, classAttribute).append(obj)
            return obj
        self.setObjectCallback('ArticulationInNote', replaceWithSpecificClass)
        self.setObjectCallback('ExpressionInNote', replaceWithSpecificClass)
        
        # PartInStaffGroup
        def addPartsToStaffGroup(self, partDict, part, staffGroup, r):
            childId = _id(r[0].start_node)            
            part = self.nodeLookup.get(childId, None)
            if part:
                staffGroup.addSpannedElements(part)
            return None
        self.setObjectCallback('PartInStaffGroup', addPartsToStaffGroup)
        
        # spannerTo
        def replaceWithSpecificSpanner(self, noteDict, note, otherNote, r):
            classLookup = self._listMusic21Classes()
            spanDict = r[1]
            spanType = spanDict.pop('name')
            span = classLookup[spanType]()
            start = self.nodeLookup[_id(r[0].start_node)]
            end = self.nodeLookup[_id(r[0].end_node)]
            span.addComponents(start, end)
            self._addProperties(span, spanDict)
            measure = start.getContextByClass('Measure')
            measure.append(span)
            return None
        self.setObjectCallback('spannerTo', replaceWithSpecificSpanner)
        
        # Default
        def defaultChild(self, childDict, child, parent, r):
            if hasattr(child, 'offset') and hasattr(parent, 'insert'):
                parent.insert(child)
            elif hasattr(parent, 'append'):
                parent.append(child)
            else:
                sys.stderr.write('%s has no append function for %s objects! Please set a callback function.' 
                                 % (parent, child))
                sys.exit(1)
            return child
        self.setObjectCallback('default', defaultChild)

    def _listMusic21Classes(self):
        if hasattr(self, 'classLookup'):
            return self.classLookup
        import pkgutil
        import inspect
        self.classLookup = {}
        for importer, modname, ispkg in pkgutil.iter_modules(music21.__path__):
            if ispkg: continue
            mod = sys.modules['music21.' + modname]
            for c in inspect.getmembers(mod, inspect.isclass):
                self.classLookup[c[0]] = getattr(mod, c[0])
        return self.classLookup

    def _inspectMusic21ExpressionsArticulations(self):
        import inspect
        self.m21_classes = {}
        # inspect music21 modules:
        for module in (expressions, articulations):
            mName = module.__name__
            self.m21_classes[mName] = {}
            classes = inspect.getmembers(module, inspect.isclass)
            for cName, ref in classes:
                self.m21_classes[mName][cName] = ref

    def _assemblePattern(self, distinct=False):
#        for node in self.nodes:
#            self.addComparisonFilter(node.type, '=', node.nodeType)
        if self.pattern:
            return self.pattern
        startStr = self.start
        if startStr == None:
            sys.stderr.write('setStartNode() or setStartRelationship() must be called first.\n')
            sys.exit(1)
        matchStr = whereStr = returnStr = ''
        if self.match:
            matchStr = 'match\n' + ',\n'.join([str(x) for x in self.match]) + '\n'
        if self.where:
            whereStr = 'where\n' + '\nand '.join([str(x) for x in self.where]) + '\n'
        props = []
        if self.returns:
            props = self.returns[:]
#            returnStr = 'return ' + ', '.join([str(x) for x in self.returns]) + '\n'
#       else:
        if self.limitReturnToWhere == False:
            props.append('*')
#            [props.extend([x.pre, x.post]) for x in self.where]
#            props = [x for x in props if isinstance(x, music21.musicNet.Property) and x.name != 'ID']
        props = [str(x) for x in set(props)]
        distinctStr = ''
        #if (distinct):
        #    distinctStr = 'distinct '
        returnStr = 'return ' + distinctStr + ', '.join(props) + '\n'
        limitStr = 'limit {maxResults}\n'
        skipStr = 'skip {minRow}\n'
        self.pattern = startStr + matchStr + whereStr + returnStr + self.order + skipStr + limitStr + ';'
        return self.pattern

    def _addHierarchicalNodes(self, results, metadata, buildFullScore):
        ''' Fill in a minimal score hierarchy sufficient to contain the notes in the result.
        (Assumes only Note objects are passed in the results array.)
        Then fill in all the other notes in the minimal score.
        Then add one more layer of nodes within the objects in the score 
        (articulations, dynamics, expressions, etc.).
        '''
        nodes = {}
        relations = {}
        self._filterNodesAndRelationships(results, nodes, relations)
        
        # Get the minimal set of measures containing these Notes
        q = Query(self.db)
        n = Node(q, 'Note')
        inMeasure = q.addRelationship(relationType='NoteInMeasure', start=n)
        inPart = q.addRelationship(relationType='MeasureInPart', start=inMeasure.end)
        inScore = q.addRelationship(relationType='PartInScore', start=inPart.end)
        for i in range(len(results)):
            node = results[i]
            node.queryName = metadata[i]
            n.id = _id(node)
            q.setStartNode(n)
            subresults, meta = q.results()
            self._filterNodesAndRelationships(subresults[0], nodes, relations)
        measures = [x for x in nodes.values() if _getPy2neoMetadata(x)['type'] == 'Measure']
        measureNumbers = [_getPy2neoMetadata(x)['number'] for x in measures]
        measureRange = range(min(measureNumbers), max(measureNumbers) + 1)
        
        # Fill in measures in other parts and other structural nodes
        score = [x for x in nodes.values() if _getPy2neoMetadata(x)['type'] == 'Score'][0]
        parts = [x for x in nodes.values() if _getPy2neoMetadata(x)['type'] == 'Part']
        q = Query(self.db)
        s = Node(q, nodeId=_id(score))
        q.setStartNode(s)
        q.addRelationship(relationType='MetadataInScore', end=s, optional=True)
        q.addRelationship(relationType='StaffGroupInScore', end=s, optional=True)
        for part in parts:
            pRelate = q.addRelationship(relationType='PartInScore', end=s)
            p = pRelate.start
            q.addComparisonFilter(p.ID, '=', _id(part))
            q.addRelationship(relationType='InstrumentInPart', end=p, optional=True)
            for number in measureRange:
                mRelate = q.addRelationship(relationType='MeasureInPart', end=p)
                m = mRelate.start
                q.addComparisonFilter(m.number, '=', number)
        subresults, meta = q.results()
        self._filterNodesAndRelationships(subresults[0], nodes, relations)
        
        if (buildFullScore):
            # Fill in the other notes in the measures.
            measures = [x for x in nodes.values() if _getPy2neoMetadata(x)['type'] == 'Measure']
            for m in measures:
                self._addChildren(m, 'NoteInMeasure', nodes, relations)
                self._addChildren(m, 'RestInMeasure', nodes, relations)
    
            # Add one more layer of objects below the existing ones.
            rTypes = self.db.listRelationshipTypes()
            for node in nodes.values():
                nType = _getPy2neoMetadata(node)['type']
                inNodeRTypes = [x for x in rTypes if x['end'] == nType]
                for r in inNodeRTypes:
                    rType = r['type']
                    if not (rType.endswith('In' + nType) or rType == 'spannerTo'):
                        continue
                    if rType in ('NoteInMeasure', 'MomentInScore', 'PartInScore', 'MeasureInPart'):
                        continue
                    self._addChildren(node, rType, nodes, relations)

        results[:] = nodes.values() + relations.values()

    def _filterNodesAndRelationships(self, results, nodes, relations):
        ''' Nodes and Relations must be hashed separately to avoid ID number clashes.
        '''
        for item in results:
            if isinstance(item, py2neo.neo4j.Node):
                if _id(item) not in nodes:
                    nodes[_id(item)] = item
            elif isinstance(item, py2neo.neo4j.Relationship):
                relations[_id(item)] = item
    
    def _addChildren(self, node, rType, nodes, relations):
        ''' Add all the children of this node that are connected by the specified Relationship type.
        '''
        q = Query(self.db)
        n = Node(q, nodeId=_id(node))
        q.setStartNode(n)
        q.addRelationship(relationType=rType, end=n)
        subresults, meta = q.results(limit=500)
        for result in subresults:
            self._filterNodesAndRelationships(result, nodes, relations)
                    
    def _addHierarchicalMusic21Data(self, parent, parentId, nodes, relates):
        # Some bits of the music21-to-MusicXML conversion process are sensitive to order.
        relatesToNode = sorted([x for x in relates if _id(x[0].end_node) == parentId],
                               key = lambda r: _id(r[0]))
        for r in relatesToNode:
            rType = r[0]['type']
            parentType = parent.__class__.__name__
            if not (rType.endswith('In' + parentType) or rType in ('spannerTo')):
                continue
            childId = _id(r[0].start_node)
            childDict = nodes[childId][1]
            child = self._addMusic21Child(childDict, parent, r)
            if child == None:
                continue
            queryName = getattr(nodes[childId][0], 'queryName', None)
            if queryName:
                child.editorial.color = "red"
                child.queryName = queryName
            self.nodeLookup[childId] = child
            self._addHierarchicalMusic21Data(child, childId, nodes, relates)

    def _addMusic21Child(self, childDict, parent, r):
        classLookup = self._listMusic21Classes()
        childType = childDict['type']
        if childType in classLookup:
            child = classLookup[childType]()
        else:
            child = None
        rType = r[0]['type']
        if rType not in self._constructCallbacks:
            rType = 'default'
        child = self._constructCallbacks[rType](self, childDict, child, parent, r)
        if child == None:
            return None
        self._addMusic21Properties(child, childDict)
        return child

    def _addMusic21Properties(self, obj, objDict):
        for key, val in objDict.iteritems():
            if key == 'type':
                continue
            if key.startswith('m21_'):
                key = key[4:]
            val = _convertFromString(val)
            if hasattr(obj, key):
                setattr(obj, key, val)
            else:
                obj.__dict__[key] = val

#-------------------------------------------------------------------------------
class Entity(object):
    '''The generic class of objects that are used to construct queries.
    All entities require a :class:`Query` object as their first argument
    to establish the namespace which they belong to. 
    '''

    _DOC_ATTR = {
    'name': 'The text string that names the entity in the query and in the results.',             
    }
    
    def __init__(self, query):
        self.name = None
        self.query = query
    
    def __getattr__(self, name):
        return Property(self.query, self, name)
    
    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __key(self):
        return tuple(self.__dict__.values())
    
    def __hash__(self):
        return hash(self.__key())

    def _addName(self, name):
        while not name:
            entityType = self._type
            if entityType == '*':
                entityType = 'Generic'
            testName = '%s%04d' % (entityType, random.randint(0, 9999))
            if testName not in self.query._usedNames:
                name = testName
        if name in self.query._usedNames:
            raise ValueError('The name "%s" is already being used.' % name)
        self.name = name
        self.query._usedNames.append(name)

class Node(Entity):
    '''An object that represents a database node in a query.
    
    If the Node is created without a nodeType, the node will match any type.
    The list of available node types is can be obtained
    from the :meth:`Database.printStructure` method.
    
    A name attribute will be randomly generated unless a `name` argument
    is given. There is no particular reason to name objects unless we want to control 
    the query text exactly.

    Each node in a Neo4j database has a numeric ID, and passing that ID to the `nodeId` argument
    will save it in the Node's `id` attribute.
    If a Node with an ID number is passed to the :meth:`Query.setStartNode` method,
    that ID will be used to target the search. Otherwise the `id` attribute is ignored.
    
    Any properties of a node type can be accessed as an attribute of the Node. Accessing a
    Node's attribute will return a :class:`Property` object for that node property, which can be
    used with the :meth:`Query.addComparisonFilter` and :meth:`Query.addReturns` methods.
    '''
    
    _DOC_ATTR = {
    'nodeType': 'The type of database node this object represents.',
    'id': 'The numeric ID of this node in the database (default=None).'
    }

    def __init__(self, query, nodeType=None, name=None, nodeId=None):
        Entity.__init__(self, query)
        if not nodeType:
            nodeType = '*'
        self._type = self.nodeType = nodeType
        self.id = nodeId
        self._addName(name)

    def __repr__(self):
        return self.name
        
class Relationship(Entity):
    '''An object that represents a database relationship in a query.
    
    If the Relationship is created without a relationType, the relationship will match any type.
    The list of available relationship types is available
    from the :meth:`Database.printStructure` method.
    
    The `start` and `end` arguments take :class:`Node` objects, and allow the relationship 
    to be connected to other nodes in the query. If either is omitted, a new node object
    is created implicitly to fill it.

    A name attribute will be randomly generated unless a `name` argument
    is given. There is no particular reason to name objects unless we want to control 
    the query text exactly.

    Any properties of a relationship type can be accessed as an attribute of the
    Relationship. Accessing a Relationship's attribute will return a
    :class:`Property` object for that relationship property, which can be used
    with the :meth:`Query.addComparisonFilter` and :meth:`Query.addReturns`
    methods.
    
    Setting the `optional` argument to True will cause the query pattern to match even
    if this relationship doesn't exist in a particular instance.
    '''

    _DOC_ATTR = {
    'relationType': 'The type of database relationship this object represents.',
    'start': 'The start :class:`Node` of this relationship.',
    'end': 'The end :class:`Node` of this relationship',
    'optional': "Whether the query pattern will match even if this relationship doesn't exist in a particular instance"
    }

    def __init__(self, query, relationType=None, start=None, end=None, name=None, optional=False):
        Entity.__init__(self, query)
        if not relationType:
            relationType = '*'
        self._type = self.relationType = relationType
        self.start = start or Node(query)
        self.end = end or Node(query)
        self.optional = ''
        if optional:
            self.optional = '?'
        self._addName(name)

    def __repr__(self):
        return '(%s)-[%s%s:%s]->(%s)' % (self.start, self.name, self.optional, self.relationType, self.end)

class Property(Entity):
    '''An object representing a property of a node or relationship in the database.
    
    Accessing an attribute of a :class:`Node` or :class:`Relationship` will return a
    :class:`Property` object, which can be used
    with the :meth:`Query.addComparisonFilter` and :meth:`Query.addReturns`
    methods.
    '''
    
    def __init__(self, query, parent, name):
        Entity.__init__(self, query)
        self.parent = parent
        self.name = name
        
    def __repr__(self):
        if (self.name == 'ID'):
            return 'ID(%s)' % self.parent.name
        return '%s.%s' % (self.parent.name, self.name)
    
    def __getattr__(self):
        raise AttributeError
        
class Filter(Entity):
    '''An object representing a filter phrase in a query, which is a condition 
    that must be true for the query to match.
    
    The `pre` and `post` arguments are the values that are being tested, and should each be either:
    
    * an attribute of a :class:`Node` or :class:`Relationship` (which will return a
      :class:`Property` object), or
    * some text or a number.
     
    The list of available node and relationship properties can be obtained 
    from the :meth:`Database.printStructure` method.
    
    The `operator` argument is a text string of the comparison we want to test
    between the two properties. The available comparison operators are
    `=, <>, <, >, <=,` and `>=`.
    '''
    
    def __init__(self, query, pre, operator, post):
        Entity.__init__(self, query)
        self.pre = pre
        self.operator = operator
        self.post = post
        
    def __repr__(self):
        operands = []
        for operand in (self.pre, self.post):
            if isinstance(operand, (unicode, str, bool)):
                text = '"%s"' % operand
                operands.append(text)
            else:
                operands.append(str(operand))
        return '%s %s %s' % (operands[0], self.operator, operands[1])
    
    def __getattr__(self):
        raise AttributeError

class Moment(base.Music21Object):
    '''This object is similar in purpose to a
    :class:`~music21.voiceLeading.VerticalSlice` in that it contains every
    :class:`~music21.note.Note` that occurs at a given offset in a
    :class:`~music21.stream.Score`. (Notes are stored as references
    to the original objects in :class:`~weakref.WeakSet` objects.) A Moment acts like a
    :class:`~music21.spanner.Spanner` placed at the end of a Score.
    
    A Moment can serve the same function as a VerticalSlice or a call 
    to :meth:`~music21.stream.Stream.getElementsByOffset` on a flattened Score,
    providing easy access to all the Notes active at a particular offset.
    It is also a place to store information about that moment in the score, such as
    chord quality or pitch-class set.
    
    When a Score with Moments is added to a :class:`Database` object, it will
    add vertical relationships between Notes (`NoteSimultaneousWithNote`). It
    will also add Moment nodes to the database, along with their corresponding
    Note relationships (`MomentInNote`).
    
    Typically Moments are added to a score via the :meth:`musicNet.addMomentsToScore` 
    class method. 
    '''
    
    _DOC_ORDER = ['getComponents', 'addComponents']
    
    _DOC_ATTR = {
    'sameOffset': 'A :class:`weakref.WeakSet` referring to all the Notes starting at the Moment.',
    'simultaneous': 'A :class:`weakref.WeakSet` referring to any Notes that started before the Moment but hold over into it.'
    }
        
    def __init__(self, components=None, sameOffset=None, *arguments):
        base.Music21Object.__init__(self)
        self.sameOffset = weakref.WeakSet()
        self.simultaneous = weakref.WeakSet()
        if components:
            self.addComponents(components, sameOffset, *arguments)
        
    def getComponents(self):
        '''Returns the contents of the object as a :class:`weakref.WeakSet`. This is simply the
        union of two of the object's attributes: `sameOffset` and `simultaneous`.
        '''
        return self.sameOffset | self.simultaneous
    
    def addComponents(self, components, sameOffset=None, *arguments):
        '''Adds a :class:`~music21.note.Note` object (or a list of Notes) to the
        object. If a Note has the same offset as this object, a reference to it
        is added to `sameOffset`. Otherwise a reference is added to `simultaneous`.
        '''
        if not common.isListLike(components):
            components = [components]
        components += arguments
        for c in components:
            if not isinstance(c, note.Note):
                raise ValueError('cannot add a non-Note object to a Moment')
            if sameOffset == True:
                self.sameOffset.add(c)
            elif sameOffset == False:
                self.sameOffset.add(c)
            else:
                offset = c.getContextByClass('Measure').offset + c.offset
                if offset == self.offset:
                    self.sameOffset.add(c)
                else:
                    self.simultaneous.add(c)        

class Test(unittest.TestCase):

    def runTest(self):
        pass
    
class TestExternal(unittest.TestCase):
    def runTest(self):
        pass

_DOC_ORDER = [Query, Database, Entity, Node, Relationship, Property, Filter, Moment]

# This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. 
# If a copy of the MPL was not distributed with this file, You can obtain one at 
# http://mozilla.org/MPL/2.0/.
