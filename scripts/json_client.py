#!/usr/bin/python

import sys
import time
import json
import urllib2

nodes = [ 
         { 'type': 'Note', 'name': 'n1' },
         { 'type': 'Note', 'name': 'n2' },
         { 'type': 'Note', 'name': 'p1' },
         { 'type': 'Note', 'name': 'p2' }
         ]
relationships = [
                 { 'start': 'n1', 'type': 'NoteSimultaneousWithNote', 'end': 'n2', 'name': 'nswn' },
                 { 'start': 'p1', 'type': 'NoteToNote', 'end': 'n1', 'name': 'ntn1' },
                 { 'start': 'p2', 'type': 'NoteToNote', 'end': 'n2', 'name': 'ntn2' }
                 ]
nProperties = [
               
               ]
rProperties = [
               { 'relationship': 'nswn', 'property': 'sameOffset', 'name': 'nswnSo' },
               { 'relationship': 'nswn', 'property': 'simpleHarmonicInterval', 'name': 'nswnShi' },
               { 'relationship': 'ntn1', 'property': 'interval', 'name': 'ntn1I' },
               { 'relationship': 'ntn2', 'property': 'interval', 'name': 'ntn2I' }
               ]
filters = [
           { 'preType': 'property', 'pre': 'nswnSo', 'operator': '=', 'postType': 'value', 'post': 'True' },
           { 'preType': 'property', 'pre': 'nswnShi', 'operator': '=', 'postType': 'value', 'post': 7 },          
           { 'preType': 'property', 'pre': 'ntn1I', 'operator': '<>', 'postType': 'value', 'post': 0 },
           { 'preType': 'property', 'pre': 'ntn2I', 'operator': '<>', 'postType': 'value', 'post': 0 }
           ]
returns = [ ]
makePreviews = True
query = { 'nodes': nodes,
          'startNode': 'n1',
          'relationships': relationships,
          'relationshipProperties': rProperties,
          'nodeProperties': nProperties,
          'comparisonFilters': filters,
          'returns': returns,
          'makePreviews': makePreviews
         }

host = 'http://76.19.115.205:8080'
jsonQuery = json.dumps(query)
req = urllib2.Request(host + '/submitquery', jsonQuery, {'Content-Type': 'application/json'})
try:
    response = json.loads(urllib2.urlopen(req).readline())
except urllib2.URLError:
    print 'Unable to contact musicNetBottle server.'
    sys.exit(1)
if 'error' in response:
    print response['error']
    sys.exit(1)
token = response['token']

response = urllib2.urlopen(host + '/getresults?token=%s&row=%d' % (token, 0))
response = urllib2.urlopen(host + '/getimages?token=%s&block=true' % token)

doneList = []
if not makePreviews:
    doneList.append('images')
row = 0
imagerow = 0
metadata = None
while 'data' not in doneList or (makePreviews and 'images' not in doneList):
    if 'data' not in doneList:
        response = urllib2.urlopen(host + '/getresults?token=%s&row=%d' % (token, row))
        lines = response.readlines()
        if (len(lines) == 0):
            doneList.append('data')
        for line in lines:
            data = json.loads(line)
            if data['type'] == 'metadata':
                if not metadata:
                    metadata = data
                    print metadata
                continue
            print data
            row += 1
    if 'images' not in doneList:
        response = urllib2.urlopen(host + '/getimages?token=%s' % token)
        for line in response:
            data = json.loads(line)
            if 'error' in data:
                print data['error']
                doneList.append('images')
            print data
            imagerow += 1
        if imagerow >= row:
            doneList.append('images')
        elif 'data' in doneList:
            time.sleep(0.25)
