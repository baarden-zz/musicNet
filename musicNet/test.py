'''
Created on Sep 13, 2012

@author: bretaarden
'''
import unittest
from musicNet import *
from music21 import *


def _prepDoctests():
    '''Execute this function before running doctests. 
    Results from the tests depend on the state of the database, and there is
    no way (!?!) to control the order of block testing. This function will
    purge the database and import a sample file.
    '''
    db = Database()
    db.wipeDatabase()
    bwv84_5 = corpus.parse('bach/bwv84.5.mxl')
    addMomentsToScore(bwv84_5)
    db.addScore(bwv84_5, index='bach/bwv84.5.mxl')


class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()