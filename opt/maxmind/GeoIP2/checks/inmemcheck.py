#! /space/storage/site/opt/anaconda2/bin/python

# -*- coding: utf-8 -*-

import sys
import geoip2.database
from maxminddb import MODE_MEMORY, MODE_AUTO

class MaxmindReader(object):

    def __init__(self, dbFilename, locales = None):
        if locales is None:
            locales = ['de', 'en']
        self.locales = locales
        self.reader = geoip2.database.Reader(dbFilename, locales = self.locales, mode = MODE_MEMORY)

    # @LruCache(maxsize=2**19, timeout = None)
    def getCity(self, ipaddr):
        return self.reader.city(ip_address = ipaddr)

def main(argv):
    maxmindReader = MaxmindReader('/space/storage/site/data/maxmind/GeoIP2/GeoIP2-City.mmdb', locales = ['de', 'en'])
    print maxmindReader.reader._db_reader._buffer_size
    for i in xrange(2**16):
        response = maxmindReader.reader.city('128.101.101.101')
    print response

def timeit():
    import timeit
    print(timeit.timeit("main(None)", setup="from __main__ import main", number=1))

if __name__ == '__main__':
    timeit()
