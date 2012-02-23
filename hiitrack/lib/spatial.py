#!/usr/bin/env python
# -*- coding: utf-8 -*-

from itertools import cycle

def spatial_hash(coordinates, boundaries, precision=12):
    boundaries = [list(x) for x in boundaries]
    spatialhash = []
    bits = [128, 64, 32, 16, 8, 4, 2, 1]
    bit = 0
    character_code = 0
    dimension_cycle = cycle(range(0, len(coordinates)))
    binary_parts = [[], []]
    binary = []
    while len(spatialhash) < precision:
        dimension = dimension_cycle.next()
        mid = (boundaries[dimension][0] + boundaries[dimension][1]) / 2
        if coordinates[dimension] > mid:
            character_code |= bits[bit]
            boundaries[dimension][0] = mid
        else:
            boundaries[dimension][1] = mid
        if bit < 7:
            bit += 1
        else:
            spatialhash += chr(character_code)
            bit = 0
            character_code = 0
    return ''.join(spatialhash)

#def encode(latitude, longitude, precision=12):
#    """
#    Encode a position given in float arguments latitude, longitude to
#    a geohash which will have the character count precision.
#    """
#    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
#    geohash = []
#    bits = [ 16, 8, 4, 2, 1 ]
#    bit = 0
#    ch = 0
#    even = True
#    while len(geohash) < precision:
#        if even:
#            mid = (lon_interval[0] + lon_interval[1]) / 2
#            if longitude > mid:
#                ch |= bits[bit]
#                lon_interval = (mid, lon_interval[1])
#            else:
#                lon_interval = (lon_interval[0], mid)
#        else:
#            mid = (lat_interval[0] + lat_interval[1]) / 2
#            if latitude > mid:
#                ch |= bits[bit]
#                lat_interval = (mid, lat_interval[1])
#            else:
#                lat_interval = (lat_interval[0], mid)
#        even = not even
#        if bit < 4:
#            bit += 1
#        else:
#            geohash += __base32[ch]
#            bit = 0
#            ch = 0
#    return ''.join(geohash)#