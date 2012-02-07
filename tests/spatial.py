#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from hiitrack.lib.spatial import spatial_hash
from random import randint
from base64 import b64encode

class SpatialTestCase(unittest.TestCase):
	def test_generation(self): 
		size = randint(0, 1000000)
		point_count = 1000
		dimensions = 2
		boundaries = []
		box = []
		points = []
		i = 0
		for dimension in range(0, dimensions):
			lower_limit = randint(0, size)
			upper_limit = randint(lower_limit, size)
			lower_box = randint(lower_limit, upper_limit)
			upper_box = randint(lower_box, upper_limit)
			boundaries.append((lower_limit, upper_limit))
			box.append((lower_box, upper_box))
		low_point = [x[0] for x in box]
		high_point = [x[1] for x in box]
		for i in range(0, point_count):
			point = []
			for dimension in range(0, dimensions):
				coordinate = randint(boundaries[dimension][0], boundaries[dimension][1])
				point.append(coordinate)
			points.append((spatial_hash(point, boundaries), point))
		low_point_hash = spatial_hash(low_point, boundaries)
		high_point_hash = spatial_hash(high_point, boundaries)
		box_misses = 0
		for sh, coordinates in points:
			in_box = []
			for dimension in range(0, dimensions):
				in_box.append(coordinates[dimension] > low_point[dimension] and 
					coordinates[dimension] < high_point[dimension])
			if all(in_box) != (sh > low_point_hash and sh < high_point_hash):
				box_misses += 1
		print float(box_misses) / point_count