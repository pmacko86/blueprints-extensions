#!/usr/bin/python

#
# A tool to combine sampled data from one or more files
#

import os.path
import sys
import random


def helper(lines, smin=None, smax=None, prefix="", sep=" "):
	a = lines[0]
	if smin is not None and smax is not None:
		a = random.sample(a, random.randint(smin, smax))
	if len(lines) > 1:
		for l in a:
			p = prefix
			if p != "":
				p += " "
			helper(lines[1:], smin, smax, p + l.strip(), sep)
	else:
		for l in a:
			p = prefix
			if p != "":
				p += " "
			print p + l.strip()


if len(sys.argv) <= 1:
	sys.stderr.write("Usage: " + os.path.basename(sys.argv[0]) + " FILES...\n")
	sys.exit(1)

lines = []
for f in sys.argv[1:]:
	lines.append(open(f).readlines())

helper(lines, 10, 1000)

