import csv
import gzip
import os
import re
import sys
import numpy

APP_COLUMN=0
INTERVAL_COLUMN=1
FILE_COLUMN=2
SUBMISSION_COUNT_COLUMN=3
MEDIAN_TIME_COLUMN=4
MEDIAN_COUNT_COLUMN=5

input = sys.argv[1]
rows = None
totals = {}

def key(row):
    return str(row[APP_COLUMN]) + str(row[INTERVAL_COLUMN])

def parse():
    global rows

    with open(input) as f:
        lines = f.readlines()
        rows = map(lambda x: x.split(','), lines)

    for i, row in enumerate(rows[:]):
        if row[FILE_COLUMN] == "TOTAL":
            totals[key(row)] = row
            rows.remove(row)

def normalize():
    global rows

    for row in rows:
        k = key(row)
        row[SUBMISSION_COUNT_COLUMN] = float(row[SUBMISSION_COUNT_COLUMN]) / float(totals[k][SUBMISSION_COUNT_COLUMN])

    rows = sorted(rows, key=lambda x: x[SUBMISSION_COUNT_COLUMN], reverse=True)
    for row in rows:
        row[SUBMISSION_COUNT_COLUMN] = str(row[SUBMISSION_COUNT_COLUMN])

def dump():
    with open(input, "w") as f:
        for row in rows:
            f.write(",".join(row))

parse()
normalize()
dump()
