import sys, os, csv
from collections import defaultdict
import gzip
import simplejson as json

infile, outpattern = sys.argv[1:]

class Experiment(object):
    def __init__(self):
        self.activeBranches = defaultdict(lambda: 0)
        self.activations = defaultdict(lambda: 0)
        self.terminations = defaultdict(lambda: 0)

class Channel(object):
    def __init__(self):
        self.total = 0
        self.experiments = defaultdict(Experiment)

    def addTotal(self, c):
        self.total += c

    def addActive(self, id, branch, c):
        self.experiments[id].activeBranches[branch] += c

    def addActivation(self, id, data, c):
        self.experiments[id].activations[tuple(data)] += c

    def addTermination(self, id, data, c):
        self.experiments[id].terminations[tuple(data)] += c

channels = defaultdict(lambda: Channel())

errors = []

lines = csv.reader(open(infile))
for line in lines:
    entrytype = line[0]
    if entrytype == "Error":
        errors.append(line[1])
        continue

    if entrytype == "Totals":
        channel, version, count = line[1:]
        count = int(count)
        channels[channel].addTotal(count)
    elif entrytype == "EXPERIMENT_ACTIVATION":
        channel, version, reason, id = line[1:5]
        data = line[5:-1]
        count = int(line[-1])
        channels[channel].addActivation(id, [reason] + data, count)
    elif entrytype == "EXPERIMENT_TERMINATION":
        channel, version, reason, id = line[1:5]
        data = line[5:-1]
        count = int(line[-1])
        channels[channel].addTermination(id, [reason] + data, count)
    elif entrytype == "ACTIVE":
        channel, version, id, branch, count = line[1:]
        count = int(count)
        channels[channel].addActive(id, branch, count)
    else:
        raise ValueError("Unexpected data key, line %i: %s" % (lines.line_num, entrytype))

if len(errors):
    errorfd = gzip.open("%s-errors.txt.gz" % (outpattern,), "wb")
    for e in errors:
        print >>errorfd, e
    errorfd.close()

channels = channels.items()
channels.sort(key=lambda i: i[1].total, reverse=True)

for cname, channel in channels:
    d = {
        "total": channel.total,
        "experiments": {},
    }
    for id, experiment in channel.experiments.items():
        d["experiments"][id] = {
            "active": experiment.activeBranches,
            "activations": experiment.activations.items(),
            "terminations": experiment.terminations.items(),
        }
    fd = gzip.open("%s-%s.json.gz" % (outpattern, cname), "wb")
    json.dump(d, fd)
    fd.close()
