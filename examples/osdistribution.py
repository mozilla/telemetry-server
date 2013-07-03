# Same as the osdistribution.py example in jydoop
import json

def map(k, d, v, cx):
    j = json.loads(v)
    os = j['info']['OS']
    cx.write(os, 1)

def reduce(k, v, cx):
    cx.write(k, sum(v))
