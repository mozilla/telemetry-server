# Same as the osdistribution.py example in jydoop
import json

def map(k, v, cx):
    os = v['environment']['system']['os']['name']
    cx.write(os, 1)

def reduce(k, v, cx):
    cx.write(k, sum(v))
