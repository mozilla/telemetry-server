import json

def map(k, d, v, cx):
    try:
        j = json.loads(v)
        if "version" in j:
            cx.write(j["version"], 1)
        else:
            cx.write("MISSING VERSION", 1)
    except Exception as e:
        cx.write("ERROR", 1)

def reduce(k, v, cx):
    cx.write(k, sum(v))
