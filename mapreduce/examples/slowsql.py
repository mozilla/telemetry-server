# SlowSQL export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox
import re
import simplejson as json
import traceback

string_literal_pattern = re.compile(r'([\'\"])(.+?)\1');
def replace_quoted_values(sql):
    # Replace single- or double-quoted literals:
    return string_literal_pattern.sub(":private", sql)

def sanitize(json_string):
    parsed = json.loads(json_string)
    if "slowSQL" in parsed:
        if "otherThreads" in parsed["slowSQL"]:
            sanitized = {}
            for sql, arr in parsed["slowSQL"]["otherThreads"].iteritems():
                new_key = replace_quoted_values(sql)
                # Sanitizing might result in different raw queries becoming
                # the same, so in that case we have to combine their data.
                if new_key not in sanitized:
                    sanitized[new_key] = arr
                else:
                    new_arr = arr
                    for i in range(len(sanitized[new_key])):
                        old_val = sanitized[new_key][i]
                        if i >= len(new_arr):
                            new_arr.push(old_val)
                        else:
                            new_arr[i] += old_val

                    sanitized[new_key] = new_arr
            parsed["slowSQL"]["otherThreads"] = sanitized
    return parsed

def map(k, d, v, cx):
    if '"slowSQL":' not in v:
        return
    if '"slowSQL":{"mainThread":{},"otherThreads":{}}' in v:
        return
    try:
        j = sanitize(v)
        [reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date] = d
        slowSQL = j["slowSQL"]
        for threadType, queries in slowSQL.iteritems():
            for query, arr in queries.iteritems():
                query = query.encode('string_escape')
                cx.write(",".join([threadType, submission_date, appName, appVersion, appUpdateChannel, query]), arr)
    except Exception as e:
        cx.write(",".join(["Error", str(e), traceback.format_exc()] + d), 1)

def setup_reduce(cx):
    cx.field_separator = ","

def median(v, already_sorted=False):
    ls = len(v)
    if ls == 0:
        return 0
    if already_sorted:
        s = v
    else:
        s = sorted(v)
    middle = int(ls / 2)
    if ls % 2 == 1:
        return s[middle]
    else:
        return (s[middle] + s[middle-1]) / 2.0

def reduce(k, v, cx):
    counts = [c for c,d in v]
    durations = [d for c,d in v]
    # Output fields:
    # thread_type, submission_date, app_name, app_version, app_update_channel,
    # query, document_count, total_invocations, total_duration, median_duration
    total_invocations = sum(counts)
    if total_invocations > 100:
        cx.write(",".join([k, str(len(v)), str(total_invocations), str(sum(durations))]), median(durations))
    else:
        print "Skipping a query with only", total_invocations, "invocations"


if __name__ == "__main__":
    raw = '''{
  "slowSQL": {
    "mainThread": {
      "SELECT a.item_id FROM moz_anno_attributes n JOIN moz_items_annos a ON n.id = a.anno_attribute_id WHERE n.name = :anno_name": [
        1,
        122
      ],
      "INSERT INTO locale (name, description, creator, homepageURL) VALUES (:name, :description, :creator, :homepageURL)": [
        1,
        146
      ]
    },
    "otherThreads": {
      "SELECT * FROM moz_places WHERE url LIKE '%facebook.com/blah%' ORDER BY frecency DESC LIMIT 3": [
        5,
        1000
      ],
      "SELECT * FROM moz_places WHERE url LIKE '%twitter.com/blah%' ORDER BY frecency DESC LIMIT 3": [
        5,
        1000
      ],
      "SELECT * FROM moz_places WHERE domain IN ('twitter.com','facebook.com')": [
        5,
        1000
      ],
      "SELECT * FROM moz_places WHERE domain NOT IN('twitter.com','facebook.com')": [
        5,
        1000
      ],
      "SELECT * FROM moz_places WHERE domain NOT IN(\\"twitter.com\\",\\"facebook.com\\")": [
        5,
        1000
      ]
    }
  }
}'''
    expected = '''{
  "slowSQL": {
    "mainThread": {
      "INSERT INTO locale (name, description, creator, homepageURL) VALUES (:name, :description, :creator, :homepageURL)": [
        1,
        146
      ],
      "SELECT a.item_id FROM moz_anno_attributes n JOIN moz_items_annos a ON n.id = a.anno_attribute_id WHERE n.name = :anno_name": [
        1,
        122
      ]
    },
    "otherThreads": {
      "SELECT * FROM moz_places WHERE domain IN (:private,:private)": [
        5,
        1000
      ],
      "SELECT * FROM moz_places WHERE domain NOT IN(:private,:private)": [
        10,
        2000
      ],
      "SELECT * FROM moz_places WHERE url LIKE :private ORDER BY frecency DESC LIMIT 3": [
        10,
        2000
      ]
    }
  }
}'''

    sanitized = sanitize(raw)
    sanitized_str = json.dumps(sanitized, sort_keys=True, indent=2, separators=(',', ': '))
    if sanitized_str != expected:
        print "Error sanitizing."
        print "Original:"
        print raw
        print "Sanitized:"
        print sanitized_str
        print "Expected:"
        print expected

    tests = ['select * From t where foo = "%bar%"', "hello there", "bla = '1235'", "bla = ain't no sunshine", "bla = ain't no su\"nshine", "bla = ain't no su\"ns'hine", "another with 'one' and \"two\" replacements"]
    expected = ['select * From t where foo = :private', "hello there", "bla = :private", "bla = ain't no sunshine", "bla = ain't no su\"nshine", "bla = ain:privatehine", "another with :private and :private replacements"]
    good = 0
    bad = 0
    for i in range(len(tests)):
        t = tests[i]
        e = expected[i]
        replaced = replace_quoted_values(t)
        if replaced != e:
            bad += 1
            print "Bad original:", t
            print "    expected:", e
            print "     cleaned:", replaced
        else:
            good += 1
            #print "Good: ", t, " -> ", e
    if bad > 0:
        print "Found", good, "good,", bad, "bad"
