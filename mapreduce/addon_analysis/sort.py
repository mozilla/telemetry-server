import sys
import pandas

filename = sys.argv[1]
addons = pandas.io.parsers.read_csv(filename, header=None)

is_agg = addons[0] == "TOTAL"
total = float(addons[addons[0] == "TOTAL"][1])
addons = addons[addons[0] != "TOTAL"]
addons[1] /= total
addons = addons.sort(1, ascending=False)[:500]
addons.to_csv(sys.argv[2], index=False, header=False)
