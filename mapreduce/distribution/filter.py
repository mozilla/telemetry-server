import sys

with open(sys.argv[1]) as f:
    data = map(lambda x: x.split(","), f.readlines())
    data = map(lambda x: [x[0], float(x[1])], data)
    sum = reduce(lambda x, y: x + y[1], data, 0)
    data = map(lambda x: [x[0], 100*x[1]/sum], data)

    for row in data:
        print row[0] + "," + str(row[1])
