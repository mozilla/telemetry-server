def map(key, dims, value, context):
    context.write(key[0:3], 1)

def reduce(key, values, context):
    context.write(key, sum([int(v) for v in values]))
