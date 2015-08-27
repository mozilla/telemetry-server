def map(key, dims, value, context):
    submission_day = dims[-1]
    context.write(submission_day, 1)

def reduce(key, values, context):
    context.write(key, sum(values))
