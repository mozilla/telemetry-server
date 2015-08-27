# A very simple MR job to simply count the number of 
# occurrences of each key. Useful for investigating
# the number of duplicate submissions.

def map(k, d, v, cx):
    cx.write(k, 1)

def reduce(k, v, cx):
    cx.write(k, sum(v))
