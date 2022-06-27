import copy
from functools import reduce

def str_join(series):
    return reduce(lambda x, y: f'{x},{y}', series)


def contains(small, big):
    for i in range(len(big)-len(small)+1):
        for j in range(len(small)):
            if big[i+j] != small[j]:
                break
        else:
            return i, i+len(small)
    return False


def subtract(a, b):
    return [x for x in a if x not in b]


def intersection(lst1, lst2):
    return [value for value in lst1 if value in lst2]


def merge_text_list(lst):
    if isinstance(lst, str):
        return '+'.join(set(map(lambda c: c.strip(), lst.split(','))))
    else:
        return 'NA'
