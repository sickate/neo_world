import copy

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

