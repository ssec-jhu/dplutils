def dict_from_coord(coord, value):
    d = {}
    if '.' in coord:
        k, r = coord.split('.', 1)
        d[k] = dict_from_coord(r, value)
    else:
        d[coord] = value
    return d


def deque_extract(queue, condition):
    for i in range(len(queue)):
        if condition(queue[-1]):
            yield queue.pop()
        else:
            queue.rotate()
