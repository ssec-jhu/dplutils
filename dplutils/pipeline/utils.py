def dict_from_coord(coord, value):
    d = {}
    if '.' in coord:
        k, r = coord.split('.', 1)
        d[k] = dict_from_coord(r, value)
    else:
        d[coord] = value
    return d
