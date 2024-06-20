import numpy as np


def dict_from_coord(coord, value):
    d = {}
    if "." in coord:
        k, r = coord.split(".", 1)
        d[k] = dict_from_coord(r, value)
    else:
        d[coord] = value
    return d


def deque_extract(queue, condition):
    # pull items off the queue that match condition, while retaining the
    # prioritization. Not thread-safe
    for i in range(len(queue)):
        if condition(queue[-1]):
            yield queue.pop()
        else:
            queue.rotate()


def split_dataframe(df, max_rows=None, num_splits=None):
    """Split dataframe by max number of rows or given splits.

    If num_splits is provided, ignore max_rows and produce a set of num_splits
    splits with each dataframe having approximately equal number of rows (one may be
    different in size).

    If max_rows provided, produce the smallest number of splits that ensure no
    single split has more than max_rows
    """
    if num_splits is None:
        num_splits = np.ceil(len(df) / max_rows)
    chunks = np.linspace(0, num_splits, num=len(df), endpoint=False, dtype=np.int32)
    return [chunk for _, chunk in df.groupby(chunks)]
