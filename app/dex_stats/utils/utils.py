from functools import wraps
from decimal import Decimal
from time import time
import numpy as np


def measure(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            print(f"Total execution time for {func.__name__} : {end_ if end_ > 0 else 0} ms")
    return _time_it


def remove_exponent(d):
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()


def enforce_float( num : [float, int, str] ) -> float:
    return "{:.10f}".format(num)


def sort_orders(orders, reverse=False):
    return sorted(orders, key=lambda order: order[0], reverse=reverse)


def prettify_orders(orders):
    return [ [enforce_float(order[0]), enforce_float(order[1])]
              for order
              in orders ]


def numforce_float(num) -> float:
    return enforce_float(np.format_float_positional(num, unique=True, trim='-', precision=10))