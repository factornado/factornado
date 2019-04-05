# -*- coding: utf-8 -*-

import re
import pandas as pd


class ArgParseError(Exception):
    pass


class MissingArgError(Exception):
    pass


class SwaggerPath(str):
    """A simple class to overload str.format."""
    def format(self, uri, **kwargs):
        return super().format(
            uri=uri.replace("/([^/]*?)", ""), **kwargs)


def tansform_bson_id(y):
    x = {key: val for key, val in y.items()}
    x['id'] = str(x['id']) if x['id'] is not None else None
    return x


def to_ts(x):
    """Transforms a string, a timestamp or a timezoned-timestamp into a timestamp.
    """
    if pd.isnull(x) or x == '':
        return pd.NaT
    if type(x) is bytes:
        x = x.decode('utf-8')
    if type(x) is str and re.match("^[0-9]*$", x):
        x = int(x)
    ts = pd.Timestamp(x).value
    return pd.Timestamp(ts) if ts > 1e15 else pd.Timestamp(ts*1e6)
