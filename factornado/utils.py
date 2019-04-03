# -*- coding: utf-8 -*-

import re
import pandas as pd


class ArgParseError(Exception):
    pass


class MissingArgError(Exception):
    pass


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
