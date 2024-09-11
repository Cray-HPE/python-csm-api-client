#
# MIT License
#
# (C) Copyright 2019-2022, 2024 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
"""
Contains structures and code that are generally useful the API client implementation.
"""

from typing import (
    Any,
    Mapping,
    Optional,
)


def get_val_by_path(
    mapping: Mapping,
    dotted_path: str,
    default_value: Optional[Any] = None
) -> Optional[Any]:
    """Get a value from a mapping (e.g. dict) based on a dotted path.

    For example, if `dict_val` is as follows:

    dict_val = {
        'foo': {
            'bar': 'baz'
        }
    }

    Then get_val_by_path(dict_val, 'foo.bar') would return 'baz', and something
    like get_val_by_path(dict_val, 'no.such.keys') would return None.

    Args:
        mapping: The dictionary in which to search for the dotted path.
        dotted_path: The dotted path to look for in the dictionary. The
            dot character, '.', separates the keys to use when traversing a
            nested dictionary.
        default_value: The default value to return when the given dotted path
            does not exist in the dict_val.

    Returns:
        The value that exists at the dotted path or `default_value` if no such
        path exists in `dict_val`.
    """
    keys = dotted_path.split('.')
    if not keys:
        raise ValueError(f'Invalid dotted_path "{dotted_path}"')

    current_val = mapping
    for key in keys:
        if current_val and key in current_val:
            current_val = current_val[key]
        else:
            return default_value
    return current_val


# Add a function called pop_val_by_path that removes a dotted path value from a dictionary
def pop_val_by_path(dict_val: dict, dotted_path: str, default_value: Optional[Any] = None) -> Any:
    """Remove a value from a dictionary using a dotted path.

    For example, if `dict_val` is as follows:

        dict_val = {
            'price_is_right': {
                'host': 'Bob Barker'
            }
        }

    Then the following call:

        pop_val_by_path(dict_val, 'price_is_right.host')

    Would return 'Bob Barker' and result in this dictionary:

        dict_val = {
            'price_is_right': {}
        }

    The dictionary `dict_val` is modified in place.
    """
    if not dotted_path:
        raise ValueError('pop_val_by_path requires a non-empty path')

    split_path = dotted_path.split('.')

    for key in split_path[:-1]:
        if not isinstance(dict_val.get(key), dict):
            return default_value
        dict_val = dict_val[key]

    # The final key is all that remains, so pop the key
    return dict_val.pop(split_path[-1], default_value)


def set_val_by_path(dict_val: dict, dotted_path: str, value: Any) -> None:
    """Set a value in a dictionary using a dotted path.

    If any values along the path are not a dictionary, this will overwrite those
    values with a dictionary.

    For example, if `dict_val` is as follows:

        dict_val = {
            'price_is_right': {
                'host': 'Bob Barker'
            }
        }

    Then the following calls:

        set_val_by_path(dict_val, 'price_is_right.host', 'Drew Carey')
        set_val_by_path(dict_val, 'price_is_right.genre', 'Game Show')

    Would result in this dictionary:

        dict_val = {
            'price_is_right': {
                'host': 'Drew Carey'
                'genre': 'Game Show'
            }
        }

    The subsequent calls:

        set_val_by_path(dict_val, 'price_is_right.host.first_name', 'Drew')
        set_val_by_path(dict_val, 'price_is_right.host.last_name', 'Carey')

    Would result in:

        dict_val = {
            'price_is_right': {
                'host': {
                    'first_name': 'Drew',
                    'last_name': 'Carey'
                }
                'genre': 'Game Show'
            }
        }

    The dictionary `dict_val` is modified in place.
    """
    if not dotted_path:
        raise ValueError('set_val_by_path requires a non-empty path')

    split_path = dotted_path.split('.')

    for key in split_path[:-1]:
        if not isinstance(dict_val.get(key), dict):
            dict_val[key] = {}
        dict_val = dict_val[key]

    dict_val[split_path[-1]] = value


def strip_suffix(s: str, suffix: str) -> str:
    """Remove a suffix from a string if it exists.

    Args:
        s: The string to remove the suffix from.
        suffix: The suffix to remove from the string.
    """
    if not suffix:
        return s
    if s.endswith(suffix):
        return s[:-len(suffix)]
    return s
