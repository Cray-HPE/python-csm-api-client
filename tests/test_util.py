#
# MIT License
#
# (C) Copyright 2024 Hewlett Packard Enterprise Development LP
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

import unittest

from csm_api_client.util import pop_val_by_path, strip_suffix


class TestPopValByPath(unittest.TestCase):
    """Tests for the pop_val_by_path function."""

    def test_pop_val_by_path_flat(self):
        """Test that the function can pop a value by a flat path."""
        mapping = {
            'foo': 'bar'
        }
        self.assertEqual(pop_val_by_path(mapping, 'foo'), 'bar')
        self.assertEqual(mapping, {})

    def test_pop_val_by_path_dotted_path(self):
        """Test that the function can pop a value by dotted path."""
        mapping = {
            'foo': {
                'bar': 'baz'
            }
        }
        self.assertEqual(pop_val_by_path(mapping, 'foo.bar'), 'baz')
        self.assertEqual(mapping, {'foo': {}})

    def test_pop_val_by_path_dotted_path_remaining(self):
        """Test that the function can pop a value by a dotted path and leave other keys unchanged."""
        mapping = {
            'foo': {
                'bar': 'baz',
                'bat': 'qux'
            }
        }
        self.assertEqual(pop_val_by_path(mapping, 'foo.bar'), 'baz')
        self.assertEqual(mapping, {'foo': {'bat': 'qux'}})

    def test_pop_val_by_path_missing(self):
        """Test that the function can pop a value by path when path doesn't exist."""
        mapping = {
            'foo': {
                'bar': 'baz'
            }
        }
        self.assertIsNone(pop_val_by_path(mapping, 'foo.bat'))
        self.assertEqual(mapping, {'foo': {'bar': 'baz'}})

    def test_pop_val_by_path_missing_default(self):
        """Test that the function can pop a value by path when path doesn't exist and return a default."""
        mapping = {
            'foo': {
                'bar': 'baz'
            }
        }
        self.assertEqual(pop_val_by_path(mapping, 'foo.bat', 'default'), 'default')
        self.assertEqual(mapping, {'foo': {'bar': 'baz'}})

    def test_pop_val_by_path_empty(self):
        """Test that the function can pop a value by path when the dict is empty"""
        mapping = {}
        self.assertIsNone(pop_val_by_path(mapping, 'foo.bar'))
        self.assertEqual(mapping, {})

    def test_pop_val_by_path_empty_path(self):
        """Test that the function raises an error when the path is empty."""
        mapping = {
            'foo': {
                'bar': 'baz'
            }
        }
        with self.assertRaises(ValueError):
            pop_val_by_path(mapping, '')


class TestStripSuffix(unittest.TestCase):
    """Tests for the strip_suffix function."""

    def test_strip_suffix(self):
        """Test that the function can strip a suffix from a string."""
        self.assertEqual(strip_suffix('foo.bar', '.bar'), 'foo')
        self.assertEqual(strip_suffix('foo.bar', 'bar'), 'foo.')
        self.assertEqual(strip_suffix('foo.bar', 'baz'), 'foo.bar')
        self.assertEqual(strip_suffix('foo.bar', ''), 'foo.bar')
        self.assertEqual(strip_suffix('foo.bar', 'foo.bar'), '')
        self.assertEqual(strip_suffix('foo.bar', 'oo.bar'), 'f')
        self.assertEqual(strip_suffix('foo.bar', 'foo.'), 'foo.bar')


if __name__ == '__main__':
    unittest.main()
