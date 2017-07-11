# -*- coding: utf-8 -*-
from __future__ import division, print_function

import unittest

from google.cloud.bigtable.row_filters import (
    ColumnQualifierRegexFilter, ColumnRangeFilter, RowFilterChain,
    RowFilterUnion, RowKeyRegexFilter
)

from playground.bigtable.bigtable_emulator import (
    BigtableEmulator, EmulatorCredentials
)
from playground.bigtable.bigtable_utils import (
    build_bigtable_client, build_row_filter
)


class TestBigtableUtils(unittest.TestCase):

    def test_build_bigtable_client(self):
        with BigtableEmulator():
            credentials = EmulatorCredentials()
            client = build_bigtable_client('project-id', credentials)
            self.assertIn('project-id', client.project_name)
            self.assertEqual(credentials, client.credentials)


class TestRowFilterBuilder(unittest.TestCase):

    def test_build_row_key_filter(self):
        row_filter = build_row_filter()
        self.assertIsNone(row_filter)

        row_filter = build_row_filter('rk')
        self.assertIsInstance(row_filter, RowKeyRegexFilter)
        for obj in [{}, 100, 10.0]:
            with self.assertRaises(TypeError):
                build_row_filter(obj)

    def test_build_column_families_filter(self):
        row_filter = build_row_filter(column_families=['cf0'])
        self.assertIsInstance(row_filter, ColumnRangeFilter)

        row_filter = build_row_filter(column_families=['cf0', 'cf1'])
        self.assertIsInstance(row_filter, RowFilterUnion)

        row_filter = build_row_filter('rk', column_families=['cf0'])
        self.assertIsInstance(row_filter, RowFilterChain)

        for obj in [{}, 100, 10.0]:
            with self.assertRaises(TypeError):
                build_row_filter(column_families=obj)

    def test_build_columns_filter(self):
        row_filter = build_row_filter(columns=['c0'])
        self.assertIsInstance(row_filter, ColumnQualifierRegexFilter)

        row_filter = build_row_filter(columns=['c0', 'c1'])
        self.assertIsInstance(row_filter, RowFilterUnion)

        row_filter = build_row_filter('rk', columns=['c0', 'c1'])
        self.assertIsInstance(row_filter, RowFilterChain)

        row_filter = build_row_filter(column_families=['cf0'], columns=['c0'])
        self.assertIsInstance(row_filter, RowFilterChain)

        row_filter = build_row_filter('rk', ['cf0'], ['c0'])
        self.assertIsInstance(row_filter, RowFilterChain)

        for obj in [{}, 100, 10.0]:
            with self.assertRaises(TypeError):
                build_row_filter(columns=obj)
