# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import unittest

from google.cloud.bigtable.client import Client

from playground.bigtable.bigtable_emulator import (
    BigtableEmulator, EmulatorCredentials
)


class TestBigtableEmulator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.emulator = BigtableEmulator()
        cls.emulator.start()

    @classmethod
    def tearDownClass(cls):
        cls.emulator.finish()

    def test_bigtable_client(self):
        client = Client('project-id', credentials=EmulatorCredentials())
        self.assertIn('localhost', client.emulator_host)

    def test_create_table(self):
        client = Client('project-id', admin=True, credentials=EmulatorCredentials())
        instance = client.instance('instance-id')
        tables = [os.path.basename(t.name) for t in instance.list_tables()]
        self.assertNotIn('table-id', tables)

        instance.table('table-id').create()
        tables = [os.path.basename(t.name) for t in instance.list_tables()]
        self.assertIn('table-id', tables)

        table = instance.table('table-id')
        table.delete()
        tables = [os.path.basename(t.name) for t in instance.list_tables()]
        self.assertNotIn('table-id', tables)
