# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import unittest

from google.cloud.bigtable.client import Client
from google.cloud.environment_vars import BIGTABLE_EMULATOR

from playground.bigtable.bigtable_emulator import (
    BigtableEmulator, EmulatorCredentials
)

PROJECT_ID = 'project-id'
INSTANCE_ID = 'instance-id'
TABLE_ID = 'table-id'


class TestBigtableEmulator(unittest.TestCase):
    """ Tests for initialization and clean up of the emulator. """

    def test_bigtable_emulator(self):
        # Test that the BIGTABLE_EMULATOR environment variable is not set.
        self.assertNotIn(BIGTABLE_EMULATOR, os.environ)

        # Test that emulator is set up and running.
        with BigtableEmulator() as emulator:
            self.assertIsNotNone(emulator._emulator_pid)
            self.assertIn(BIGTABLE_EMULATOR, os.environ)

        # Test that emulator is not running and resources have been cleaned up.
        self.assertIsNone(emulator._emulator_pid)
        self.assertNotIn(BIGTABLE_EMULATOR, os.environ)

    def test_bigtable_emulator_client(self):
        # Test that we can get a client for the running emulator.
        with BigtableEmulator() as emulator:
            client = emulator.get_client(PROJECT_ID)
            self.assertIsNotNone(client)
            self.assertIn('localhost', client.emulator_host)

        # Test that we can't get a client if emulator is not running.
        self.assertIsNone(emulator.get_client(PROJECT_ID))


class TestBigtableEmulatorOperations(unittest.TestCase):
    """ Tests for performing Bigtable operations using the emulator. """

    emulator = BigtableEmulator()

    @classmethod
    def setUpClass(cls):
        cls.emulator.start()

    @classmethod
    def tearDownClass(cls):
        cls.emulator.finish()

    def test_bigtable_client(self):
        # Sanity test to ensure that we are using the emulator for testing.
        client = Client(PROJECT_ID, credentials=EmulatorCredentials())
        self.assertIn('localhost', client.emulator_host)

    def test_create_table(self):
        client = self.emulator.get_client(PROJECT_ID)
        instance = client.instance(INSTANCE_ID)
        table = instance.table(TABLE_ID)

        # Test that table is created successfully.
        table.create()
        tables = [os.path.basename(t.name) for t in instance.list_tables()]
        self.assertIn(TABLE_ID, tables)

        # Test that table is deleted successfully.
        table.delete()
        tables = [os.path.basename(t.name) for t in instance.list_tables()]
        self.assertNotIn(TABLE_ID, tables)
