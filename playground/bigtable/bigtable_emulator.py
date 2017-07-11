# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import subprocess

import psutil

from google.cloud.bigtable.client import Client
from google.cloud.environment_vars import BIGTABLE_EMULATOR

EMULATOR_BASE_CMD = ['gcloud', 'beta', 'emulators', 'bigtable']

BIGTABLE_READY_LINE_PREFIX = '[bigtable] Cloud Bigtable emulator running on '


class EmulatorCredentials(object):
    """ Mock credentials used only for Bigtable emulator. """

    @staticmethod
    def create_scoped_required():
        return False


class BigtableEmulator(object):
    """ Emulator for interacting with Bigtable. """

    def __init__(self):
        self._emulator_pid = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()

    def start(self):
        """ Start the Bigtable emulator. """

        process = subprocess.Popen(
            EMULATOR_BASE_CMD + ['start'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for emulator to initialize.
        emulator_ready = False
        while not emulator_ready:
            emulator_ready = process.stderr.readline().startswith(
                BIGTABLE_READY_LINE_PREFIX
            )

        self._emulator_pid = process.pid
        # Initialize emulator host environment variables.
        BigtableEmulator._initialize_environment()

    def finish(self):
        """ Stop the Bigtable emulator and clean up resources. """
        if self._emulator_pid:
            self._cleanup(self._emulator_pid)
            self._emulator_pid = None
            del os.environ[BIGTABLE_EMULATOR]

    @staticmethod
    def _initialize_environment():
        """ Initialize Bigtable emulator environment variables for testing. """
        process = subprocess.Popen(
            EMULATOR_BASE_CMD + ['env-init'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        emulator_host = stdout.split('=')[-1].strip()
        os.environ[BIGTABLE_EMULATOR] = emulator_host

    @staticmethod
    def _cleanup(pid):
        """ Cleanup a process (including all of its children).

        Args:
            pid (str): The process ID to kill.
        """
        try:
            proc = psutil.Process(pid)
            for child_proc in proc.children(recursive=True):
                try:
                    child_proc.kill()
                    child_proc.terminate()
                except psutil.NoSuchProcess:
                    pass
            proc.terminate()
            proc.kill()
        except psutil.NoSuchProcess:
            pass

    @staticmethod
    def get_client(project_id, read_only=False):
        """
        Get the client associated with the Bigtable emulator.

        Args:
            project_id (str): Project ID to work with. Any arbitrary string
                will work.
            read_only (:obj:`bool`, optional): Indicates whether client should
                be read-only or not. Defaults to False.

        Returns:
            Client: Google Cloud Bigtable API client initialized with emulator
                credentials. None if emulator is not running.
        """
        admin = not read_only
        client = Client(
            project=project_id, credentials=EmulatorCredentials(),
            admin=admin, read_only=read_only
        )
        return client if client.emulator_host is not None else None
