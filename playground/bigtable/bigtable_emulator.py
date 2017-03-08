# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import subprocess

import psutil

from google.cloud.bigtable.client import Client
from google.cloud.environment_vars import BIGTABLE_EMULATOR

EMULATOR_BASE_CMD = ['gcloud', 'beta', 'emulators', 'bigtable']

BIGTABLE_READY_LINE_PREFIX = '[bigtable] Cloud Bigtable emulator running on '


def cleanup(pid):
    """ Cleanup a process (including all of its children). """
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


class EmulatorCredentials(object):

    @staticmethod
    def create_scoped_required():
        return False


class BigtableEmulator:

    def __init__(self):
        self.emulator_pid = None

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
                BIGTABLE_READY_LINE_PREFIX)

        self.emulator_pid = process.pid
        # Initialize emulator host environment variables.
        BigtableEmulator.initialize_environment()

    def finish(self):
        """ Stop the Bigtable emulator and clean up resources. """
        if self.emulator_pid:
            cleanup(self.emulator_pid)

    @staticmethod
    def initialize_environment():
        """ Initialize Bigtable emulator environment variables for testing. """
        process = subprocess.Popen(
            EMULATOR_BASE_CMD + ['env-init'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        emulator_host = stdout.split('=')[-1].strip()
        os.environ[BIGTABLE_EMULATOR] = emulator_host

    @staticmethod
    def get_client(project_id, read_only=False):
        """
        Get the client associated with the Bigtable emulator.

        Parameters
        ----------
        project_id : str
            Project ID to work with. Any arbitrary string will work.
        read_only : bool
            Indicates whether client should be read-only or not.

        Returns
        -------
        client : Client
            Client initialized with emulator credentials.
        """
        admin = not read_only
        client = Client(project=project_id, credentials=EmulatorCredentials(),
                        admin=admin, read_only=read_only)
        return client if client.emulator_host is not None else None
