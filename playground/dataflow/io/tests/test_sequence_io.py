# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

import os
import shutil
import tempfile
import unittest

from apache_beam.io.source_test_utils import (
    assert_sources_equal_reference_source
)

try:
    from playground.dataflow.io.sequence_io import SequenceSource
    skip = False
except ImportError:
    skip = True


class TestCaseWithTempDirCleanUp(unittest.TestCase):
    def setUp(self):
        self.tmp_dirs = []

    def tearDown(self):
        for path in self.tmp_dirs:
            if os.path.exists(path):
                shutil.rmtree(path)
        self.tmp_dirs = []

    def _create_temp_dir(self):
        temp_dir = tempfile.mkdtemp()
        self.tmp_dirs.append(temp_dir)
        return temp_dir

    def _create_temp_file(self, name='', suffix='', tmp_dir=None):
        if not name:
            name = tempfile.template
        if tmp_dir is None:
            tmp_dir = self._create_temp_dir()
        file_name = tempfile.NamedTemporaryFile(
            delete=False, prefix=name, dir=tmp_dir,
            suffix=suffix
        ).name
        return file_name


@unittest.skipIf(skip, 'BioPython dependency is not installed.')
class TestSequenceSource(TestCaseWithTempDirCleanUp):
    def setUp(self):
        super(TestSequenceSource, self).setUp()
        self.maxDiff = None

    def write_data(self, num_records, tmp_dir=None, no_data=False):
        from Bio import SeqIO
        from Bio.Seq import Seq
        from Bio.SeqRecord import SeqRecord
        tmp_file = self._create_temp_file(suffix='.fastq', tmp_dir=tmp_dir)
        expected_sequences = []
        if not no_data:
            with open(tmp_file, 'w') as file_handle:
                for i in range(1, num_records + 1):
                    seq = Seq('ACGT' * i)
                    record = SeqRecord(
                        seq, id='test{}'.format(i),
                        letter_annotations={'phred_quality': [1] * len(seq)},
                    )
                    SeqIO.write(record, file_handle, 'fastq')
                    expected_sequences.append(seq)
        return tmp_file, expected_sequences

    def write_pattern(self, num_records_per_file, no_data=False):
        expected_data = []
        tmp_dir = self._create_temp_dir()
        for num_records in num_records_per_file:
            tmp_file, expected = self.write_data(
                num_records, tmp_dir=tmp_dir, no_data=no_data
            )
            expected_data.extend(expected)
        return os.path.join(tmp_dir, '*.fastq'), expected_data

    def _run_read_test(self, file_or_pattern, expected_data):
        source = SequenceSource(file_or_pattern)
        range_tracker = source.get_range_tracker(None, None)
        read_data = [record.seq for record in source.read(range_tracker)]
        self.assertItemsEqual(expected_data, read_data)

    def test_read_single_file(self):
        file_name, expected_data = self.write_data(10)
        assert len(expected_data) == 10
        self._run_read_test(file_name, expected_data)

    def test_read_empty_single_file(self):
        file_name, expected_data = self.write_data(10, no_data=True)
        assert len(expected_data) == 0
        self._run_read_test(file_name, expected_data)

    def test_read_file_pattern(self):
        file_pattern, expected_data = self.write_pattern([2, 3, 5])
        assert len(expected_data) == 10
        self._run_read_test(file_pattern, expected_data)

    def test_read_file_pattern_with_empty_files(self):
        file_pattern, expected_data = self.write_pattern(
            [2, 3, 5], no_data=True
        )
        assert len(expected_data) == 0
        self._run_read_test(file_pattern, expected_data)

    def test_read_after_splitting(self):
        file_name, expected_data = self.write_data(10)
        assert len(expected_data) == 10
        source = SequenceSource(file_name)
        splits = [split for split in source.split(desired_bundle_size=100)]
        ref_source_info = (source, None, None)
        sources_info = ([
            (split.source, split.start_position, split.stop_position)
            for split in splits
        ])
        assert_sources_equal_reference_source(ref_source_info, sources_info)
