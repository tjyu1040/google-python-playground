# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import os

import apache_beam as beam
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filebasedsink import FileBasedSink

try:
    from Bio import SeqIO
    from Bio.SeqRecord import SeqRecord
except ImportError:
    raise ImportError('BioPython dependency is not installed.')


@beam.typehints.with_output_types(SeqRecord)
class ReadFromSequence(beam.PTransform):
    def __init__(self, file_pattern):
        super(ReadFromSequence, self).__init__()
        self._source = SequenceSource(file_pattern)

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.io.Read(self._source)


@beam.typehints.with_input_types(SeqRecord)
class WriteToSequence(beam.PTransform):
    def __init__(self, file_path_prefix, file_format='fasta'):
        super(WriteToSequence, self).__init__()
        self._sink = SequenceSink(file_path_prefix, file_format)

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.io.Write(self._sink)


class SequenceSource(FileBasedSource):
    def __init__(self, file_pattern, alphabet=None):
        super(SequenceSource, self).__init__(file_pattern)
        self._alphabet = alphabet

    def read_records(self, file_name, offset_range_tracker):
        """

        Args:
            file_name (str): The file name to open for reading.
            offset_range_tracker (beam.io.OffsetRangeTracker): The range
                tracker for determining how many records to read in.

        Yields:
            SeqRecord: The sequence record from the read file.
        """
        file_extension = os.path.splitext(file_name)[-1][1:]

        with self.open_file(file_name) as file_handle:
            file_handle.seek(offset_range_tracker.start_position())
            records = SeqIO.parse(file_handle, file_extension, self._alphabet)
            for record in records:
                # Offset byte position by 1 to check for EOL.
                byte_position = file_handle.tell() - 1
                if offset_range_tracker.try_claim(byte_position):
                    yield record
                else:
                    break


class SequenceSink(FileBasedSink):
    def __init__(self, file_path_prefix, file_format='fasta'):
        super(SequenceSink, self).__init__(
            file_path_prefix, coder=None, file_name_suffix='.' + file_format
        )
        self._file_format = file_format

    def write_record(self, file_handle, value):
        SeqIO.write(value, file_handle, self._file_format)

    def write_encoded_record(self, file_handle, encoded_value):
        raise NotImplementedError
