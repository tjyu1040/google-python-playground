# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import logging
from pathlib2 import Path
import traceback

import apache_beam as beam
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filebasedsink import FileBasedSink

try:
    from Bio import SeqIO
    from Bio.SeqRecord import SeqRecord
except ImportError:
    raise ImportError('BioPython dependency is not installed.')


logger = logging.getLogger(__name__)


@beam.typehints.with_output_types(SeqRecord)
class ReadFromSequence(beam.PTransform):
    """ A PTransform class to read sequences from sequence files. """

    def __init__(self, file_pattern):
        """
        Initializes ReadFromSequence class.

        Args:
            file_pattern (str): The file pattern glob to search for sequence
            files.
        """
        super(ReadFromSequence, self).__init__()
        self._source = SequenceSource(file_pattern)

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.io.Read(self._source)


@beam.typehints.with_input_types(SeqRecord)
class WriteToSequence(beam.PTransform):
    """ A PTransform class to write sequences to sequence files. """

    def __init__(self, file_path_prefix, file_name_suffix):
        """
        Initializes WriteToSequence class.

        Args:
            file_path_prefix (str): The file path prefix to prepend to files.
            file_name_suffix (str): The file name suffix to append to files.
        """
        super(WriteToSequence, self).__init__()
        self._sink = SequenceSink(
            file_path_prefix, coder=None, file_name_suffix=file_name_suffix
        )

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.io.Write(self._sink)


class SequenceSource(FileBasedSource):
    """ A file-based source for reading a file glob of sequence files. """

    def read_records(self, file_name, offset_range_tracker):
        """

        Args:
            file_name (str): The file name to open for reading.
            offset_range_tracker (beam.io.OffsetRangeTracker): The range
                tracker for determining how many records to read in.

        Yields:
            SeqRecord: The sequence record from the read file.
        """
        # TODO: Use `offset_range_tracker` for claiming records.
        file_format = Path(file_name).suffix.replace('.', '')

        with self.open_file(file_name) as file_handle:
            try:
                file_handle.seek(offset_range_tracker.start_position())
                records = SeqIO.parse(file_handle, file_format)
                for record in records:
                    # Offset byte position by 1 to check for EOL.
                    byte_position = file_handle.tell() - 1
                    if offset_range_tracker.try_claim(byte_position):
                        yield record
                    else:
                        break
            except ValueError:
                msg = 'Skipping {!r}\n{}'
                logger.warning(msg.format(file_name, traceback.format_exc()))

    def to_runner_api_parameter(self, unused_context):
        super(SequenceSource, self).to_runner_api_parameter(unused_context)


class SequenceSink(FileBasedSink):
    """ A file-based sink for writing sequence records to. """

    def write_record(self, file_handle, value):
        file_format = self.file_name_suffix.value.replace('.', '')
        SeqIO.write(value, file_handle, file_format)

    def write_encoded_record(self, file_handle, encoded_value):
        # We use `write_record()` instead to avoid encoding SeqRecord objects
        # here.
        raise NotImplementedError
