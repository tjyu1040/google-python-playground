# -*- coding: utf-8 -*-
""" Example of a minimal wordcount pipeline. """
from __future__ import division, print_function

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText


def run():
    pipeline = beam.Pipeline()

    # Read lines from input file.
    lines = pipeline | 'read' >> ReadFromText('data/king_arthur.txt')

    # Count words.
    counts = (
        lines | 'split' >> beam.FlatMap(
            lambda x: re.findall(r'[A-Za-z\']+', x)
        ).with_output_types(unicode)
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
    )

    # Format and write to output file.
    output = counts | 'format' >> beam.Map(
        lambda (word, count): '{}: {}'.format(word, count))
    output | 'write' >> WriteToText('minimal-wordcount-output', '.txt')

    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
