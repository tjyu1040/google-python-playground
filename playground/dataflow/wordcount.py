# -*- coding: utf-8 -*-
""" Example of a wordcount pipeline. """
from __future__ import division, print_function

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.metrics import Metrics


class WordExtractingDoFn(beam.DoFn):
    """ Parse each line of input text into words. """

    def __init__(self):
        super(WordExtractingDoFn, self).__init__()
        cls = self.__class__
        self.words_counter = Metrics.counter(cls, 'words')
        self.word_lengths_counter = Metrics.counter(cls, 'word_lengths')
        self.empty_line_counter = Metrics.counter(cls, 'empty_lines')

    def process(self, element, *args, **kwargs):
        """
        Returns an iterator over the words of this element.

        The element is a line of text. If the line is blank, note that too.
        """
        text_line = element.strip()
        if not text_line:
            self.empty_line_counter.inc()

        words = re.findall(r'[A-Za-z\']+', text_line)
        word_count = len(words)
        lengths = sum([len(word) for word in words])

        self.words_counter.inc(word_count)
        self.word_lengths_counter.inc(lengths)

        return words


def run():
    pipeline = beam.Pipeline()

    # Read lines from input file.
    lines = pipeline | 'read' >> ReadFromText('data/king_arthur.txt')

    counts = (
        lines | 'split' >> beam.ParDo(
            WordExtractingDoFn()
        ).with_output_types(unicode)
        | 'pair_with_ones' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
    )

    # Format and write to output file.
    output = counts | 'format' >> beam.Map(
        lambda (word, count): '{}: {}'.format(word, count)
    )
    output | 'write' >> WriteToText('wordcount-output', '.txt')

    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
