# -*- coding: utf-8 -*-
from __future__ import division, print_function

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.metrics import Metrics


class FilterTextFn(beam.DoFn):
    """
    A DoFn that filters for a specific key based on a regular expression.
    """

    def __init__(self, pattern):
        super(FilterTextFn, self).__init__()
        self.pattern = pattern
        cls = self.__class__
        self.matched_words = Metrics.counter(cls, 'matched_words')
        self.unmatched_words = Metrics.counter(cls, 'unmatched_words')

    def process(self, element, *args, **kwargs):
        word, _ = element
        if re.match(self.pattern, word):
            logging.info('Matched {}'.format(word))
            self.matched_words.inc()
            yield element
        else:
            logging.debug('Did not match {}'.format(word))
            self.unmatched_words.inc()


class CountWords(beam.PTransform):
    """
    A transform to count the occurrences of each word.

    A PTransform that converts a PCollection containing lines of text into a
    PCollection of (word, count) tuples.
    """

    def __init__(self):
        super(CountWords, self).__init__()

    def expand(self, input_or_inputs):
        return (
            input_or_inputs | 'split' >> beam.FlatMap(
                lambda x: re.findall(r'[A-Za-z\']+', x)
            ).with_output_types(unicode)
            | 'pair_with_ones' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
        )


def run():
    pipeline = beam.Pipeline()

    filtered_words = (
        pipeline | 'read' >> ReadFromText('data/king_arthur.txt')
        | CountWords()
        | 'FilterText' >> beam.ParDo(FilterTextFn('Camelot|Excalibur'))
    )

    beam.assert_that(
        filtered_words, beam.equal_to([('Camelot', 33), ('Excalibur', 17)])
    )

    output = filtered_words | 'format' >> beam.Map(
        lambda (word, count): '{}: {}'.format(word, count)
    )
    output | 'write' >> WriteToText('debugging-wordcount', '.txt')

    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
