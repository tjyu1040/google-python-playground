# -*- coding: utf-8 -*-
""" Various example implementations of a custom PTransform object. """
from __future__ import division, print_function

import logging

import apache_beam as beam


class CountSubClass(beam.PTransform):
    """ Count as a subclass of PTransform, with an apply method. """

    def __init__(self):
        super(CountSubClass, self).__init__()

    def expand(self, input_or_inputs):
        return (
            input_or_inputs | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | beam.CombinePerKey(sum)
        )


def run_count_sub_class(input, output):
    """ Runs the example pipeline with a sub-classed PTransform class. """
    logging.info('Running pipeline with sub-classed PTransform.')
    pipeline = beam.Pipeline()
    (pipeline | beam.io.ReadFromText(input)
     | CountSubClass()
     | beam.io.WriteToText(output))
    pipeline.run().wait_until_finish()


@beam.ptransform_fn
def count_decorated_fn(input_or_inputs):
    """ Count as a decorated function. """
    return (
        input_or_inputs | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | beam.CombinePerKey(sum)
    )


def run_count_decorated_fn(input, output):
    """ Runs the example pipeline with a decorated PTransform function. """
    logging.info('Running pipeline with decorated PTransform function.')
    pipeline = beam.Pipeline()
    (pipeline | beam.io.ReadFromText(input)
     | count_decorated_fn()
     | beam.io.WriteToText(output))
    pipeline.run().wait_until_finish()


@beam.ptransform_fn
def count_decorated_with_side_input_fn(input_or_inputs, factor=1):
    """ Count as a decorated function with a side input. """
    return (
        input_or_inputs | 'pair_with_one' >> beam.Map(lambda x: (x, factor))
        | beam.CombinePerKey(sum)
    )


def run_count_decorated_with_side_input_fn(input, output):
    """
    Runs the example pipeline with a decorated PTransform function with a
    side input.
    """
    logging.info(
        'Running pipeline with decorated PTransform function with side input.'
    )
    pipeline = beam.Pipeline()
    (pipeline | beam.io.ReadFromText(input)
     | count_decorated_with_side_input_fn(2)
     | beam.io.WriteToText(output))
    pipeline.run().wait_until_finish()


def run():
    input_file = 'data/king_arthur.txt'
    run_count_sub_class(input_file, 'subclass')
    run_count_decorated_fn(input_file, 'decorated')
    run_count_decorated_with_side_input_fn(input_file, 'decorated_side_input')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
