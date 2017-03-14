# -*- coding: utf-8 -*-
"""
A workflow that uses a simple Monte Carlo method to estimate π.

The algorithm computes the fraction of points drawn uniformly within the unit
square that also fall in the quadrant of the unit circle that overlaps the
square. A simple area calculation shows that this fraction should be π/4, so
we multiply our counts ratio by four to estimate π.
"""
from __future__ import division, print_function

import json
import logging
import random

import apache_beam as beam
from apache_beam.coders import Coder
from apache_beam.io import WriteToText
from apache_beam.typehints import Any, Iterable, Tuple


@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(Tuple[int, int, int])
def run_trials(runs):
    """
    Run trials and returns results.

    Parameters
    ----------
    runs : int
        Number of trial runs to be executed.

    Returns
    -------
    total_trials : int
        The total number of trials.
    inside_trials : int
        The number of inside trials.
    0
        The final zero is needed solely to make sure that the combine_results
        function has same type for inputs and outputs (a requirement for
        combiner functions).
    """
    inside_runs = 0
    for _ in range(runs):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        inside_runs += 1 if x ** 2 + y ** 2 <= 1.0 else 0
    return runs, inside_runs, 0


@beam.typehints.with_input_types(Iterable[Tuple[int, int, Any]])
@beam.typehints.with_output_types(Tuple[int, int, float])
def combine_results(results):
    """
    Combiner function to sum up trials and compute the estimate.

    Parameters
    ----------
    results : iter
        Iterable of 3-tuples (total trials, sum of inside trials, ignored)

    Returns
    -------
    sum_total_trials : int
        The sum of total trials.
    sum_inside_trials : int
        The sum of inside trials.
    probability : float
        The probability computed from the two numbers
    """
    total, inside = sum(r[0] for r in results), sum(r[1] for r in results)
    return total, inside, 4 * float(inside) / total


class EstimatePiTransform(beam.PTransform):
    """ Run 100 million trials and combine the results to estimate π. """

    def __init__(self, tries_per_work_items=100000):
        super(EstimatePiTransform, self).__init__()
        self.tries_per_work_items = tries_per_work_items

    def expand(self, input_or_inputs):
        return (
            input_or_inputs | 'Initialize' >> beam.Create(
                [self.tries_per_work_items] * 100
            ).with_output_types(int)
            | 'Run trials' >> beam.Map(run_trials)
            | 'Sum' >> beam.CombineGlobally(combine_results).without_defaults()
        )


class JsonCoder(Coder):
    """ A JSON coder used to format the final result. """

    def encode(self, x):
        return json.dumps(x)


def run():
    pipeline = beam.Pipeline()
    (pipeline | EstimatePiTransform()
     | WriteToText('estimate-pi', '.txt', coder=JsonCoder()))
    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
