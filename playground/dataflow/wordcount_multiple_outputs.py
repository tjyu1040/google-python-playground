# -*- coding: utf-8 -*-
""" Example of a wordcount pipeline with multiple outputs. """
from __future__ import division, print_function

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.pvalue import SideOutputValue


class SplitLinesToWordsFn(beam.DoFn):
    """
    A transform to split a line of text into individual words.

    This transform will have 3 outputs:
        - main output: all words that are longer than 3 characters.
        - short words side output: all other words.
        - character count side output: Number of characters in each processed
          line.
    """

    # These tags will be used to tag the side outputs of this DoFn.
    SIDE_OUTPUT_TAG_SHORT_WORDS = 'tag_short_words'
    SIDE_OUTPUT_TAG_CHARACTER_COUNT = 'tag_character_count'

    def __init__(self):
        super(SplitLinesToWordsFn, self).__init__()

    def process(self, element, *args, **kwargs):
        """
        Receives a single element (a line) and produces words and side outputs.

        Important things to note here:
          - For a single element you may produce multiple main outputs:
            words of a single line.
          - For that same input you may produce multiple side outputs, along
            with multiple main outputs.
          - Side outputs may have different types (count) or may share the same
            type (words) as with the main output.
        """
        # Yield a count (integer) to the SIDE_OUTPUT_TAG_CHARACTER_COUNT tagged
        # collection.
        yield SideOutputValue(
            self.SIDE_OUTPUT_TAG_CHARACTER_COUNT, len(element)
        )

        words = re.findall(r'[A-Za-z\']+', element)

        for word in words:
            if len(word) < 3:
                # Yield word as a side output to the
                # SIDE_OUTPUT_TAG_SHORT_WORDS tagged collection.
                yield SideOutputValue(self.SIDE_OUTPUT_TAG_SHORT_WORDS, word)
            else:
                # Yield word to add it to the main collection.
                yield word


class CountWords(beam.PTransform):
    """
    A transform to count the occurrences of each word.

    A PTransform that converts a PCollection containing words into a
    PCollection of "word: count" strings.
    """

    def __init__(self):
        super(CountWords, self).__init__()

    def expand(self, input_or_inputs):
        return (
            input_or_inputs | 'pair_with_ones' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
            | 'format' >> beam.Map(
                lambda (word, count): '{}: {}'.format(word, count)
            )
        )


def run():
    pipeline = beam.Pipeline()
    output = 'wordcount-multiple-outputs'
    output_suffix = '.txt'

    lines = pipeline | 'read' >> ReadFromText('data/king_arthur.txt')

    # Split lines into several outputs.
    split_lines_result = (
        lines | beam.ParDo(SplitLinesToWordsFn()).with_outputs(
            SplitLinesToWordsFn.SIDE_OUTPUT_TAG_SHORT_WORDS,
            SplitLinesToWordsFn.SIDE_OUTPUT_TAG_CHARACTER_COUNT,
            main='words'
        )
    )

    # Multiple ways to access result.
    words, _, _ = split_lines_result
    short_words = split_lines_result[
        SplitLinesToWordsFn.SIDE_OUTPUT_TAG_SHORT_WORDS
    ]
    character_count = split_lines_result.tag_character_count

    # Write character count.
    (character_count | 'pair_with_key' >> beam.Map(
        lambda x: ('chars_temp_key', x))
     | beam.GroupByKey()
     | 'count_chars' >> beam.Map(lambda (_, counts): sum(counts))
     | 'write_chars' >> WriteToText(output + '-chars', output_suffix))

    # Write short word counts.
    (short_words | 'count_short_words' >> CountWords()
     | 'write_short_words' >> WriteToText(output + '-short', output_suffix))

    # Write word counts.
    (words | 'count_words' >> CountWords()
     | 'write_words' >> WriteToText(output + '-words', output_suffix))

    pipeline.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
