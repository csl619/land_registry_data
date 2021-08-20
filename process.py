import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime
from hashlib import md5


class RemoveQuotations(beam.DoFn):
    def process(self, element):
        # remove double quotes from each item within the array.
        for i, item in enumerate(element):
            element[i] = item.replace('"', '')
        return [
            element
        ]


class AddPropertyId(beam.DoFn):
    def process(self, element):
        # start hash algorithm.
        hash = md5()
        # create a hash object using the array items for paon, saon, street and postcode.
        hash.update(
            f'{element[7]}{element[8]}{element[9]}{element[3]}'.encode('utf-8'))
        # convert to digest and insert into the element array at position zero.
        element.insert(0, hash.hexdigest())
        return [
            element
        ]


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='sample_data.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default=f'output_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.ndjson',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        regex = r''',(?=(?:(?:[^\"]*\"){2})*[^\"]*$)''' 
        lines = p | 'Open input file' >> ReadFromText(known_args.input)

        data = (
            lines
            | 'Split lines to arrays' >> beam.Regex.split(regex, outputEmpty=True)
            | 'Remove double quotes from array items' >> beam.ParDo(RemoveQuotations())
            | 'Add unique identifier for each property' >> beam.ParDo(AddPropertyId())
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
