import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime

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


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
