import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class TransformDoFn(beam.DoFn):
    def process(self, element):
        if None in element.values():
            return
        
        element['pressure'] = element['pressure'] / 6.895
        element['temperature'] = element['temperature'] * 1.8 + 32
        
        return [element]

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True, help='Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".')
    parser.add_argument('--output_topic', required=True, help='Output PubSub topic for results.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
           | "UTF-8 Decode" >> beam.Map(lambda x: x.decode('utf-8'))
           | "To Dict" >> beam.Map(lambda x: json.loads(x))
           | "Transform" >> beam.ParDo(TransformDoFn())
           | "To JSON String" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
           | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=known_args.output_topic)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
