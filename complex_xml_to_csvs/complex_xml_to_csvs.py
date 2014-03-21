import argparse

import os
import gzip

import xml.sax
import sys

import complex_schema
import logging

log = logging.getLogger('complex_xml_to_csvs')


# classes that do something with the data
from record_processors import (
    CountLimitingRecordProcessor,
    BatchMakerRecordProcessor,
    CsvSplitter
)


STATES = 'export/ceg/rovat/alrovat/mezo/ujsor'.split('/')


class InvalidHierarchy(xml.sax.SAXParseException):
    pass


class State:

    def __init__(self, record_processor=None):
        self.document = None
        self.rovat = None
        self.alrovat = None

        self.record_processor = record_processor

        self.index = 0
        self.ceg_id = None
        self.mezo_id = None

    @property
    def next_element(self):
        if 0 <= self.index < len(STATES):
            return STATES[self.index]

    @property
    def current_element(self):
        if 0 < self.index <= len(STATES):
            return STATES[self.index - 1]

    def start_ceg(self, ceg_id):
        self.ceg_id = ceg_id
        self.document = dict(ceg_id=ceg_id)

    def start_rovat(self, rovat_id):
        self.rovat = []
        self.document[rovat_id] = self.rovat

    def start_alrovat(self, alrovat_id):
        self.alrovat = {'alrovat_id': alrovat_id}
        self.rovat.append(self.alrovat)

    def start_mezo(self, mezo_id):
        self.mezo_id = mezo_id
        self.alrovat[mezo_id] = ''

    def append_mezo(self, characters):
        self.alrovat[self.mezo_id] += characters

    def record_complete(self):
        self.record_processor.process(self.document)


class ComplexXMLHandler(xml.sax.handler.ContentHandler):

    def __init__(self, handlers, state):
        xml.sax.handler.ContentHandler.__init__(self)
        self.state = state
        self.handlers = handlers

    def startElement(self, name, attrs):
        if name != self.state.next_element:
            raise InvalidHierarchy(
                'hierarchy problem: unexpected start-element {0}'
                .format(name),
                AssertionError(),
                self._locator
            )
        assert name in self.handlers, name
        self.handlers[name].start(name, attrs, self.state)
        self.state.index += 1

    def endElement(self, name):
        if name != self.state.current_element:
            raise InvalidHierarchy(
                'hierarchy problem: unexpected end-element {0}'.format(name),
                AssertionError(),
                self._locator
            )
        assert name in self.handlers, name
        self.handlers[name].end(name, self.state)
        self.state.index -= 1

    def characters(self, characters):
        name = self.state.current_element
        self.handlers[name].characters(name, characters, self.state)


class ElementHandler:

    ALL_RECORDS = 0

    def start(self, name, attrs, state):
        pass

    def end(self, name, state):
        pass

    def characters(self, name, characters, state):
        pass


class DoNothing(ElementHandler):

    pass


class Handle_ceg(ElementHandler):

    def start(self, name, attrs, state):
        state.start_ceg(attrs['id'])

    def end(self, name, state):
        state.record_complete()


class Handle_rovat(ElementHandler):

    def start(self, name, attrs, state):
        state.start_rovat(attrs['id'])


class Handle_alrovat(ElementHandler):

    def start(self, name, attrs, state):
        state.start_alrovat(attrs['id'])


class Handle_mezo(ElementHandler):

    def start(self, name, attrs, state):
        state.start_mezo(attrs['id'])

    def characters(self, name, characters, state):
        state.append_mezo(characters)


class Handle_ujsor(ElementHandler):

    def start(self, name, attrs, state):
        state.append_mezo('\n')


def xml_handler_map():
    return dict(
        export=DoNothing(),
        ceg=Handle_ceg(),
        rovat=Handle_rovat(),
        alrovat=Handle_alrovat(),
        mezo=Handle_mezo(),
        ujsor=Handle_ujsor(),
    )


class FileProcessor:

    def __init__(self, record_processor):
        self.record_processor = record_processor

    def parse(self, input_source):
        state = State(record_processor=self.record_processor)
        try:
            xml.sax.parse(
                input_source,
                ComplexXMLHandler(
                    handlers=xml_handler_map(),
                    state=state)
            )
        except:
            log.exception('Error during parsing')

    def process(self, input_source):
        self.parse(input_source)
        self.record_processor.flush()


def make_directory(dir):
    try:
        os.mkdir(dir)
    except OSError:
        pass


def open_file(fname, mode='rb'):
    '''open gzip and non-gzip files transparently

    Whether to use compression is determined by the extension of the filename
    '''
    if fname.endswith('.gz'):
        return gzip.open(fname, mode)
    return open(fname, mode)


def xml_to_csv_batches(input_fname, output_dir, schema_file_xls, maxrecords):
    make_directory(output_dir)
    tables = complex_schema.read_tables(schema_file_xls)
    for table in tables:
        make_directory('{}/{}'.format(output_dir, table.name))

    record_processor = BatchMakerRecordProcessor(
        batch_size=1000,
        batch_processor=CsvSplitter(input_fname, output_dir, tables)
    )

    if maxrecords:
        record_processor = CountLimitingRecordProcessor(
            record_processor=record_processor,
            maxrecords=maxrecords
        )

    log.info('Converting %s', input_fname)
    input_source = open_file(input_fname)
    try:
        FileProcessor(record_processor).process(input_source)
    finally:
        input_source.close()


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Convert Complex's XML"
    )

    parser.add_argument(
        '--maxrecords',
        type=int,
        default=Handle_ceg.ALL_RECORDS,
        help='process MAXRECORDS records per file (default: all)'
    )
    parser.add_argument(
        '--output-dir',
        default='output',
        help='create csv files under this directory (default: %(default)s)'
    )
    parser.add_argument(
        '--schema-file-xls',
        default='R_export.txt.xls',
        help='xls file accompanying Complex\'s dump (default: %(default)s)'
    )
    parser.add_argument(
        'complex_xml_file',
        help='file to process'
    )

    return parser.parse_args(args)


def main():
    logging.basicConfig()
    args = parse_args(sys.argv[1:])
    if args.maxrecords != Handle_ceg.ALL_RECORDS:
        log.warning('Processing only %s "ceg"/file', args.maxrecords)

    xml_to_csv_batches(
        args.complex_xml_file,
        args.output_dir,
        args.schema_file_xls,
        args.maxrecords
    )


if __name__ == '__main__':
    main()
