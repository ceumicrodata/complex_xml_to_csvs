# -*- encoding: utf-8 -*-
import sys
from unittest import TestCase
from complex_xml_to_csvs import complex_xml_to_csvs as module
from complex_xml_to_csvs import record_processors
import xml.sax
import StringIO
import mock


VALID_COMPLEX_XML = '''<?xml version="1.0" encoding="ISO8859-2" ?>
<export>
<ceg id="0006414987">
<rovat id = "0">
<alrovat id = "1">
<mezo id = "bir">00</mezo>
<mezo id = "cf">06</mezo>
<mezo id = "szam">414987</mezo>
<mezo id = "plus">10</mezo>
</alrovat>
</rovat>
</ceg>
</export>
'''

VALID_COMPLEX_XML_AS_JSON = {
    'ceg_id': '0006414987',
    '0': [
        {
            'alrovat_id': '1',
            'bir': '00',
            'cf': '06',
            'szam': '414987',
            'plus': '10'
        },
    ]
}


def no_output_xml_processor(handlers=None, state=None):
    return module.ComplexXMLHandler(
        handlers=handlers or module.xml_handler_map(),
        state=(
            state
            or module.State(
                record_processor=record_processors.RecordProcessor()
            )
        )
    )


def make_xml(elements):
    lines = (['<?xml version="1.0" encoding="ISO8859-2" ?>']
             + ['<{0} id="1">'.format(e) for e in elements]
             + ['</{0}>'.format(e) for e in reversed(elements)])
    return '\n'.join(lines)


class RememberingHandlers(module.ElementHandler):

    # adds a dictionary interface above the handler interface
    # to spare another layer

    def __init__(self):
        self.referenced_elements = set()
        self.start_called = set()
        self.end_called = set()
        self.characters_called = set()

    def start(self, name, attrs, state):
        self.start_called.add(name)

    def end(self, name, state):
        self.end_called.add(name)

    def characters(self, name, characters, state):
        self.characters_called.add(name)

    def __getitem__(self, name):
        self.referenced_elements.add(name)
        return self

    def __contains__(self, name):
        self.referenced_elements.add(name)
        return True


class TestComplexXMLHandler(TestCase):

    def test_parse(self):
        xml.sax.parseString(
            '<?xml version="1.0" encoding="ISO8859-2" ?>\n'
            '<export></export>',
            no_output_xml_processor()
        )

    def test_hierarchy(self):
        good_xml = make_xml('export ceg rovat alrovat mezo'.split())
        xml.sax.parseString(good_xml, no_output_xml_processor())

        bad_xml = make_xml('export ceg rovat rovat'.split())
        with self.assertRaises(Exception):
            xml.sax.parseString(bad_xml, no_output_xml_processor())

    def _generate_startElement_asserts_if_hierarchy_not_followed(self):
        for i, element in enumerate(module.STATES):
            print "s.startElement('{0}', dict(id='{0}_id'))".format(element)
            for j, bad_element in enumerate(module.STATES):
                if j != i + 1:
                    print "check_bad('{0}')".format(bad_element)
        assert False

    def test_startElement_asserts_if_hierarchy_not_followed(self):
        # extensive test: test all invalid continuations
        def check_bad(element):
            with self.assertRaises(xml.sax.SAXParseException):
                p.startElement(element, dict(id="bad_element"))

        p = no_output_xml_processor()
        p.setDocumentLocator(xml.sax.xmlreader.Locator())

        p.startElement('export', dict(id='export_id'))
        check_bad('export')
        check_bad('rovat')
        check_bad('alrovat')
        check_bad('mezo')
        p.startElement('ceg', dict(id='ceg_id'))
        check_bad('export')
        check_bad('ceg')
        check_bad('alrovat')
        check_bad('mezo')
        p.startElement('rovat', dict(id='rovat_id'))
        check_bad('export')
        check_bad('ceg')
        check_bad('rovat')
        check_bad('mezo')
        p.startElement('alrovat', dict(id='alrovat_id'))
        check_bad('export')
        check_bad('ceg')
        check_bad('rovat')
        check_bad('alrovat')
        p.startElement('mezo', dict(id='mezo_id'))
        check_bad('export')
        check_bad('ceg')
        check_bad('rovat')
        check_bad('alrovat')
        check_bad('mezo')

    def test_endElement_steps_back_in_hierarchy(self):
        p = no_output_xml_processor()

        p.startElement('export', dict(id='export_id'))
        self.assertEquals(1, p.state.index)
        p.endElement('export')
        self.assertEquals(0, p.state.index)

    def test_endElement_checks_validates_element_name(self):
        p = no_output_xml_processor()
        p.setDocumentLocator(xml.sax.xmlreader.Locator())

        p.startElement('export', dict(id='export_id'))
        self.assertEquals(1, p.state.index)
        with self.assertRaises(xml.sax.SAXParseException):
            p.endElement('something')

    def test_startElement_handlers_are_called(self):
        remembering_handlers = RememberingHandlers()
        s = no_output_xml_processor(handlers=remembering_handlers)

        s.startElement('export', dict(id='export_id'))
        self.failUnless('export' in remembering_handlers.referenced_elements)
        self.failUnless('export' in remembering_handlers.start_called)

    def test_characters_handlers_are_called(self):
        remembering_handlers = RememberingHandlers()
        s = no_output_xml_processor(handlers=remembering_handlers)

        s.startElement('export', dict(id='export_id'))
        s.characters('zs')

        self.failUnless('export' in remembering_handlers.referenced_elements)
        self.failUnless('export' in remembering_handlers.characters_called)

    def test_endElement_handlers_are_called(self):
        remembering_handlers = RememberingHandlers()
        s = no_output_xml_processor(handlers=remembering_handlers)

        s.startElement('export', dict(id='export_id'))
        s.endElement('export')

        self.failUnless('export' in remembering_handlers.referenced_elements)
        self.failUnless('export' in remembering_handlers.end_called)


class TestState(TestCase):

    def test_next_element(self):
        s = module.State()
        self.assertEquals('export', s.next_element)
        s.index = 1
        self.assertEquals('ceg', s.next_element)
        s.index = len(module.STATES)
        self.assertEquals(None, s.next_element)

    def test_current_element(self):
        s = module.State()
        self.assertEquals(None, s.current_element)
        s.index = 1
        self.assertEquals('export', s.current_element)

    def test_start_ceg(self):
        s = module.State()
        s.start_ceg('a ceg_id')

        self.assertEquals('a ceg_id', s.ceg_id)
        self.assertEquals({'ceg_id': 'a ceg_id'}, s.document)

    def test_start_rovat(self):
        s = module.State()
        s.start_ceg('a ceg_id')

        s.start_rovat('a rovat')
        self.assertEquals([], s.rovat)
        self.assertEquals({'ceg_id': 'a ceg_id', 'a rovat': []}, s.document)

    def test_start_alrovat(self):
        s = module.State()
        s.start_ceg('a ceg_id')
        s.start_rovat('a rovat')

        s.start_alrovat('alrovat')
        self.assertEquals({'alrovat_id': 'alrovat'}, s.alrovat)
        self.assertEquals(
            {
                'ceg_id': 'a ceg_id',
                'a rovat': [{'alrovat_id': 'alrovat'}]
            },
            s.document
        )

    def test_start_mezo(self):
        s = module.State()
        s.start_ceg('a ceg_id')
        s.start_rovat('a rovat')
        s.start_alrovat('alrovat')
        s.start_mezo('mezo')

        self.assertEquals('mezo', s.mezo_id)
        self.assertEquals(
            {
                'ceg_id': 'a ceg_id',
                'a rovat': [
                    {
                        'alrovat_id': 'alrovat',
                        'mezo': ''
                    }
                ]
            },
            s.document
        )

    def test_start_mezo_on_second_alrovat(self):
        s = module.State()
        s.start_ceg('a ceg_id')
        s.start_rovat('a rovat')
        s.start_alrovat('alrovat1')
        s.start_alrovat('alrovat2')
        s.start_mezo('mezo')

        self.assertEquals('mezo', s.mezo_id)
        self.assertEquals(
            {
                'ceg_id': 'a ceg_id',
                'a rovat': [
                    {'alrovat_id': 'alrovat1'},
                    {'alrovat_id': 'alrovat2', 'mezo': ''}
                ]
            },
            s.document
        )

    def test_append_mezo(self):
        s = module.State()
        s.start_ceg('a ceg_id')
        s.start_rovat('a rovat')
        s.start_alrovat('alrovat')
        s.start_mezo('mezo')

        s.append_mezo('value')

        self.assertEquals('mezo', s.mezo_id)
        self.assertEquals(
            {
                'ceg_id': 'a ceg_id',
                'a rovat': [
                    {'alrovat_id': 'alrovat', 'mezo': 'value'}
                ]
            },
            s.document
        )

    def test_complete_record_calls_record_processor_with_the_record(self):
        rp = record_processors.RecordProcessor()
        rp.process = mock.Mock(rp.process)
        s = module.State(record_processor=rp)
        s.document = mock.sentinel.document

        s.record_complete()
        rp.process.assert_called_once_with(mock.sentinel.document)


class TestHandle_ceg(TestCase):

    def test_start_calls_state_start_ceg(self):
        s = module.State()
        s.start_ceg = mock.Mock(s.start_ceg)
        h = module.Handle_ceg()
        attrs = dict(id='4')
        h.start('ceg', attrs, s)

        s.start_ceg.assert_called_once_with('4')

    def test_end_calls_state_record_complete(self):
        s = module.State()
        s.record_complete = mock.Mock(s.record_complete)
        h = module.Handle_ceg()
        attrs = dict(id='mezo_4')

        h.start('ceg', attrs, s)
        h.end('ceg', s)
        s.record_complete.assert_called_once_with()


class TestHandle_rovat(TestCase):

    def test_start_calls_state_start_rovat(self):
        s = module.State()
        s.start_rovat = mock.Mock(s.start_rovat)
        h = module.Handle_rovat()
        attrs = dict(id='4')
        h.start('rovat', attrs, s)

        s.start_rovat.assert_called_once_with('4')


class TestHandle_alrovat(TestCase):

    def test_start_calls_state_start_alrovat(self):
        s = module.State()
        s.start_alrovat = mock.Mock(s.start_alrovat)
        h = module.Handle_alrovat()
        attrs = dict(id='alrovat_4')
        h.start('alrovat', attrs, s)

        s.start_alrovat.assert_called_once_with('alrovat_4')


class TestHandle_mezo(TestCase):

    def state_for_mezo(self):
        s = module.State()
        s.start_ceg('ceg')
        s.start_rovat('rovat')
        s.start_alrovat('alrovat')
        return s

    def test_start_calls_state_start_mezo(self):
        s = module.State()
        s.start_mezo = mock.Mock(s.start_mezo)
        h = module.Handle_mezo()
        attrs = dict(id='mezo_4')
        h.start('mezo', attrs, s)

        s.start_mezo.assert_called_once_with('mezo_4')

    def test_characters_calls_state_append_mezo(self):
        s = self.state_for_mezo()
        s.append_mezo = mock.Mock(s.append_mezo)

        h = module.Handle_mezo()
        h.characters('mezo', 'asd', s)

        s.append_mezo.assert_called_once_with('asd')

    def test_characters_stored(self):
        s = self.state_for_mezo()
        h = module.Handle_mezo()
        h.start('mezo', attrs=dict(id='mezo'), state=s)

        h.characters('mezo', 'a', s)
        self.assertEquals('a', s.alrovat['mezo'])

    def test_multiple_calls_to_characters_concatenated(self):
        s = self.state_for_mezo()
        h = module.Handle_mezo()
        h.start('mezo', attrs=dict(id='mezo'), state=s)

        h.characters('mezo', 'a', s)
        h.characters('mezo', 'b', s)
        self.assertEquals('ab', s.alrovat['mezo'])


class TestHandle_ujsor(TestCase):

    def test_start_stores_newline_to_mezo_value(self):
        s = module.State()
        s.start_ceg('ceg')
        s.start_rovat('rovat')
        s.start_alrovat('alrovat')
        s.start_mezo('mezo')

        s.append_mezo('value')

        h = module.Handle_ujsor()

        h.start('ujsor', None, s)
        self.assertEquals('value\n', s.alrovat['mezo'])


class TestFileProcessor(TestCase):

    def record_processor(self):
        return record_processors.RecordProcessor()

    def get_processor(self, record_processor=None):
        return module.FileProcessor(
            record_processor=record_processor or self.record_processor())

    def test_process_calls_parse_to_read_xml(self):
        p = self.get_processor()
        p.parse = mock.Mock(p.parse)

        fname = 'file_does_not_exists_as_it_will_not_be_opened.xml'
        p.process(fname)

        p.parse.assert_called_once_with(fname)

    def test_record_processor_is_called_with_records(self):
        rp = record_processors.RecordProcessor()
        rp.process = mock.Mock(rp.process)
        p = self.get_processor(record_processor=rp)
        p.process(StringIO.StringIO(VALID_COMPLEX_XML))

        rp.process.assert_called_once_with(VALID_COMPLEX_XML_AS_JSON)

    def test_process_calls_flush_on_record_processor_after_document(self):
        calls = []

        def process_called(document):
            calls.append('process')

        def flush_called():
            calls.append('flush')

        rp = record_processors.RecordProcessor()
        rp.process = mock.Mock(rp.process, side_effect=process_called)
        rp.flush = mock.Mock(rp.flush, side_effect=flush_called)
        p = self.get_processor(record_processor=rp)
        p.process(StringIO.StringIO(VALID_COMPLEX_XML))

        self.assertEquals(['process', 'flush'], calls)


class Test_parse_args(TestCase):

    def test_arguments_are_stored_into_files(self):
        args = module.parse_args('complex321.xml.gz'.split())
        self.assertEquals('complex321.xml.gz', args.complex_xml_file)

    def test_file_parameter_is_mandatory(self):
        real_stderr = sys.stderr
        fake_stderr = StringIO.StringIO()
        try:
            sys.stderr = fake_stderr
            with self.assertRaises(SystemExit):
                module.parse_args(''.split())
        finally:
            sys.stderr = real_stderr

        output = fake_stderr.getvalue()
        self.failUnless('too few arguments' in output)
        self.failUnless('usage' in output)

    def test_optional_maxrecords_argument_defaults_to_all(self):
        args = module.parse_args('complex421.xml.gz'.split())
        self.assertEquals(module.Handle_ceg.ALL_RECORDS, args.maxrecords)
