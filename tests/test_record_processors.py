from unittest import TestCase
import mock
from complex_xml_to_csvs import record_processors as module
from collections import defaultdict
from complex_schema import Table, Field


class TestCountLimitingRecordProcessor(TestCase):

    def test_flush_is_passed_through(self):
        rp = module.RecordProcessor()
        rp.flush = mock.Mock(rp.flush)

        clrp = module.CountLimitingRecordProcessor(rp, maxrecords=1)
        clrp.flush()

        rp.flush.assert_called_once_with()

    def test_process_is_passed_through(self):
        rp = module.RecordProcessor()
        rp.process = mock.Mock(rp.process)

        clrp = module.CountLimitingRecordProcessor(rp, maxrecords=2)
        clrp.process(mock.sentinel.document)

        rp.process.assert_called_once_with(mock.sentinel.document)

    def test_process_raises_exception_after_the_maxrecord_th_call(self):
        rp = module.RecordProcessor()
        rp.process = mock.Mock(rp.process)

        clrp = module.CountLimitingRecordProcessor(rp, maxrecords=1)
        with self.assertRaises(module.RequiredNumberOfRecordsRead):
            clrp.process(mock.sentinel.document)

        # the underlying record_processor is called before the exception
        rp.process.assert_called_once_with(mock.sentinel.document)


class TestBatchMakerRecordProcessor(TestCase):

    def test_process_small_batch_adds_document_to_batch(self):
        bm = module.BatchMakerRecordProcessor(
            batch_size=3,
            batch_processor=None
        )
        bm.process(mock.sentinel.doc1)
        bm.process(mock.sentinel.doc2)

        self.assertEquals([mock.sentinel.doc1, mock.sentinel.doc2], bm.batch)

    def test_process_batch_size_reaches_limit_flush_called(self):
        bm = module.BatchMakerRecordProcessor(
            batch_size=2,
            batch_processor=None
        )
        bm.flush = mock.Mock(bm.flush)

        bm.process(mock.sentinel.doc1)
        bm.process(mock.sentinel.doc2)

        bm.flush.assert_called_once_with()

    def test_flush_calls_batch_processor(self):
        bp = module.BatchProcessor()
        bp.process = mock.Mock(bp.process)

        bm = module.BatchMakerRecordProcessor(
            batch_size=2,
            batch_processor=bp
        )
        bm.process(mock.sentinel.doc1)
        bm.process(mock.sentinel.doc2)

        bp.process.assert_called_once_with(
            [mock.sentinel.doc1, mock.sentinel.doc2])

    def test_flush_starts_new_batch(self):
        bp = module.BatchProcessor()

        bm = module.BatchMakerRecordProcessor(
            batch_size=2,
            batch_processor=bp
        )
        bm.process(mock.sentinel.doc1)
        bm.process(mock.sentinel.doc2)

        self.assertEquals([], bm.batch)


class StringIO(object):

    def __init__(self):
        self.__content = []

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        pass

    def write(self, data):
        self.__content.append(str(data))

    @property
    def content(self):
        return u''.join(self.__content)


class XCsvSplitter(module.CsvSplitter):

    def __init__(self, input_fname, output_dir, tables):
        super(XCsvSplitter, self).__init__(input_fname, output_dir, tables)
        self.fs = defaultdict(StringIO)

    def batch_csv_file(self, rovat):
        return self.fs[self.batch_csv_name(rovat)]


class TestCsvSplitter(TestCase):

    def table_a(self):
        table = Table('rovat_a', 'test table a')
        table.add(Field('a', 'a', 11, 'char'))
        return table

    def table_b(self):
        table = Table('rovat_b', 'test table b')
        table.add(Field('b', 'b', 11, 'char'))
        return table

    def setUp(self):
        tables = [self.table_a(), self.table_b()]
        self.csv_splitter = XCsvSplitter(
            'ixput_fname.xml.gz',
            'oxtput_dir',
            tables
        )

    @property
    def fs(self):
        return self.csv_splitter.fs

    def test_process_spreads_records(self):
        batch = [
            {
                'ceg_id': '110011001100',
                'a': [
                    {'alrovat_id': 9, 'a': 2},
                ],
                'b': [
                    {'alrovat_id': 1, 'b': 0},
                    {'alrovat_id': 2, 'b': 'b'},
                ],
            },
        ]

        self.csv_splitter.process(batch)

        # only two files are created
        self.assertSetEqual(
            {
                'oxtput_dir/rovat_a/ixput_fname_0000.csv',
                'oxtput_dir/rovat_b/ixput_fname_0000.csv',
            },
            set(self.fs.keys())
        )

        # their content is
        self.assertEqual(
            [
                u'ceg_id,alrovat_id,a',
                u'110011001100,9,2'
            ],
            self.fs['oxtput_dir/rovat_a/ixput_fname_0000.csv']
            .content.splitlines()
        )
        self.assertEqual(
            [
                u'ceg_id,alrovat_id,b',
                u'110011001100,1,0',
                u'110011001100,2,b'
            ],
            self.fs['oxtput_dir/rovat_b/ixput_fname_0000.csv']
            .content.splitlines()
        )

    def test_process_increments_batch_number(self):
        old_number = self.csv_splitter.batch_number

        self.csv_splitter.process({})

        self.assertLess(old_number, self.csv_splitter.batch_number)

    def test_process_can_be_called_many_times(self):
        batch = [
            {
                'ceg_id': '1',
                'a': [
                    {'alrovat_id': 1, 'a': 'a1'},
                    {'alrovat_id': 2, 'a': 'a11'},
                ],
            },
            {
                'ceg_id': '2',
                'a': [
                    {'alrovat_id': 4, 'a': 'a2'},
                    {'alrovat_id': 0, 'a': 'a22'},
                ],
            },
        ]

        self.csv_splitter.process({})
        self.csv_splitter.process(batch)
        self.csv_splitter.process({})
        self.csv_splitter.process(batch)

        # only two files are created
        self.assertSetEqual(
            {
                'oxtput_dir/rovat_a/ixput_fname_0001.csv',
                'oxtput_dir/rovat_a/ixput_fname_0003.csv',
            },
            set(self.fs.keys())
        )

        # their content is
        self.assertEqual(
            self.fs['oxtput_dir/rovat_a/ixput_fname_0001.csv'].content,
            self.fs['oxtput_dir/rovat_a/ixput_fname_0003.csv'].content
        )
        self.assertEqual(
            [
                u'ceg_id,alrovat_id,a',
                u'1,1,a1',
                u'1,2,a11',
                u'2,4,a2',
                u'2,0,a22',
            ],
            self.fs['oxtput_dir/rovat_a/ixput_fname_0001.csv']
            .content.splitlines()
        )
