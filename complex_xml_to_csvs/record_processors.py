import os
import xml.sax
import unicodecsv

import logging


log = logging.getLogger(__name__)


class RequiredNumberOfRecordsRead(xml.sax.SAXException):
    pass


class RecordProcessor(object):

    def process(self, document):
        pass

    def flush(self):
        pass


class CountLimitingRecordProcessor(RecordProcessor):

    def __init__(self, record_processor, maxrecords):
        self.record_processor = record_processor
        self.record_count = 0
        self.maxrecords = maxrecords

    def process(self, document):
        self.record_processor.process(document)
        self.record_count += 1
        if self.record_count == self.maxrecords:
            raise RequiredNumberOfRecordsRead(
                '{0} required records processed'.format(self.record_count)
            )

    def flush(self):
        self.record_processor.flush()


class BatchMakerRecordProcessor(RecordProcessor):

    def __init__(self, batch_size, batch_processor):
        self.batch_processor = batch_processor
        self.batch = []
        self.batch_size = batch_size

    def process(self, document):
        self.batch.append(document)
        if len(self.batch) == self.batch_size:
            self.flush()

    def flush(self):
        log.debug('<<flushing>>')
        self.batch_processor.process(self.batch)
        self.batch = []


class BatchProcessor(object):

    def process(self, batch):
        pass


class CsvSplitter(BatchProcessor):

    def __init__(self, input_fname, output_dir, tables):
        self.rows_per_tables = {}
        self.rovat_to_table = {
            table.name: table
            for table in tables
        }
        self.batch_number = 0
        self.base_fname = (
            os.path.basename(input_fname)
            .replace('.xml', '')
            .replace('.gz', '')
        )
        self.output_dir = output_dir

    @property
    def tables(self):
        return self.rows_per_tables.keys()

    def spread_record(self, js):
        keys = set(js.keys())
        ceg_id = js['ceg_id']
        keys.remove('ceg_id')
        for rovat in keys:
            rows = self.rows_per_tables.get(rovat, [])
            self.rows_per_tables[rovat] = rows
            for alrovat in js[rovat]:
                rows.append(dict(alrovat, ceg_id=ceg_id))

    def get_table_name(self, rovat):
        if rovat.startswith('0') and rovat != '0':
            rovat = rovat.lstrip('0')
        return 'rovat_{}'.format(rovat)

    def get_fields(self, rovat):
        table = self.rovat_to_table[self.get_table_name(rovat)]
        return [field.name for field in table.fields]

    def batch_csv_name(self, rovat):
        return (
            '{output_dir}/{table}/{base_fname}_{batch_number:04d}.csv'
            .format(
                output_dir=self.output_dir,
                table=self.get_table_name(rovat),
                base_fname=self.base_fname,
                batch_number=self.batch_number,
            )
        )

    def batch_csv_file(self, rovat):
        batch_csv_name = self.batch_csv_name(rovat)

        assert not os.path.exists(batch_csv_name)
        return open(batch_csv_name, 'w')

    def flush_table(self, rovat):
        log.debug('CsvSplitter.flush_table START: %s', rovat)
        table_fields = self.get_fields(rovat)
        rows = self.rows_per_tables[rovat]

        with self.batch_csv_file(rovat) as f:
            writer = unicodecsv.DictWriter(f, table_fields)
            writer.writeheader()
            try:
                writer.writerows(rows)
            except:
                log.exception(
                    '%s: rovat_%s batch #%s',
                    self.base_fname,
                    rovat,
                    self.batch_number
                )
                log.info(
                    'CEG_ID %s - %s',
                    rows[0]['ceg_id'],
                    rows[-1]['ceg_id']
                )
                raise
        log.debug('CsvSplitter.flush_table END: %s', rovat)

    def process(self, batch):
        log.debug('CsvSplitter.process START')
        for record in batch:
            self.spread_record(record)

        for table in self.tables:
            self.flush_table(table)

        self.rows_per_tables = {}
        self.batch_number += 1
        log.debug('CsvSplitter.process END')
