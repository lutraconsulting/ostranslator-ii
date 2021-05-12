__author__ = 'Pete'

import os, threading

from csv_processor import CsvProcessor


def copy_in(cur, stdin, schema, table):
        """

        :param stdin:   A file-like object, most likely the read-side of a pipe object
        :return:
        """
        cur.copy_expert("""COPY """ + schema + """.""" + table + """ FROM STDIN (FORMAT csv)""", stdin)


class AddressBasePremiumProcessor(CsvProcessor):

    """

    Class for importing Address Base Premium

    Typical input looks like this:

    10,"NAG Hub - GeoPlace",9999,2014-06-07,1,2014-06-07,09:01:38,"1.0","F"
    11,"I",1,14200759,1,1110,2,1990-01-01,1,8,0,2003-07-02,,2007-07-19,2003-07-02,292906.00,093301.00,292968.00,093238.00,10
    15,"I",2,14200759,"SILVER LANE","","EXETER","DEVON","ENG"
    11,"I",3,14200769,1,1110,2,1990-01-01,1,8,0,2003-07-02,,2007-07-19,2003-07-02,292774.00,093582.00,292694.00,093519.00,10

    """

    def __init__(self, parent, **kwargs):
        CsvProcessor.__init__(self, parent, **kwargs)

        # self.dst_tables is a dictionary based on the numerical identifiers used in OS CSV-based datasets. Keys are the
        # integer identifiers, values are the names of the destination tables.

        self.dst_tables = {
            10: 'ab_prem_headers',
            11: 'ab_prem_streets',  # Street
            15: 'ab_prem_street_descriptors',
            21: 'ab_prem_blpus',
            23: 'ab_prem_application_cross_references',
            24: 'ab_prem_lpis',
            28: 'ab_prem_delivery_point_addresses',
            29: 'ab_prem_metadata',
            30: 'ab_prem_successor_cross_references',
            31: 'ab_prem_organisations',
            32: 'ab_prem_classifications',
            99: 'ab_prem_trailers'
        }

    def prepare(self):
        # Create or replace the required tables
        self.prepare_application_cross_references()
        self.prepare_blpus()
        self.prepare_classifications()
        self.prepare_delivery_point_addresses()
        self.prepare_headers()
        self.prepare_lpis()
        self.prepare_metadata()
        self.prepare_organisations()
        self.prepare_successor_cross_references()
        self.prepare_street_descriptors()
        self.prepare_streets()
        self.prepare_trailers()

    def prepare_streets(self):
        cur = self.parent.getDbCur()
        # TODO: need to warn the user the dataset will be overwritten
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_streets""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_streets (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "USRN" integer NOT NULL,
                           "RECORD_TYPE" smallint NOT NULL,
                           "SWA_ORG_REF_NAMING" smallint NOT NULL,
                           "STATE" smallint,
                           "STATE_DATE" date,
                           "STREET_SURFACE" smallint,
                           "STREET_CLASSIFICATION" smallint,
                           "VERSION" smallint NOT NULL,
                           "STREET_START_DATE" date NOT NULL,
                           "STREET_END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "RECORD_ENTRY_DATE" date NOT NULL,
                           "STREET_START_X" double precision NOT NULL,
                           "STREET_START_Y" double precision NOT NULL,
                           -- These lat/long entries are in the standard but oddly not in the data - another mistake in the
                           -- tech docs?
                           "STREET_START_LAT" double precision NOT NULL,
                           "STREET_START_LONG" double precision NOT NULL,
                           "STREET_END_X" double precision NOT NULL,
                           "STREET_END_Y" double precision NOT NULL,
                           "STREET_END_LAT" double precision NOT NULL,
                           "STREET_END_LONG" double precision NOT NULL,
                           "STREET_TOLERANCE" smallint NOT NULL,
                           CONSTRAINT ab_prem_streets_pkey PRIMARY KEY ("USRN")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_street_descriptors(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_street_descriptors""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_street_descriptors (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "USRN" integer NOT NULL,
                           "STREET_DESCRIPTION" character varying(100) NOT NULL,
                           "LOCALITY" character varying(35),
                           "TOWN_NAME" character varying(30),
                           "ADMINSTRATIVE_AREA" character varying(30) NOT NULL,
                           "LANGUAGE" character varying(3) NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_lpis(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_lpis""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_lpis (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           "LPI_KEY" character varying(14) NOT NULL,
                           "LANGUAGE" character varying(3) NOT NULL,
                           "LOGICAL_STATUS" smallint NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           "SAO_START_NUMBER" smallint,
                           "SAO_START_SUFFIX" character varying(2),
                           "SAO_END_NUMBER" smallint,
                           "SAO_END_SUFFIX" character varying(2),
                           "SAO_TEXT" character varying(90),
                           "PAO_START_NUMBER" smallint,
                           "PAO_START_SUFFIX" character varying(2),
                           "PAO_END_NUMBER" smallint,
                           "PAO_END_SUFFIX" character varying(2),
                           "PAO_TEXT" character varying(90),
                           "USRN" integer NOT NULL,
                           "USRN_MATCH_INDICATOR" smallint NOT NULL,
                           "AREA_NAME" character varying(40),
                           "LEVEL" character varying(30),
                           "OFFICIAL_FLAG" character varying(1),
                           CONSTRAINT ab_prem_lpis_pkey PRIMARY KEY ("LPI_KEY")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_blpus(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_blpus""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_blpus (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           "LOGICAL_STATUS" smallint NOT NULL,
                           "BLPU_STATE" smallint,
                           "BLPU_STATE_DATE" date,
                           "PARENT_UPRN" bigint,
                           "X_COORDINATE" double precision NOT NULL,
                           "Y_COORDINATE" double precision NOT NULL,
                           "LATITUDE" double precision NOT NULL,
                           "LONGITUDE" double precision NOT NULL,
                           "RPC" smallint NOT NULL,
                           "LOCAL_CUSTODIAN_CODE" smallint NOT NULL,
                           "COUNTRY" character varying(1) NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           "ADDRESSBASE_POSTAL" character varying(1) NOT NULL,
                           "POSTCODE_LOCATOR" character varying(8) NOT NULL,
                           "MULTI_OCC_COUNT" smallint NOT NULL,
                           CONSTRAINT ab_prem_blpus_pkey PRIMARY KEY ("UPRN")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_organisations(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_organisations""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_organisations (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           -- FIXME:  Check through OS docs for primary keys
                           "ORG_KEY" character varying(14) NOT NULL,
                           "ORGANISATION" character varying(100) NOT NULL,
                           "LEGAL_NAME" character varying(60),
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date,
                           CONSTRAINT ab_prem_organisations_pkey PRIMARY KEY ("ORG_KEY")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_classifications(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_classifications""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_classifications (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           -- FIXME:  Check through OS docs for primary keys
                           "CLASS_KEY" character varying(14) NOT NULL,
                           "CLASSIFICATION_CODE" character varying(6) NOT NULL,
                           "CLASS_SCHEME" character varying(60) NOT NULL,
                           "SCHEME_VERSION" float NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           CONSTRAINT ab_prem_classifications_pkey PRIMARY KEY ("CLASS_KEY")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_headers(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_headers""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_headers (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CUSTODIAN_NAME" character varying(40) NOT NULL,
                           "LOCAL_CUSTODIAN_CODE" smallint NOT NULL,
                           "PROCESS_DATE" date NOT NULL,
                           "VOLUME_NUMBER" smallint NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           "TIME_STAMP" time NOT NULL,
                           "VERSION" character varying(7) NOT NULL,
                           "FILE_TYPE" character varying(1) NOT NULL
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_trailers(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_trailers""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_trailers (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "NEXT_VOLUME_NAME" smallint NOT NULL,
                           "RECORD_COUNT" bigint NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           "TIME_STAMP" time NOT NULL
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_metadata(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_metadata""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_metadata (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "GAZ_NAME" character varying(60) NOT NULL,
                           "GAZ_SCOPE" character varying(60) NOT NULL,
                           "TER_OF_USE" character varying(60) NOT NULL,
                           "LINKED_DATA" character varying(100) NOT NULL,
                           "GAZ_OWNER" character varying(15) NOT NULL,
                           "NGAZ_FREQ" character varying(1) NOT NULL,
                           "CUSTODIAN_NAME" character varying(40) NOT NULL,
                           "CUSTODIAN_UPRN" character varying(12) NOT NULL,
                           "LOCAL_CUSTODIAN_CODE" smallint NOT NULL,
                           "CO_ORD_SYSTEM" character varying(40) NOT NULL,
                           "CO_ORD_UNIT" character varying(10) NOT NULL,
                           "META_DATE" date NOT NULL,
                           "CLASS_SCHEME" character varying(60) NOT NULL,
                           "GAZ_DATE" date NOT NULL,
                           "LANGUAGE" character varying(3) NOT NULL,
                           "CHARACTER_SET" character varying(30) NOT NULL
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_successor_cross_references(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_successor_cross_references""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_successor_cross_references (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           "SUCC_KEY" character varying(14) NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           "SUCCESSOR" bigint NOT NULL,
                           CONSTRAINT ab_prem_successor_cross_references_pkey PRIMARY KEY ("SUCC_KEY")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_application_cross_references(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_application_cross_references""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_application_cross_references (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           "XREF_KEY" character varying(14) NOT NULL,
                           "CROSS_REFERENCE" character varying(50) NOT NULL,
                           "VERSION" smallint,
                           "SOURCE" character varying(6) NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           CONSTRAINT ab_prem_application_cross_references_pkey PRIMARY KEY ("XREF_KEY")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_delivery_point_addresses(self):
        cur = self.parent.getDbCur()
        cur.execute("""DROP TABLE IF EXISTS """ + self.dst_schema + """.ab_prem_delivery_point_addresses""")
        cur.execute("""CREATE TABLE """ + self.dst_schema + """.ab_prem_delivery_point_addresses (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           "UDPRN" integer NOT NULL,
                           "ORGANISATION_NAME" character varying(60),
                           "DEPARTMENT_NAME" character varying(60),
                           "SUB_BUILDING_NAME" character varying(30),
                           "BUILDING_NAME" character varying(50),
                           "BUILDING_NUMBER" smallint,
                           "DEPENDENT_THOROUGHFARE" character varying(80),
                           "THOROUGHFARE" character varying(80),
                           "DOUBLE_DEPENDENT_LOCALITY" character varying(35),
                           "DEPENDENT_LOCALITY" character varying(35),
                           "POST_TOWN" character varying(30) NOT NULL,
                           "POSTCODE" character varying(8) NOT NULL,
                           "POSTCODE_TYPE" character varying(1) NOT NULL,
                           "DELIVERY_POINT_SUFFIX" character varying(2) NOT NULL,
                           "WELSH_DEPENDENT_THOROUGHFARE" character varying(80),
                           "WELSH_THOROUGHFARE" character varying(80),
                           "WELSH_DOUBLE_DEPENDENT_LOCALITY" character varying(35),
                           "WELSH_DEPENDENT_LOCALITY" character varying(35),
                           "WELSH_POST_TOWN" character varying(30),
                           "PO_BOX_NUMBER" character varying(6),
                           "PROCESS_DATE" date NOT NULL,
                           "START_DATE" date NOT NULL,
                           "END_DATE" date,
                           "LAST_UPDATE_DATE" date NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           CONSTRAINT ab_prem_delivery_point_addresses_pkey PRIMARY KEY ("UDPRN")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def process(self):

        output_pipes = {}
        for table_identifier, table_name in self.dst_tables.items():
            p_r, p_w = os.pipe()  # Make a pipe (read and write ends)
            output_pipes[table_identifier] = {
                'read_pipe': p_r,
                'write_pipe': p_w,
                'read_f': os.fdopen(p_r, 'r', encoding=self.src_encoding),
                'write_f': os.fdopen(p_w, 'w', encoding=self.src_encoding),
                'write_count': 0
            }
            cur = self.parent.getDbCur()  # These cursors could be part of the same connection.
            output_pipes[table_identifier]['copy_thread'] = threading.Thread(target=copy_in,
                                                                             args=(cur,
                                                                                   output_pipes[table_identifier]['read_f'],
                                                                                   self.dst_schema,
                                                                                   table_name
                                                                                   )
                                                                             )
            output_pipes[table_identifier]['copy_thread'].start()

        for input_file_path in self.input_file_paths:
            input_file = open(input_file_path, 'r', encoding=self.src_encoding)
            channel = 0
            for line in input_file:
                if len(line) > 1:
                    # Then it's a real line and not just a blank line with \n on the end
                    channel = int(line[:2])
                    output_pipes[channel]['write_f'].write(line)
                    output_pipes[channel]['write_count'] += 1
            # We've now finished with the file, which should have ended with a blank line. If it does not, send a \n
            # down the last used channel
            if len(line) != 0:
                # There was no blank line at the end of the file
                output_pipes[channel]['write_f'].write('\n')
            input_file.close()

        for table_identifier in self.dst_tables.keys():
            # The following may block if the COPY is still pushing data into the DB
            output_pipes[table_identifier]['write_f'].close()
            output_pipes[table_identifier]['copy_thread'].join()

        print('Counts:')
        for k in output_pipes.keys():
            print('\t%d: %d' % (k, output_pipes[k]['write_count']))

def main():

    import psycopg2

    class dummy():

        def __init__(self):
            default_server = 'localhost'
            default_port = '5432'
            default_user = 'postgres'
            default_passwd = 'postgres'
            default_schema = 'os_ab_premium'
            default_db = 'abp'
            default_encoding = 'utf_8'

            print('Specify destination server (press ENTER for default)')
            self.server = input('(%s) >' % default_server)
            if self.server == '':
                self.server = default_server

            print('Specify server port (press ENTER for default)')
            self.port = input('(%s) >' % default_port)
            if self.port == '':
                self.port = int(default_port)

            print('Specify destination database (press ENTER for default)')
            self.db = input('(%s) >' % default_db)
            if self.db == '':
                self.db = default_db

            print('Specify (existing) destination schema (press ENTER for default)')
            self.schema = input('(%s) >' % default_schema)
            if self.schema == '':
                self.schema = default_schema

            print('Specify username (press ENTER for default)')
            self.user = input('(%s) >' % default_user)
            if self.user == '':
                self.user = default_user

            print('Specify password (press ENTER for default)')
            self.passwd = input('(%s) >' % default_passwd)
            if self.passwd == '':
                self.passwd = default_passwd

            self.source_folder = ''
            while not os.path.isdir(self.source_folder):
                print('Specify folder containing source .CSV files (all CSV files in this folder [but not below it] will be processed)')
                self.source_folder = input('>').strip('"')

            print('Specify source encoding (press ENTER for default)')
            self.encoding = input('(%s) >' % default_encoding)
            if self.encoding == '':
                self.encoding = default_encoding

            self.source_files = []
            for f in os.listdir(self.source_folder):
                if f.lower().endswith('.csv'):
                    self.source_files.append(os.path.join(self.source_folder, f))

        def getDbCur(self):
            # TODO: Pull this from the main plugin
            dbConn = psycopg2.connect( database = self.db,
                                       user = self.user,
                                       password = self.passwd,
                                       host = self.server,
                                       port = self.port)
            dbConn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            return dbConn.cursor()

    d = dummy()
    p = AddressBasePremiumProcessor(d,
                                    src_encoding=d.encoding,
                                    input_files=d.source_files,
                                    dest_schema=d.schema)
    p.prepare()
    p.process()

if __name__ == '__main__':
    main()
