__author__ = 'Pete'

import os, threading, psycopg2

from csv_processor import CsvProcessor


def copy_in(cur, stdin, table):
        """

        :param stdin:   A file-like object, most likely the read-side of a pipe object
        :return:
        """
        cur.copy_expert("""COPY public.""" + table + """ FROM STDIN (FORMAT csv)""", stdin)


class AddressBasePremiumProcessor(CsvProcessor):

    def __init__(self):
        CsvProcessor.__init__(self)

        self.identifiers = {
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

    def get_cur(self):
        dbConn = psycopg2.connect( database = 'training',
                                   user = 'postgres',
                                   password = 'postgres',
                                   host = 'localhost',
                                   port = 5432)
        dbConn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return dbConn.cursor()

    def prepare(self, schema):
        self.prepare_streets(schema)
        self.prepare_street_descriptors(schema)

        self.prepare_lpis(schema)

        self.prepare_blpus(schema)
        self.prepare_organisations(schema)
        self.prepare_successor_cross_references(schema)
        self.prepare_application_cross_references(schema)
        self.prepare_classifications(schema)
        self.prepare_delivery_point_addresses(schema)

        self.prepare_headers(schema)
        self.prepare_trailers(schema)
        self.prepare_metadata(schema)

    def prepare_streets(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_streets""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_streets (
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
                           --"STREET_START_LAT" double precision NOT NULL,
                           --"STREET_START_LONG" double precision NOT NULL,
                           "STREET_END_X" double precision NOT NULL,
                           "STREET_END_Y" double precision NOT NULL,
                           --"STREET_END_LAT" double precision NOT NULL,
                           --"STREET_END_LONG" double precision NOT NULL,
                           "STREET_TOLERANCE" smallint NOT NULL,
                           CONSTRAINT ab_prem_streets_pkey PRIMARY KEY ("USRN")
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_street_descriptors(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_street_descriptors""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_street_descriptors (
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

    def prepare_lpis(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_lpis""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_lpis (
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

    def prepare_blpus(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_blpus""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_blpus (
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

    def prepare_organisations(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_organisations""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_organisations (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "CHANGE_TYPE" character varying(1) NOT NULL,
                           "PRO_ORDER" bigint NOT NULL,
                           "UPRN" bigint NOT NULL,
                           -- FIXME:  Check through OS docs for primary keys
                           "ORG_KEY" bigint NOT NULL,
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

    def prepare_classifications(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_classifications""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_classifications (
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

    def prepare_headers(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_headers""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_headers (
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

    def prepare_trailers(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_trailers""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_trailers (
                           "RECORD_IDENTIFIER" smallint NOT NULL,
                           "NEXT_VOLUME_NAME" smallint NOT NULL,
                           "RECORD_COUNT" bigint NOT NULL,
                           "ENTRY_DATE" date NOT NULL,
                           "TIME_STAMP" time NOT NULL
                       )
                       WITH (
                           OIDS=FALSE
                       )""", dict())

    def prepare_metadata(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_metadata""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_metadata (
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

    def prepare_successor_cross_references(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_successor_cross_references""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_successor_cross_references (
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

    def prepare_application_cross_references(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_application_cross_references""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_application_cross_references (
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

    def prepare_delivery_point_addresses(self, schema):
        cur = self.get_cur()
        cur.execute("""DROP TABLE IF EXISTS """ + schema + """.ab_prem_delivery_point_addresses""")
        cur.execute("""CREATE TABLE """ + schema + """.ab_prem_delivery_point_addresses (
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

        input_file = open(r'C:\tmp\stockton\SX9090.csv', 'r')
        output_files = {}
        for key, table in self.identifiers.iteritems():
            p_r, p_w = os.pipe()
            output_files[key] = {
                'read_pipe': p_r,
                'write_pipe': p_w,
                'read_f': os.fdopen(p_r),
                'write_f': os.fdopen(p_w, 'w')
            }
            cur = self.get_cur()
            output_files[key]['copy_thread'] = threading.Thread(target=copy_in, args=(cur, output_files[key]['read_f'], table))
            output_files[key]['copy_thread'].start()

        for line in input_file:
            # TODO: Optimise the ordering of this if/elif
            if line.startswith('10,'):
                continue  # FIXME
                output_files[10]['write_f'].write(line)
            elif line.startswith('11,'):
                output_files[11]['write_f'].write(line)
            elif line.startswith('15,'):
                continue  # FIXME
                output_files[15]['write_f'].write(line)
            elif line.startswith('21,'):
                continue  # FIXME
                output_files[21]['write_f'].write(line)
            elif line.startswith('23,'):
                continue  # FIXME
                output_files[23]['write_f'].write(line)
            elif line.startswith('24,'):
                continue  # FIXME
                output_files[24]['write_f'].write(line)
            elif line.startswith('28,'):
                continue  # FIXME
                output_files[28]['write_f'].write(line)
            elif line.startswith('29,'):
                continue  # FIXME
                output_files[29]['write_f'].write(line)
            elif line.startswith('30,'):
                continue  # FIXME
                output_files[30]['write_f'].write(line)
            elif line.startswith('31,'):
                continue  # FIXME
                output_files[31]['write_f'].write(line)
            elif line.startswith('32,'):
                continue  # FIXME
                output_files[32]['write_f'].write(line)
            elif line.startswith('99,'):
                continue  # FIXME
                output_files[99]['write_f'].write(line)

        for key in self.identifiers.keys():
            # The following may block if the COPY is still pushing data into the DB
            output_files[key]['write_f'].close()
            output_files[key]['copy_thread'].join()

        input_file.close()

def main():
    p = AddressBasePremiumProcessor()
    p.prepare('public')
    p.process()

if __name__ == '__main__':
    main()