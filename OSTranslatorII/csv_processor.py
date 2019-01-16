__author__ = 'Pete'


class CsvProcessor():

    """
    A base class for importing OS CSV-based datasets (e.g. AddressBase Premium)

    """

    def __init__(self, parent, **kwargs):
        # self.parent should should provide a method called getDbCur() which returns a psycopg2 db cursor. Each cursor returned
        # should be on a seperate DB connection.
        self.parent = parent
        self.concurrent_jobs = kwargs.get('concurrent_jobs', 1)
        self.dst_schema = kwargs.get('dest_schema', None)
        self.input_file_paths = kwargs.get('input_files', list())  # Absolute paths to CSV files
        if not hasattr(parent, 'getDbCur'):
            raise Exception('parent must provide getDbCur method')
        if self.dst_schema is None:
            raise Exception('dest_schema must be specified')  # TODO: Ensure the parent has checked that this schema exists and that we have appropriate permissions on it


    def prepare(self):
        """
        Defined in subclass. Typically used to create database tables before importing data.
        :return:
        """
        pass

    def process(self):
        """
        Defined in subclass. Typically used to import data using COPY.
        :return:
        """
        pass

    def post_process(self):
        """
        Defined in subclass. Typically used to create indices.
        :return:
        """
        pass




