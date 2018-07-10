# -*- coding: utf-8 -*-
# OsTranslatorII QGIS Plugin
#
# Copyright (C) 2017 Lutra Consulting
# info@lutraconsulting.co.uk
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# Fix ValueError: API 'QDate' has already been set to version 1
from __future__ import print_function
from __future__ import absolute_import
try:
    import qgis.PyQt
except ImportError:
    pass

import traceback
import sys
import argparse
import os
from qgis.PyQt import QtCore
from .import_manager import ImportManager
from .post_processor_thread import PostProcessorThread
from .utils import (
    build_args,
    get_input_files,
    get_pioneer_file,
    get_supported_datasets,
    create_schema,
    get_db_cur,
    get_OSMM_schema_ver
)


class OSTranslatorCli(QtCore.QObject):

    finished = QtCore.pyqtSignal(int)

    def __init__(self,
                 input_path,
                 osmm_data_type,
                 host,
                 database,
                 port,
                 user,
                 schema,
                 num_processes,
                 ignore_fid):

        super(OSTranslatorCli, self).__init__()

        self.input_path = input_path
        self.ds_name = osmm_data_type
        self.schema = schema
        self.num_processes = num_processes
        self.ignore_fid = ignore_fid

        self.con_details = dict()
        self.con_details['database'] = database
        self.con_details['host'] = host
        self.con_details['port'] = port
        self.con_details['user'] = user
        self.con_details['password'] = None  # Retrieve it from PGPASSFILE instead

        self.pp_thread = None
        self.pp_errors = []

        self.app = QtCore.QCoreApplication.instance()
        self.im = ImportManager()
        self.im.finished.connect(self.post_process)
        self.im.progressChanged.connect(self.on_progress_changed)

        self.progress = None

    def __del__(self):
        self.im.progressChanged.disconnect(self.on_progress_changed)
        self.im.finished.disconnect(self.post_process)

    def run(self):

        try:
            input_files = get_input_files(self.input_path)
            input_files.insert(0, get_pioneer_file(self.ds_name))

            gfs_file_path = get_supported_datasets()[self.ds_name]

            num_processes = 2

            cur = get_db_cur(self.con_details)

            for schema in [self.schema, self.schema + '_tmp']:
                if not create_schema(cur, schema):
                    # fix_print_with_import
                    print('Failed to create schema %s' % schema)
                    self.quit(1)
                    return

            pg_source = 'PG:dbname=\'%s\' host=\'%s\' port=\'%d\' active_schema=%s user=\'%s\'' % \
                        (self.con_details['database'], self.con_details['host'], self.con_details['port'],
                         self.schema + '_tmp', self.con_details['user'])

            for arg in build_args(input_files, gfs_file_path, pg_source, self.ignore_fid):
                self.im.add(arg)
            # fix_print_with_import
            print('Importing...')
            self.im.start(num_processes)
        except:
            print()
            # fix_print_with_import
            print('Translation failed:')
            print()
            # fix_print_with_import
            print('%s\n\n' % traceback.format_exc())
            print()
            self.quit(1)
            return

    def quit(self, ret_code):
        self.finished.emit(ret_code)

    def post_process(self):
        # fix_print_with_import
        print('Post-processing...')
        cur = get_db_cur(self.con_details)
        self.pp_thread = PostProcessorThread(
            cur=cur,
            uri=None,  # Uri
            schema=self.schema,
            tables=['boundaryline', 'cartographicsymbol', 'cartographictext', 'topographicarea', 'topographicline', 'topographicpoint'],
            osmm_schema=get_OSMM_schema_ver(self.ds_name),
            osmm_style_name=None,
            createSpatialIndex=True,  # createSpatialIndex
            dedup=True,  # removeDuplicates
            addTopoStyleColumns=True,  # addOsStylingFields
            applyDefaultStyle=False)  # applyDefaultOsStyle
        self.pp_thread.finished.connect(self.on_post_process_complete)
        self.pp_thread.error.connect(self.on_post_processor_error)
        self.pp_thread.progressChanged.connect(self.on_progress_changed)
        self.pp_thread.start()

    def on_post_processor_error(self, error):
        self.pp_errors.append(error)

    def on_post_process_complete(self):
        ret_val = 0
        if len(self.im.crashedJobs) > 0 or \
           len(self.im.failedJobs) > 0 or \
           len(self.pp_errors) > 0:
            ret_val = 1

        # Write out summary information
        print()
        # fix_print_with_import
        print(self.im.getImportReport())

        if len(self.pp_errors) > 0:
            print()
            # fix_print_with_import
            print('Failed to complete one or more post-processing tasks:')
            print()
            for pp_fail in self.pp_errors:
                # fix_print_with_import
                print(pp_fail)
                print()

        self.quit(ret_val)

    def on_progress_changed(self, progress):
        if progress != self.progress:
            self.progress = progress
            if progress % 10 == 0:
                # fix_print_with_import
                print('%d %%' % progress)


def main():

    supported_data_types = ['OS Mastermap Topography (v7)', 'OS Mastermap Topography (v9)']

    parser = argparse.ArgumentParser(description='Import OS products into PostGIS from the command-line.\n\n' +
                                     'Please note that this script needs a PGPASSFILE to function - see \n' +
                                     'https://www.postgresql.org/docs/9.5/static/libpq-pgpass.html')
    parser.add_argument('--osmm-data-type', required=True,
                        help='OS MasterMap data type, e.g. "OS Mastermap Topography (v7)"')
    parser.add_argument('--input-path', required=True, help='Path under which to search of .gml.gz or .gml files')
    parser.add_argument('--host', default='localhost', help='Hostname of PostgreSQL server (default=localhost)')
    parser.add_argument('--database', required=True, help='Destination database for import')
    parser.add_argument('--port', default=5432, help='Port of PostgreSQL server (default=5432)')
    parser.add_argument('--user', default='postgres', help='Username to connect as (default=postgres)')
    parser.add_argument('--schema', required=True, help='Destination schema for import')
    parser.add_argument('--num-processes', default=2, help='Number of concurrent import processes (default=2)')
    parser.add_argument('--ignore-fid', default=False, action='store_true', help='allow to import features for boundary tiles (use GML_EXPOSE_FID NO in ogr2ogr command)')

    args = parser.parse_args()
    if args.osmm_data_type not in supported_data_types:
        # fix_print_with_import
        print('%s is not a supported data type.')
        # fix_print_with_import
        print('Supported data types are:')
        for sup in supported_data_types:
            # fix_print_with_import
            print('  %s' % sup)
        sys.exit(1)

    if not os.path.isdir(args.input_path):
        # fix_print_with_import
        print('%s doesn\'t appear to be a folder')
        sys.exit(1)

    app = QtCore.QCoreApplication(sys.argv)
    o = OSTranslatorCli(input_path=args.input_path,
                        osmm_data_type=args.osmm_data_type,
                        host=args.host,
                        database=args.database,
                        port=args.port,
                        user=args.user,
                        schema=args.schema,
                        num_processes=args.num_processes,
                        ignore_fid=args.ignore_fid)

    o.finished.connect(app.exit)

    QtCore.QTimer.singleShot(10, o.run)
    sys.exit(app.exec_())  # FIXME: For some reason the app returns -1 instead of 0 as expected


if __name__ == '__main__':
    main()
