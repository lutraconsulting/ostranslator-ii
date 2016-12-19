# -*- coding: utf-8 -*-
"""
/***************************************************************************
 OsTranslatorIIDialog
                                 A QGIS plugin
 A plugin for loading Ordnance Survey MasterMap and other GML-based datasets.
                             -------------------
        begin                : 2014-10-03
        git sha              : $Format:%H$
        copyright            : (C) 2014 by Peter Wells for Lutra Consulting
        email                : info@lutraconsulting.co.uk
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import traceback
import sys
import argparse
import os
from PyQt4 import QtCore
from import_manager import ImportManager
from utils import build_args, get_input_files, get_pioneer_file, get_supported_datasets, create_schema, get_db_cur


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
                 num_processes):
        super(OSTranslatorCli, self).__init__()

        self.input_path = input_path
        self.ds_name = osmm_data_type
        self.schema = schema
        self.num_processes = num_processes

        self.con_details = dict()
        self.con_details['database'] = database
        self.con_details['host'] = host
        self.con_details['port'] = port
        self.con_details['user'] = user
        self.con_details['password'] = None  # Retrieve it from PGPASSFILE instead

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
                    print 'Failed to create schema %s' % schema
                    self.quit(1)
                    return

            pg_source = 'PG:dbname=\'%s\' host=\'%s\' port=\'%d\' active_schema=%s user=\'%s\'' % \
                        (self.con_details['database'], self.con_details['host'], self.con_details['port'],
                         self.schema + '_tmp', self.con_details['user'])

            for arg in build_args(input_files, gfs_file_path, pg_source):
                self.im.add(arg)
            self.im.start(num_processes)
        except:
            print
            print 'Translation failed:'
            print
            print '%s\n\n' % traceback.format_exc()
            print
            self.quit(1)
            return

    def quit(self, ret_code):
        self.finished.emit(ret_code)

    def post_process(self):
        self.on_post_process_complete()

    def on_post_process_complete(self):
        ret_val = 0
        if len(self.im.crashedJobs) > 0 or len(self.im.failedJobs) > 0:
            ret_val = 1
        # Write out summary information
        print self.im.getImportReport()
        self.quit(ret_val)  # Everything went well, return 0

    def on_progress_changed(self, progress):
        if progress != self.progress:
            self.progress = progress
            if progress % 10 == 0:
                print '%d %%' % progress


def main():

    supported_data_types = ['OS Mastermap Topography (v7)']

    parser = argparse.ArgumentParser()
    parser.add_argument('--osmm-data-type', required=True,
                        help='OS MasterMap data type, e.g. "OS Mastermap Topography (v7)"')
    parser.add_argument('--input-path', required=True, help='Path under which to search of .gml.gz or .gml files')
    parser.add_argument('--host', default='localhost', help='Hostname of PostgreSQL server (default=localhost)')
    parser.add_argument('--database', required=True, help='Destination database for import')
    parser.add_argument('--port', default=5432, help='Port of PostgreSQL server (default=5432)')
    parser.add_argument('--user', default='postgres', help='Username to connect as (default=postgres)')
    parser.add_argument('--schema', required=True, help='Destination schema for import')
    parser.add_argument('--num-processes', default=2, help='Number of concurrent import processes (default=2)')

    args = parser.parse_args()
    if args.osmm_data_type not in supported_data_types:
        print '%s is not a supported data type.'
        print 'Supported data types are:'
        for sup in supported_data_types:
            print '  %s' % sup
        sys.exit(1)

    if not os.path.isdir(args.input_path):
        print '%s doesn\'t appear to be a folder'
        sys.exit(1)

    app = QtCore.QCoreApplication(sys.argv)
    o = OSTranslatorCli(input_path=args.input_path,
                        osmm_data_type=args.osmm_data_type,
                        host=args.host,
                        database=args.database,
                        port=args.port,
                        user=args.user,
                        schema=args.schema,
                        num_processes=args.num_processes)

    o.finished.connect(app.exit)

    QtCore.QTimer.singleShot(10, o.run)
    sys.exit(app.exec_())  # FIXME: For some reason the app returns -1 instead of 0 as expected


if __name__ == '__main__':
    main()
