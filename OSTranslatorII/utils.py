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

import gdal
import os
import psycopg2

def OSII_icon_path():
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "images", "icon.png")

def get_supported_datasets():
    """ Read the content of the gfs folder """
    ds = dict()
    this_dir = os.path.dirname(__file__)
    gfs_path = os.path.join(this_dir, 'gfs')
    for entry in os.listdir(gfs_path):
        entry_path = os.path.join(gfs_path, entry)
        head, tail = os.path.splitext(entry)
        if tail == '.gfs' and os.path.isfile(entry_path):
            ds[head] = entry_path
    return ds


def get_pioneer_file(ds_name):
    gfs_file_name = get_supported_datasets()[ds_name]
    head, tail = os.path.splitext(gfs_file_name)
    pioneer_file_path = head + ' Pioneer.gz'
    return pioneer_file_path


def build_args(input_files, gfs_file_path, pg_source):
    i = 0
    all_args = []
    for input_file in input_files:
        if input_file.lower().endswith('.gz'):
            input_file = '/vsigzip/' + input_file
        args = ['-f', 'PostgreSQL',
                '--config', 'PG_USE_COPY', 'YES',
                '--config', 'GML_GFS_TEMPLATE', gfs_file_path,
                pg_source, input_file]
        if str(gdal.VersionInfo()).startswith('2'):
            # -lyr_transaction added to negate
            # ERROR 1: ERROR: current transaction is aborted, commands ignored until end of transaction block
            #
            # ERROR 1: no COPY in progress
            args.insert(0, '-lyr_transaction')
        if i == 0:
            args.insert(0, '-overwrite')
            args.extend(['-lco', 'OVERWRITE=YES',
                         '-lco', 'SPATIAL_INDEX=OFF',
                         '-lco', 'PRECISION=NO'])
        else:
            args.insert(0, '-append')

        i += 1
        all_args.append(args)
    return all_args


def get_input_files(input_dir):
    input_files = []
    if not os.path.isdir(input_dir):
        raise Exception('%s is not a valid folder.' % input_dir)
    for path, dirs, files in os.walk(input_dir):
        for f in files:
            if f.lower().endswith('.gml') or f.lower().endswith('.gz'):
                input_files.append(os.path.join(path, f))
    return input_files


def create_schema(cur, schema_name):
    q_dic = {'schema_name': schema_name}
    try:
        cur.execute("""CREATE SCHEMA IF NOT EXISTS """ + schema_name, q_dic)
    except psycopg2.ProgrammingError:
        cur.execute("""CREATE SCHEMA """ + schema_name, q_dic)
    if cur.statusmessage != 'CREATE SCHEMA':
        return False
    return True


def get_db_cur(con_details):
    if len(con_details['user']) == 0:
        db_conn = psycopg2.connect(database=con_details['database'])
    else:
        db_conn = psycopg2.connect(database=con_details['database'],
                                   user=con_details['user'],
                                   password=con_details['password'],
                                   host=con_details['host'],
                                   port=con_details['port'])
    db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return db_conn.cursor()
