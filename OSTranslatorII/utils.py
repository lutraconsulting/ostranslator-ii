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
import re
import os, urllib2
import psycopg2
from PyQt4.QtCore import QSettings
from PyQt4.QtNetwork import QNetworkRequest
from PyQt4.QtCore import QUrl, QEventLoop
import tempfile

def OSII_icon_path():
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "images", "icon.png")


def download(packageUrl, destinationFileName):
    handle = tempfile.NamedTemporaryFile(delete=False, suffix=destinationFileName)
    name = handle.name

    try:
        from qgis.core import QgsNetworkAccessManager
        _download_qgis(packageUrl, handle)

    except ImportError:
        # in case we are using cli and qgis is not installed
        _download_urllib2(packageUrl, handle)
    except Exception as err:
        # in case we are using cli and qgis is not installed
        _download_urllib2(packageUrl, handle)

    handle.close()
    return name

def delete(fileName):
    if os.path.isfile(fileName):
        os.unlink(fileName)

def _download_qgis(packageUrl, handle):
    from qgis.core import QgsNetworkAccessManager

    request = QNetworkRequest(QUrl(packageUrl))
    reply = QgsNetworkAccessManager.instance().get(request)
    evloop = QEventLoop()
    reply.finished.connect(evloop.quit)
    evloop.exec_(QEventLoop.ExcludeUserInputEvents)
    content_type = reply.rawHeader('Content-Type')
    if bytearray(content_type) == bytearray('text/plain; charset=utf-8'):
        handle.write(bytearray(reply.readAll()))
    else:
        msg = 'Failed to download %s\n\nPlease check your QGIS network settings and authentication db' % (packageUrl)
        ret_code = reply.attribute(QNetworkRequest.HttpStatusCodeAttribute)
        if ret_code:
            msg += '\n\nThe HTTP status code was %d.' % (ret_code)
        raise Exception(msg)

def _download_urllib2(url, handle):
    s = QSettings()
    try:
        useProxy = s.value("proxy/proxyEnabled", False).toBool()
    except:
        useProxy = s.value("proxy/proxyEnabled", False, type=bool)
    if useProxy:
        proxyHost = s.value("proxy/proxyHost", unicode())
        proxyPassword = s.value("proxy/proxyPassword", unicode())
        proxyPort = s.value("proxy/proxyPort", unicode())
        proxyType = s.value("proxy/proxyType", unicode())
        proxyTypes = {'DefaultProxy': 'http', 'HttpProxy': 'http', 'Socks5Proxy': 'socks', 'HttpCachingProxy': 'http',
                      'FtpCachingProxy': 'ftp'}
        if proxyType in proxyTypes: proxyType = proxyTypes[proxyType]
        proxyUser = s.value("proxy/proxyUser", unicode())
        proxyString = 'http://' + proxyUser + ':' + proxyPassword + '@' + proxyHost + ':' + proxyPort
        proxy = urllib2.ProxyHandler({proxyType: proxyString})
        auth = urllib2.HTTPBasicAuthHandler()
        opener = urllib2.build_opener(proxy, auth, urllib2.HTTPHandler)
        urllib2.install_opener(opener)

    generalDownloadFailureMessage = '\n\nPlease check your network settings. Styles may need to be added manually. This does not affect the loaded data.'
    try:
        conn = urllib2.urlopen(url, timeout=30)  # Allow 30 seconds for completion
        if conn.getcode() != 200:
            # It looks like the request was unsuccessful
            raise Exception('Failed to download %s\n\nThe HTTP status code was %d.' % (
            url, conn.getcode()) + generalDownloadFailureMessage)
    except urllib2.URLError as e:
        raise Exception('Failed to download %s\n\nThe reason was %s.' % (
        url, e.reason) + generalDownloadFailureMessage)

    handle.write(conn.read())


def get_OSMM_schema_ver(s):
    """ Extract the schema version (as a string) from the file name"""
    supported_versions = dict()
    supported_versions['OS Mastermap Topography'] = ['7', '9']
    supported_versions['OS Mastermap ITN Urban Paths'] = ['8']
    supported_versions['OS Mastermap ITN'] = ['7', '9']
    supported_versions['OS Mastermap HN Roads and RAMI'] = ['2.2']
    supported_versions['OS Mastermap HN Paths'] = ['2.2']
    # match = re.search(pattern=r'''.*\(v(\d)\).*''', string=s)
    match = re.search(pattern=r'''(.*)\(v(\d+(\.\d+)*)\).*''', string=s)
    if match:
        try:
            dataset = match.group(1)  #0 is whole string
            sch = match.group(2)
        except Exception:
            raise Exception("Unable to parse OSMM schema version from {}".format(s))

        if sch not in supported_versions[dataset]:
            raise Exception("Unsupported OSMM schema {}".format(sch))

        return sch
    else:
        raise Exception("Unable to parse OSMM schema version from {}".format(s))


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


def build_args(input_files, gfs_file_path, pg_source, ignore_fid):
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

        if ignore_fid:
            # fixes ERROR 1: COPY statement failed. ERROR: null value in column "fid" violates not-null
            # https://github.com/lutraconsulting/ostranslator-ii/issues/18
            args.extend(['--config', 'GML_EXPOSE_FID', 'NO'])

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
