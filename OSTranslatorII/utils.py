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

from future import standard_library
from qgis._core import QgsApplication, QgsAuthMethodConfig

standard_library.install_aliases()
from builtins import str
import gdal
import re
import os, urllib.request, urllib.error, urllib.parse
import psycopg2
from qgis.PyQt.QtCore import QSettings
from qgis.PyQt.QtNetwork import QNetworkRequest
from qgis.PyQt.QtCore import QUrl, QEventLoop
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
        proxyHost = s.value("proxy/proxyHost", str())
        proxyPassword = s.value("proxy/proxyPassword", str())
        proxyPort = s.value("proxy/proxyPort", str())
        proxyType = s.value("proxy/proxyType", str())
        proxyTypes = {'DefaultProxy': 'http', 'HttpProxy': 'http', 'Socks5Proxy': 'socks', 'HttpCachingProxy': 'http',
                      'FtpCachingProxy': 'ftp'}
        if proxyType in proxyTypes: proxyType = proxyTypes[proxyType]
        proxyUser = s.value("proxy/proxyUser", str())
        proxyString = 'http://' + proxyUser + ':' + proxyPassword + '@' + proxyHost + ':' + proxyPort
        proxy = urllib.request.ProxyHandler({proxyType: proxyString})
        auth = urllib.request.HTTPBasicAuthHandler()
        opener = urllib.request.build_opener(proxy, auth, urllib.request.HTTPHandler)
        urllib.request.install_opener(opener)

    generalDownloadFailureMessage = '\n\nPlease check your network settings. Styles may need to be added manually. This does not affect the loaded data.'
    try:
        conn = urllib.request.urlopen(url, timeout=30)  # Allow 30 seconds for completion
        if conn.getcode() != 200:
            # It looks like the request was unsuccessful
            raise Exception('Failed to download %s\n\nThe HTTP status code was %d.' % (
            url, conn.getcode()) + generalDownloadFailureMessage)
    except urllib.error.URLError as e:
        raise Exception('Failed to download %s\n\nThe reason was %s.' % (
        url, e.reason) + generalDownloadFailureMessage)

    handle.write(conn.read())


def get_OSMM_schema_ver(s):
    """ Guess the OSMM schema version from the path or string"""
    match = re.search(pattern=r'''.*\(v(\d)\).*''', string=s)
    if match:
        try:
            sch = int(match.group(1)) #0 is whole string
        except Exception:
            raise Exception("Unable to parse OSMM schema version from {}".format(s))

        if not sch in [7, 8, 9]:
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


def credentials_user_pass(selectedConnection):
    s = QSettings()
    s.beginGroup('/PostgreSQL/connections/{0}'.format(selectedConnection))
    # first try to get the credentials from AuthManager, then from the basic settings
    authconf = s.value('authcfg', None)
    auth_manager = QgsApplication.authManager()
    conf = QgsAuthMethodConfig()
    auth_manager.loadAuthenticationConfig(authconf, conf, True)
    if conf.id():
        user = conf.config('username', '')
        password = conf.config('password', '')
    else:
        user = s.value('username')
        password = s.value('password')
    return user, password


def get_db_cur(con_details, conn_name=""):
    if len(con_details['user']) == 0:
        # try to use credentials according conn_name
        if conn_name:
            user, password = credentials_user_pass(conn_name)

            db_conn = psycopg2.connect(database=con_details['database'],
                                       user=user,
                                       password=password,
                                       host=con_details['host'],
                                       port=con_details['port'])
        else:
            db_conn = psycopg2.connect(database=con_details['database'])
    else:
        db_conn = psycopg2.connect(database=con_details['database'],
                                   user=con_details['user'],
                                   password=con_details['password'],
                                   host=con_details['host'],
                                   port=con_details['port'])
    db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return db_conn.cursor()
