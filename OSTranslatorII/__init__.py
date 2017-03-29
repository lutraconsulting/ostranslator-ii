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

# This script initializes the plugin, making it known to QGIS.

def classFactory(iface):
    """Load OsTranslatorII class from file OsTranslatorII."""
    from .os_translator_ii import OsTranslatorII
    return OsTranslatorII(iface)
