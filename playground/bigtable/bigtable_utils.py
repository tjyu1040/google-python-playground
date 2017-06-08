# -*- coding: utf-8 -*-
from __future__ import division, print_function

import collections
import os
import six

from google.cloud.bigtable.client import Client
from google.cloud.bigtable.row_filters import (
    ColumnQualifierRegexFilter, ColumnRangeFilter, RowFilterChain,
    RowFilterUnion, RowKeyRegexFilter
)


def build_bigtable_client(project_id=None, credentials=None,
                          service_key_file=None, read_only=False):
    """
    Build a Google Cloud Bigtable API client.

    Parameters
    ----------
    project_id : str
        The ID of the project owning the instances and tables.
    credentials : object
        Credentials object to pass into client. If None, attempt to determine
        from environment.
    service_key_file : str
        Path to a JSON keyfile containing service account credentials. This is
        recommended for production environments.
    read_only : bool
        Indicates if the client should be for reading only.

    Returns
    -------
    client : Client
        Google Cloud Bigtable API client to work with.
    """
    admin = not read_only  # Cannot perform admin tasks if read-only.
    if service_key_file and os.path.isfile(service_key_file):
        client = Client.from_service_account_json(
            service_key_file, admin=admin, read_only=read_only
        )
    else:
        client = Client(
            project=project_id, credentials=credentials, admin=admin,
            read_only=read_only
        )
    return client


def build_row_filter(row_key_regex=None, column_families=None, columns=None):
    """
    Build a row filter using a combination of row keys, column families, or
    columns to retrieve.

    Parameters
    ----------
    row_key_regex : str
        Regular expression for matching row keys.
    column_families : iterable[str]
        An iterable of column families to retrieve.
    columns : iterable[str]
        An iterable of column names or regular expressions for matching
        columns.

    Returns
    -------
    row_filter : RowFilter
        The built row filter from passed in parameters. If no parameters, None
        is returned.
    """
    if (row_key_regex is not None and
            not isinstance(row_key_regex, six.string_types)):
        raise TypeError('row_key_regex must be a str or unicode type.')
    if (column_families is not None and
            not isinstance(column_families, collections.Sequence)):
        raise TypeError('column_families must be an iterable.')
    if columns is not None and not isinstance(columns, collections.Sequence):
        raise TypeError('columns must be an iterable.')

    filters = []

    # Build a filter for row keys.
    if row_key_regex:
        row_key_filter = RowKeyRegexFilter(row_key_regex)
        filters.append(row_key_filter)

    # Build filters for column families.
    if column_families:
        cf_filters = [ColumnRangeFilter(cf) for cf in column_families]
        if len(cf_filters) > 1:
            filters.append(RowFilterUnion(cf_filters))
        else:
            filters.append(cf_filters[0])

    # Build filters for columns.
    if columns:
        col_filters = [ColumnQualifierRegexFilter(col) for col in columns]
        if len(col_filters) > 1:
            filters.append(RowFilterUnion(col_filters))
        else:
            filters.append(col_filters[0])

    if len(filters) == 1:
        return filters[0]
    else:
        return RowFilterChain(filters=filters) if filters else None
