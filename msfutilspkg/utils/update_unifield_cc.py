#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import xmlrpc.client
import base64
import logging

logger = logging.getLogger(__name__)


def update_cost_centers(file_to_import: str, dbname: str, user: str, password: str, host: str, port: int, context: str = "update", **kwargs):
    lang_context = {'lang': 'en_MF'}  # or fr_MF
    
    url = 'http://%s:%s/xmlrpc/' % (host, port)

    # retrieve the user id : http://<host>:<xmlrpcport>/xmlrpc/common
    sock = xmlrpc.client.ServerProxy(url + 'common')
    user_id = sock.login(dbname, user, password)


    # to query the server: http://<host>:<xmlrpcport>/xmlrpc/object
    sock = xmlrpc.client.ServerProxy(url + 'object', allow_none=1)

    # the content of the file must be base64 encoded
    b64_file_content = base64.b64encode(open(file_to_import, 'rb').read())
    model_list_selection = 'cost_centers_update' if context == "update" else 'cost_centers'

    # create and populate the wizard to update CC
    wiz_id = sock.execute(dbname, user_id, password, 'msf.import.export', 'create', {
        'model_list_selection': model_list_selection,
        'import_file': str(b64_file_content, 'utf8'),
    }, lang_context)

    # launch the import
    sock.execute(dbname, user_id, password,
                'msf.import.export', 'button_import_xml', wiz_id, lang_context)

    # get the summary
    result = sock.execute(
        dbname, user_id, password,
        'msf.import.export', 'read', wiz_id,
        ['state', 'info_message', 'error_message', 'warning_message'],
        lang_context)
    
    logger.info('State: %s' % result['state'])
    logger.info('Message: %s' % result['info_message'])
    logger.info('Error Message: %s' % result['error_message'])
    logger.info('Warning Message: %s' % result['warning_message'])
    return result