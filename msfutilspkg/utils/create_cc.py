#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import xmlrpc.client
import base64

dbname = 'OCBHQ'
user = 'my_user'
password = 'my_password'
host = 'my_server'
port = 8069  # xml-rpc port, 8069 on prod instance

dbname = 'OCBHQ'
user = 'talend'
password = "EprSFkzM3Tetvm46ttu4RRqLTje3fdr6!" # Check password in 1Password, name 'ocbhq user for talend middleware'

# test
host = '192.168.50.153'

# prod
# host = '192.168.50.13'

port = 8069  

lang_context = {'lang': 'en_MF'}  # or fr_MF
file_to_import = 'cc_update.xls'
file_to_import = 'C:\\Users\\DUC\\OneDrive - MSF\\Documents\\UnifieldCostCenter\\input_files\\Cost_Center_Create_Import_Test.xls'

url = 'http://%s:%s/xmlrpc/' % (host, port)

# retrieve the user id : http://<host>:<xmlrpcport>/xmlrpc/common
sock = xmlrpc.client.ServerProxy(url + 'common')
user_id = sock.login(dbname, user, password)


# to query the server: http://<host>:<xmlrpcport>/xmlrpc/object
sock = xmlrpc.client.ServerProxy(url + 'object', allow_none=1)

# the content of the file must be base64 encoded
b64_file_content = base64.b64encode(open(file_to_import, 'rb').read())

# create and populate the wizard to create new CC
wiz_id = sock.execute(dbname, user_id, password, 'msf.import.export', 'create', {
    'model_list_selection': 'cost_centers',
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
print('State: %s' % result['state'])
print('Message: %s' % result['info_message'])
print('Error Message: %s' % result['error_message'])
print('Warning Message: %s' % result['warning_message'])
