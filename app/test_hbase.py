"""
Python script that tests writing data into HBase.

References:
- https://happybase.readthedocs.io/en/latest/
"""

import happybase

HBASE_HOST: str = 'localhost'
HBASE_PORT: int = 9090
TABLE_NAME: str = 'martin2'
COLUMN_FAMILY_V1: str = 'cfv1'
COLUMN_FAMILY_V2: str = 'cfv2'
COLUMN_FAMILY_V3: str = 'cfv3'
SAMPLE_KEY1: str = 'nisman1'
SAMPLE_KEY2: str = 'nisman2'
SAMPLE_KEY3: str = 'nisman3'

connection: happybase.Connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)

print('Table:')
print(connection.tables())

try:
    connection.create_table(
        TABLE_NAME,
        {
            COLUMN_FAMILY_V1: dict(max_versions=10),
            COLUMN_FAMILY_V2: dict(max_versions=1, block_cache_enabled=False),
            COLUMN_FAMILY_V3: dict(),
        }
    )
except Exception as error:
    if 'AlreadyExists' not in str(error):
        raise

table: happybase.table.Table = connection.table(TABLE_NAME)
print('Table:', table)

print('')
print('Put:')
table.put(
    SAMPLE_KEY1.encode('utf-8'),
    {
        f'{COLUMN_FAMILY_V1}:attr1'.encode('utf-8'): 'value1'.encode('utf-8'),
        f'{COLUMN_FAMILY_V1}:attr2'.encode('utf-8'): 'value2'.encode('utf-8'),
        f'{COLUMN_FAMILY_V1}:attr3'.encode('utf-8'): 'value3'.encode('utf-8'),
        f'{COLUMN_FAMILY_V2}:attr1'.encode('utf-8'): 'value4'.encode('utf-8'),
        f'{COLUMN_FAMILY_V2}:attr2'.encode('utf-8'): 'value5'.encode('utf-8'),
        f'{COLUMN_FAMILY_V3}:attr1'.encode('utf-8'): 'value6'.encode('utf-8'),
    }
)

table.put(
    SAMPLE_KEY2.encode('utf-8'),
    {
        f'{COLUMN_FAMILY_V1}:attr10'.encode('utf-8'): 'x1'.encode('utf-8'),
        f'{COLUMN_FAMILY_V1}:attr20'.encode('utf-8'): 'x2'.encode('utf-8'),
        f'{COLUMN_FAMILY_V2}:attr10'.encode('utf-8'): 'x3'.encode('utf-8'),
        f'{COLUMN_FAMILY_V3}:attr10'.encode('utf-8'): 'x4'.encode('utf-8'),
    }
)

table.put(
    SAMPLE_KEY3.encode('utf-8'),
    {
        f'{COLUMN_FAMILY_V1}:attr10'.encode('utf-8'): 'lorem-ipsum'.encode('utf-8'),
    }
)

print('')
print('Row by ID:')
row: dict = table.row(SAMPLE_KEY1.encode('utf-8'))
print(row)

print('')
print('Query by IDs:')
keys: list = [SAMPLE_KEY1.encode('utf-8'), SAMPLE_KEY2.encode('utf-8')]
for key, data in table.rows(keys):
    print(key, '-->', data)

print('')
print('Scan with Prefix:')
for key, data in table.scan(row_prefix=SAMPLE_KEY1[:3].encode('utf-*')):
    print(key, '-->', data)

print('')
print('Delete:')
row: dict = table.delete(SAMPLE_KEY1.encode('utf-8'))
print(row)
