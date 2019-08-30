#!/usr/bin/python
import psycopg2 as pg

def get_table_columns(connection, schema, table):
    cols = []
    try:
        cursor = connection.cursor()
        sql = "SELECT * FROM {}.{} LIMIT 0".format(schema, table)
        cursor.execute(sql)
        for desc in cursor.description:
            cols.append(desc[0])
        cursor.close()
    except pg.Error as e:
        print(e)
    return cols

def check_tables():
    connection = pg.connect('dbname=training user=postgres password=postgres host=localhost port=5432')
    schema = 'hn_paths'
    # for Roads
    #tables = ['ferrynode', 'ferryterminal', 'ferrylink', 'road', 'roadjunction', 'roadlink', 'roadnode', 'street']
    # for RAMI
    #tables = ['accessrestriction', 'hazard', 'highwaydedication', 'reinstatement', 'restrictionforvehicles', 'specialdesignation', 'structure', 'turnrestriction']
    # tables = ['connectinglink', 'connectingnode', 'ferrylink', 'ferrynode', 'ferryterminal', 'highwaydedication', 'maintenance', 'path', 'pathlink', 'pathnode', 'reinstatement', 'specialdesignation', 'street']
    tables = ['street']
    for t in tables:
        columns = get_table_columns(connection, schema, t)
        for c in columns:
            try:
                cur = connection.cursor()
                sql = "SELECT DISTINCT {} FROM {}.{} ORDER BY {} NULLS LAST LIMIT 2".format(c, schema, t, c)
                cur.execute(sql)
                results = cur.fetchall()
                if results[0][0] is None:
                    print(t + ": " + c + " ...INVALID")
                else:
                    print(t + ": " + c + " ...OK")
                cur.close()
            except pg.Error as e:
                print(e)

if __name__ == '__main__':
    check_tables()
