import psycopg2
import MySQLdb

def make_conn(cred_file):
    creds = map(str.strip, open(cred_file, 'r').readlines())

    if creds[0] == 'mysql':
        try:
            c = MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3],
                unix_socket = creds[6])
        except:
            c = MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3])

        c.set_character_set('utf8')

        return c
    else:
        return psycopg2.connect(database = creds[1],
            user = creds[2],
            password = creds[3],
            host = creds[4],
            port = creds[5])
