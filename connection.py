import psycopg2

""" Convenience class that wraps a Postgres Database Connection. """
class Connection:

    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect(self):
        self.conn = psycopg2.connect("host='{}' dbname='{}' user='{}' password='{}'".format(
            self.host, self.dbname, self.user, self.password))

    def cursor(self, name=None):
        return self.conn.cursor(name=name)
    
    def commit(self):
        return self.conn.commit()
    
    def rollback(self):
        return self.conn.rollback();

