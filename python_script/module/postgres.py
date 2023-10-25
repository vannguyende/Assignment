import psycopg2.extras as extras
import psycopg2


class Postgresql:
    """
    Interact with Postgres to insert data.
    """
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        print("Connection successful")
        return conn

    def execute_batch(self, df, table, page_size=100):
        """
        Using psycopg2.extras.execute_batch() to insert the dataframe
        :param df: dataframe need to insert
        :param table: postgres table name
        :param page_size: maximum number of argslist items to include in every statement
        :return: None
        """
        # Get cpnnection
        conn = self.connect()
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_batch(cursor, query, tuples, page_size)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("execute_batch() done")
        cursor.close()

