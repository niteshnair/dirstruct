import os
import sqlite3
from sqlite3 import Error


class DbOperations:
    conn = None

    def __init__(self, path, dbname):
        """
            This constructor will take the path and name of the database and initialize the database connection.
            If a database does not exists a new database file would be created
            :param path: Directory location where the database file is located
            :param dbname: Database name that needs to be initialized
        """
        try:
            self.conn = sqlite3.connect(os.path.join(path, dbname))
            print(sqlite3.version)
        except Error as e:
            self.conn.close()
            print(e)

    """def create_db(self, path, dbname):
        try:
            if not os.path.exists(path, dbname):
                conn = sqlite3.connect(os.path.join(path, dbname))
            else:
                print("Database {} already exists at location {}".format(dbname, path))
            print(sqlite3.version)
        except Error as e:
            conn.close()
            print(e)
        return conn"""

    def create_table(self, table_name, column_list, drop_flag):
        """
            This function would create a table from the details passed in parameters
        :param table_name: This param represents the table name that needs to be created
        :param column_list: This param is a list of tuples representing the column_name,data_type & length
            in respective order. If data type is decimal pass length as string in format "10,2"
        :param drop_flag: This is a boolean param, if True it will drop the table if it exists and then create
        the table
        :return: This function does not return anything
        """
        cur = self.conn.cursor()
        col_names = DbOperations.return_columns(column_list)
        create_query = "CREATE TABLE IF NOT EXISTS {} ( {} ));".format(
            table_name, col_names
        )
        try:
            if drop_flag:
                cur.execute("DROP TABLE IF EXISTS {}".format(table_name))
                print("Table Dropped!")
            cur.execute(create_query)
            cur.close()
            print("Table Created!")
        except Error as e:
            cur.close()
            self.conn.close()
            print(e)

    @staticmethod
    def return_columns(column_list):
        # column_list = [("DRIVE_LETTER", "VARCHAR", 10), ("ROOT_FOLDER", "VARCHAR", "500")]
        col_list = []
        for col_name, data_type, length in column_list:
            column_det = "{} {}({})".format(col_name, data_type, length)
            col_list.append(column_det)
        return ",\n".join(col_list)

    def insert_data(self, table_name, insert_record, column_name):
        """
            This function would insert a row in the specified table.
        :param table_name: This param is string represent the table name
        :param insert_record: This param is a list containing the row to be inserted.
        :param column_name: This param is a list containing the column name corresponding the insert_record parameter.
            The order of the columns should be in the same order the values are specified in insert_record param.
        :return: This function does not returns anything
        """
        cur = self.conn.cursor()
        # DRIVE_LETTER,ROOT_FOLDER,SUB_FOLDER,FILE_NAME,FILE_SIZE
        try:
            cur.execute(
                "INSERT INTO {}({}) VALUES(?,?,?,?,?)".format(
                    table_name, ",".join(column_name)
                ),
                insert_record,
            )
            cur.close()
        except Error as e:
            cur.close()
            self.conn.close()
            print(e)
