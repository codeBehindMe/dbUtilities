import cx_Oracle
import pandas as pd
import warnings
import pyodbc


class OracleCommand:
    def __init__(self, connectionString, command=None, **kwargs):
        """Constructor for the OracleCommand Class.
        Required Args:
        connectionString -- Connection string to the database with the correctly formatted username and password.

        Optional Args:
        kwargs -- Optional keyword arguments.

        Note** Does not support OAuth style authentication."""
        self.__cStr = connectionString
        self.__command = command
        if kwargs is not None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def __connect__(self):
        """Attempts to open a connection with the oracle database specified by the connection string."""
        self.__db = cx_Oracle.connect(self.__cStr)
        self.__cursor = self.__db.cursor()

    def __disconnect__(self):
        """Attempts to disconnect from a open connection to an oracle database."""
        self.__cursor.close()
        self.__db.close()

    @staticmethod
    def __pandasToList__(dataFrame):

        """
        Converts a pandas data frame to a cx_oracle ready container.
        :param dataFrame: pandas data frame container the data.
        :return: list
        """
        content = []
        for i, r in dataFrame.iterrows():
            row = r.values.tolist()
            content.append(row)

        return content

    @staticmethod
    def __castToNone__(value):
        """Casts a math.NaN value to None
        :param value: Any scalar value.
        :return value: None if Numpy.Math.NaN else value.

        This method is present to correctly input SQL.NULL type values to the database.
        """
        if value != value:
            return None
        else:
            return value

    @staticmethod
    def __buildInsertStrObj__(colNames, ToUpper=False):
        # type: (list) -> tuple
        """
        Constructs a column names and column numbers query string for sql function based on a list of column names.
        :param colNames: List of column names
        :return: string of column names with parenthesis. string of column indexes with parenthesis.
        """

        _open = '('
        _close = ')'
        _sep = ','
        _colNameStr = ''
        _colNumstr = ''

        if ToUpper:
            colNames = map(lambda x: x.upper(), colNames)

        for i, c in enumerate(colNames):
            if i == 0:
                _colNameStr += _open
                _colNameStr += """"%s" """ % c
                _colNameStr = _colNameStr.rstrip()

                _colNumstr += _open
                _colNumstr += ':' + str(i + 1)

            else:
                _colNameStr += _sep
                _colNameStr += """"%s" """ % c
                _colNameStr = _colNameStr.rstrip()

                _colNumstr += _sep
                _colNumstr += ':' + str(i + 1)

        _colNameStr += _close
        _colNumstr += _close

        return _colNameStr, _colNumstr

    @staticmethod
    def __buildInsCursorString__(tableName, colNumStr, colNameStr):
        # type: (str,str,str) -> str
        """This method generates a cursor string for the insert statements.
        :param tableName: Name of the target table to be inserted into as string.
        :param colNumStr: String of column numbers to be targeted.
        :param colNameStr: String of column names to be targeted.
        :return String to be fed into the oracle cursor.
        """
        return "INSERT INTO %s %s VALUES %s" % (tableName, colNameStr, colNumStr)

    def executeScalar(self, command):
        """Executes a SQL statement which returns a scalar value."""
        raise NotImplementedError()

    def executeVector(self, command=None):
        # type: (str) -> pd.DataFrame
        """
        This method generates a pandas data frame from a sql query.
        :param command: SQL Query to be executed.
        :return: Pandas dataframe containing the results of the query.
        """
        warnings.warn("This method is not fully tested. Be sure to view the source code before implementation.")
        if command is None and self.__command is None:
            raise ValueError("No command supplied for execution.")
        elif command is None:
            command = self.__command

        _df = pd.read_sql(command, self.__cStr)

        return _df

    def executeNonQuery(self, command=None):
        # type: (str) -> None
        """Executes a SQL statement which does not return a value."""
        warnings.warn("This method is not fully tested. Be sure to view the source code before implementation.")

        if command is None and self.__command is None:
            raise ValueError("No command supplied for execution.")
        elif command is None:
            command = self.__command

        self.__connect__()
        self.__cursor.execute(command)
        self.__db.commit()
        self.__disconnect__()

    def executeQueryAsync(self, command):
        """Not supported."""
        raise NotImplementedError()

    def executeStoredProcedure(self, command):
        """Executes a stored procedure."""
        raise NotImplementedError()

    def pandasBulkInsert(self, dataFrame, tableName, columnNames=None, colNamesUpper=False):
        # type: (pd.DataFrame,str,list,bool) -> None

        """
        Bulk upload data to a oracle table from a pandas data frame.
        :param dataFrame: pandas.DataFrame containing data.
        :param tableName: Name of the destination table.
        :param columnNames: List of column names, if not specified, infer column names from the pandas dataframe. Warning, the column names have to match the destination column names.
        :param colNamesUpper: Cast column names to upper.
        :return:
        """
        # If no column names specified get column names
        if columnNames is None:
            columnNames = list(dataFrame.columns.values)
        else:
            # Check of the number of column names provided match the number in the data frame.
            assert (len(columnNames) == dataFrame.shape[1])

        # Call to make the data frame into a list of lists.
        _dtList = self.__pandasToList__(dataFrame)

        # Convert any Numpy.NaN values to None for SQl.NULL recognition.
        # TODO: Can you improve this from running in O(n2)
        for i, r in enumerate(_dtList):
            for j, c in enumerate(r):
                _dtList[i][j] = self.__castToNone__(c)

        # Call to make the column names list into a string and get the string of column indexes for upload.
        _colNameStr, _colNumStr = self.__buildInsertStrObj__(columnNames, colNamesUpper)

        # Call to make the cursor string
        _curString = self.__buildInsCursorString__(tableName, _colNumStr, _colNameStr)

        # Connect
        self.__connect__()
        # Prepare cursor
        self.__cursor.prepare(_curString)
        # Feed and execute upload pipe
        self.__cursor.executemany(None, _dtList)
        # Commit changes
        self.__db.commit()

        # Disconnect
        self.__disconnect__()

    def getConnector(self):
        # type: (None) -> cx_Oracle.connection
        """
        Returns the cx_Oracle connector object from the class. Useful since things like beautiful soup prefers this object over the connection string.
        :return: connection object.
        """

        return cx_Oracle.connect(self.__cStr)


class SqlServerCommand:
    def __init__(self, connectionString, command=None, **kwargs):
        """
        Constructor for sql server operations class.
        :param connectionString: Connection string to the database.
        :param command: Optional command to be executed.
        :param kwargs: Optional arguments for the connection string.
        """
        self.__cStr = connectionString
        self.__cmd = command

    def __connect__(self):
        self.__con = pyodbc.connect(self.__cStr, autocommit=True)
        self.__cur = self.__con.cursor()

    def __disconnect__(self):
        self.__cur.close()
        self.__con.close()

    def executeVector(self):
        pass

    def getConnector(self):
        return pyodbc.connect(self.__cStr, autocommit=True)
