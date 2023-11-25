class DatabaseConnectionError(Exception):
    pass

class AuthenticationTokenError(Exception):
    pass

class DataPushError(Exception):
    pass

class QueryExecutionError(Exception):
    pass

class TableCreationError(Exception):
    pass

class TableExistenceError(Exception):
    pass
