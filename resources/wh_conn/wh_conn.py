from secrets import token_bytes

from mssql_python import connect
from mssql_python.connection import Connection
import struct
import notebookutils

def get_bytes():
    return notebookutils.credentials.getToken("https://database.windows.net").encode("UTF-16-LE")

def get_con(server_name:str,db_name:str)-> Connection:
    token_bytes = get_bytes()
    token_struct = struct.pack(
        f"<I{len(token_bytes)}s", 
        len(token_bytes), 
        token_bytes
    )
    connection_string = (
        f"Server=tcp:{server_name},1433;"
        f"Database={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    return connect(connection_string, attrs_before={1256: token_struct})

def check_con(conn)-> bool:
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        finally:
            cur.close()
    except Exception:
        return False