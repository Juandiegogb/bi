from dotenv import load_dotenv
from os import getenv


load_dotenv()

mssql = {
    "host": getenv("HOST"),
    "port": getenv("MSSQL_PORT"),
    "user": getenv("MSSQL_USER"),
    "pwd": getenv("MSSQL_PWD"),
    "db": getenv("MSSQL_DB"),
}

pg = {
    "host": getenv("HOST"),
    "port": getenv("PG_PORT"),
    "user": getenv("PG_USER"),
    "pwd": getenv("PG_PWD"),
    "db": getenv("PG_DB"),
}


