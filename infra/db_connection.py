from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import configparser


def get_connection():
    config = configparser.ConfigParser()
    config.read("pyconf.ini")
    server = config['DATABASE']['server']
    port = config['DATABASE']['port']
    username = config['DATABASE']['username']
    password = config['DATABASE']['password']
    database = config['DATABASE']['database']
    
    connection_string = f"postgresql://{username}:{password}@{server}:{port}/{database}"
    engine: create_engine = create_engine(connection_string)
    dw_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
