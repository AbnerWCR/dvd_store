from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import configparser
import os


def test_dw_connection():
    try:
        script_directory = os.path.dirname(os.path.abspath(__file__))
        pyconf_dir = os.path.join(script_directory, "..")
        pyconf_dir = f"{pyconf_dir}/pyconf.ini"
        config = configparser.ConfigParser()
        config.read(pyconf_dir)
        server = config['DATABASE']['server']
        port = config['DATABASE']['port']
        username = config['DATABASE']['username']
        password = config['DATABASE']['password']
        database = config['DATABASE']['database']
        connection_string = f"postgresql://{username}:{password}@{server}:{port}/{database}"
        engine: create_engine = create_engine(connection_string)
        dw_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        assert True
    except Exception as ex:
        assert False