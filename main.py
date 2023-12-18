from modules.dim_time import create_dim_time
from modules.manager_table import create_tables
from modules.language import Language
from modules.category import Category
from modules.country import Country
from modules.store import Store
from modules.city import City
from modules.address import Address
from modules.customer import Customer
from modules.actor import Actor
from modules.film import Film
from modules.payment import Payment
import logging
import sys


def installation():
    logging.info('Installing modules...')
    create_tables()
    logging.info('Installed successfully!')


def registration_load():
    logging.info('Init registration_load')
    language = Language()
    language.load_stg()
    language.load_dim()

    country = Country()
    country.load_stg()
    country.load_dim()

    city = City()
    city.load_stg()
    city.load_dim()

    address = Address()
    address.load_stg()
    address.load_dim()

    store = Store()
    store.load_stg()
    store.load_dim()

    film = Film()
    film.load_stg()
    film.load_dim()

    actor = Actor()
    actor.load_stg()
    actor.load_dim()

    customer = Customer()
    customer.load_stg()
    customer.load_dim()

    category = Category()
    category.load_stg()
    category.load_dim()


def fatc_load():
    payment = Payment()
    payment.load_stg_s()
    payment.load_fact_payment()


def load():
    try:
        logging.info('Loading...')
        create_dim_time()
        registration_load()
        fatc_load()
        logging.info('Loaded successfully!')
    except Exception as ex:
        logging.exception(ex)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f'dvd_shop.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    load()
