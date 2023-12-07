from abc import ABC, abstractmethod
import logging


class IBaseModule(ABC):
    INVALID_DATA: int = -1

    @classmethod
    @abstractmethod
    def load_stg(cls):
        logging.warning('implement load_stg')

    @classmethod
    @abstractmethod
    def load_dim_from_db(cls):
        logging.warning('implement load_dim_from_db')

    @classmethod
    @abstractmethod
    def find_item_by_bk(cls, bk):
        logging.warning('implement find_item_by_bk')

    @classmethod
    @abstractmethod
    def load_dim(cls):
        logging.warning('implement load_dim')
