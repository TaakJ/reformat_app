from abc import ABC, abstractmethod
import re
import logging
from datetime import datetime
from os.path import join
from .module import Convert2File
from .setup import CONFIG, PARAMS, Folder

class CollectLog(ABC):
    def __init__(self):
        self._log = []

    @property
    def logging(self) -> list:
        return self._log

    @logging.setter
    def logging(self, log: list) -> None:
        self.logSetter(log)

    @abstractmethod
    def logSetter(self, log: list):
        pass

class SetterParams(ABC):
    
    @abstractmethod
    def paramsSetter(self, module: str):
        pass

    def get_extract_data(self, i: int, format_file: any) -> dict:

        logging.info("Extract Data Each Module")
        module = self.logging[i]["module"]

        if module == "ADM":
            data = self.extract_adm(i, format_file)
            return data
        elif module == "DOC":
            data = self.extract_doc(i, format_file)
            return data
        elif module == "LDS":
            data = self.extract_lds(i, format_file)
            return data
        elif module == "BOS":
            data = self.extract_bos(i, format_file)
            return data
        elif module == "CUM":
            data = self.extract_cum(i, format_file)
            return data
        elif module == "ICA":
            data = self.extract_ica(i, format_file)
            return data
        elif module == "IIC":
            data = self.extract_iic(i, format_file)
            return data
        elif module == "LMT":
            data = self.extract_lmt(i, format_file)
            return data
        elif module == "MOC":
            data = self.extract_moc(i, format_file)
            return data

    @abstractmethod
    def extract_adm(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_doc(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_lds(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_bos(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_cum(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_ica(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_iic(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_lmt(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def extract_moc(self, i: int, format_file: any):
        pass

class CollectParams(SetterParams):
    pass

import os
import tarfile
import time
class CollectBackup:
    def __init__(self) -> None:
        print("OK")
        
    def backup_folder(self):
        
        date = self.date.date().strftime("%Y%m%d")
        hour = time.strftime("%H")
        
        _folder = Folder.BACKUP + join(date, hour)
        print(_folder)
        
        # if not os.path.exists(os.path.dirname(filename)):
        #     try:
        #         os.makedirs(os.path.dirname(filename))
        #     except OSError:
        #         pass
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
