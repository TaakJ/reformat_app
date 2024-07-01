from abc import ABC, abstractmethod
import re
import os
import tarfile
import time
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

class CollectParams(ABC):
    
    @abstractmethod
    def paramsSetter(self, module: str):
        pass

    def get_extract_data(self, i: int, format_file: any) -> dict:
        logging.info("Extract Data Each Module")
        data = self.collect_data(i, format_file)
        return data
    
    @abstractmethod
    def collect_data(self, i: int, format_file: any):
        pass

class CollectBackup:
    def __init__(self, bk) -> None:
        self.genarate_backup(bk)
        
    def genarate_backup(self, bk):
        
        date = bk.date.date().strftime("%Y%m%d")
        _folder = join(bk.module, date)
        
        full_backup = join(Folder.BACKUP, _folder)
        if not os.path.exists(full_backup):
            try:
                os.makedirs(full_backup)
            except OSError:
                pass
            
        hour = time.strftime("%H")
        print(hour)
        
    
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
