from abc import ABC, abstractmethod
import os
import glob
import shutil
import time
import logging
from pathlib import Path
from datetime import datetime
from os.path import join
from .module import Convert2File
from .setup import CONFIG, Folder

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
        backup_dir = join(Folder.BACKUP, date)
        if not os.path.exists(backup_dir):
            try:
                os.makedirs(backup_dir)
            except OSError:
                pass
            
        ## get output from config.
        output_dir = CONFIG[bk.module]["output_dir"]
        output_file = CONFIG[bk.module]["output_file"]
        full_output = join(output_dir, output_file)
        
        ## set backup file.
        _time = time.strftime("%H")
        backup_file =  f"{Path(output_file).stem}_bk_h{_time}.csv"
        full_backup = join(backup_dir, backup_file)
        
        if glob.glob(full_output, recursive=True):
            shutil.copy2(full_output, full_backup)

    
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
