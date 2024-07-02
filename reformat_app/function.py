from abc import ABC, abstractmethod
import os
import glob
import shutil
import zipfile
import time
import logging
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from os.path import join
from .module import Convert2File
from .setup import PARAMS,CONFIG, Folder

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
    def __init__(self) -> None: 
        # setup params
        for key, value in PARAMS.items():
            setattr(self, key, value)
        
        self._date = self.batch_date.strftime("%Y%m%d")
        self._time = time.strftime("%H")
        
        # past_date_before_2yrs = ini_time_for_now - timedelta(days = 2)
        # self.backup_dir = join(Folder.BACKUP, self._date)
        # if not os.path.exists(self.backup_dir):
        #     self.zip_backup()
        # else:
        #     self.genarate_backup()
        
        self.zip_backup()

    def zip_backup(self):
        for module in self.source:
            ## set full path for backup file.
            dirname = CONFIG[module]["output_dir"]
            file = CONFIG[module]["output_file"]
            full_dir = join(dirname, file)
            
            backup_dir = join(Folder.BACKUP, module, self._date)
            if not os.path.exists(backup_dir):
                try:
                    os.makedirs(backup_dir)
                except OSError:
                    pass
            
            root_dir = os.path.dirname(backup_dir)
            for date_dir in os.listdir(root_dir):
                if date_dir < self._date:
                    print("OK")
                else:
                    self.genarate_backup(full_dir, backup_dir)
                
        # for root_dir in Path(Folder.BACKUP).iterdir():
        #     sub_dir = Path(root_dir).stem
        #     if sub_dir < self._date:
        #         ## zip file.   
        #         zip_name = join(Folder.BACKUP, f"{sub_dir}.zip")
        #         with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        #             for file in root_dir.rglob("*"):
        #                 zf.write(file, file.relative_to(root_dir))
        #         ## remove 
        #         shutil.rmtree(sub_dir)
        
    def genarate_backup(self, full_dir: str, backup_dir: str) -> None:
        ## set backup file.
        backup_file =  f"{Path(full_dir).stem}_BK{self._time}.csv"
        full_backup = join(backup_dir, backup_file)
        if glob.glob(full_dir, recursive=True):
            shutil.copy2(full_dir, full_backup)
                
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
