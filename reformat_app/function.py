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
        self._time = time.strftime("%H%M")
        self.zip_backup()

    def zip_backup(self) -> None:
        for module in self.source:
            root_dir = join(Folder.BACKUP, module)
            try:
                for date_dir in [_dir for _dir in os.listdir(root_dir) if not _dir.endswith(".zip")]:
                    if date_dir > self._date:
                        zip_dir  = join(root_dir, date_dir)
                        zip_name = join(root_dir, f"{date_dir}.zip")
                        ## zip file.
                        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                            for file in Path(zip_dir).rglob("*"):
                                zf.write(file, file.relative_to(zip_dir))
                        ## remove dir after zip file.
                        shutil.rmtree(zip_dir)
                self.genarate_backup(module)
                
            except FileNotFoundError:
                self.genarate_backup(module)
            
    def genarate_backup(self, module: str) -> None:
        ## set path output / backup.
        output_dir  = CONFIG[module]["output_dir"]
        output_name = CONFIG[module]["output_file"]
        backup_name =  f"{Path(output_name).stem}_BK{self._time}.csv"
        
        backup_dir = join(Folder.BACKUP, module, self._date)
        if not os.path.exists(backup_dir):
            try:
                os.makedirs(backup_dir)
            except OSError:
                pass
        full_output = join(output_dir, output_name)
        full_backup = join(backup_dir, backup_name)
        
        ## move file to backup.
        if glob.glob(full_output, recursive=True):
            shutil.copy2(full_output, full_backup)
            
    def remove_backup(self):
        print("OK")
                
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
