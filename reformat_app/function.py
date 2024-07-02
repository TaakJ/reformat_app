from abc import ABC, abstractmethod
import os
import glob
import shutil
import zipfile
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
        
        ## get output from config.
        output_dir = CONFIG[bk.module]["output_dir"]
        output_file = CONFIG[bk.module]["output_file"]
        self.full_output = join(output_dir, output_file)
        
        self._date = bk.date.date().strftime("%Y%m%d")
        self._time = time.strftime("%H")
        
        if self._time == 00:
            self.genarate_backup()
        else:
            self.zip_backup()
        
    def genarate_backup(self):
        
        ## set backup date folder.
        backup_dir = join(Folder.BACKUP, self._date)
        if not os.path.exists(backup_dir):
            try:
                os.makedirs(backup_dir)
            except OSError:
                pass
            
        backup_file =  f"{Path(self.full_output).stem}_bk_h{self._time}.csv"
        full_backup = join(backup_dir, backup_file)
        
        ## backup file.
        if glob.glob(self.full_output, recursive=True):
            shutil.copy2(self.full_output, full_backup)

    def zip_backup(self):
        for root_dir in Path(Folder.BACKUP).iterdir():
            
            sub_dir = Path(root_dir).stem
            ## check path with date.
            if sub_dir >= self._date:
                
                ## zip file.    
                with zipfile.ZipFile(f"{sub_dir}.zip", "w", zipfile.ZIP_DEFLATED) as zf:
                    for file in root_dir.rglob("*"):
                        zf.write(file, file.relative_to(root_dir.parent))
    
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
