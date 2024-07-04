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
        for key, value in PARAMS.items():
            setattr(self, key, value)
        
        self._date = datetime.now().date().strftime("%Y%m%d")
        self._time = time.strftime("%H%M")
        
        for module in self.source:
            self.root_dir = join(Folder.BACKUP, module)
            state = self.create_date_dir()
            
            if state == "succeed":
                for date_dir in [date_dir for date_dir in os.listdir(self.root_dir) if not date_dir.endswith(".zip")]:
                    self.zip_backup(date_dir)
                self.genarate_backup_file(module)
            
            if self.clear:
                self.remove_utility(module)
                
    def create_date_dir(self) -> str:
        state = "failed"
        date_dir = join(self.root_dir, self._date)
        
        if not os.path.exists(date_dir):
            try:
                os.makedirs(date_dir)
            except OSError:
                pass
        state = "succeed"
        
        return state
    
    def remove_utility(self, module):
        ## remove log dir
        try:
            for date_dir in os.listdir(Folder.LOG):
                if date_dir < self._date:
                    log_dir = join(Folder.LOG, date_dir)
                    shutil.rmtree(log_dir)
        except OSError:
            pass
                
        ## remove tmp file 
        try:
            tmp_dir = join(Folder.TMP, module)
            for file in os.listdir(tmp_dir):
                tmp_file = join(tmp_dir, file)
                os.remove(tmp_file)
        except OSError:
            pass
        
        ## remove backup file. 
        try:
            backup_dir = join(Folder.BACKUP, module)
            for date_dir in os.listdir(backup_dir):
                if date_dir < self._date:
                    zip_dir = join(backup_dir, date_dir)
                    os.remove(zip_dir)
        except OSError:
            pass
            
    def zip_backup(self, date_dir):
        if date_dir < self._date:
            zip_dir  = join(self.root_dir, date_dir)
            zip_name = join(self.root_dir, f"{date_dir}.zip")
            
            with zipfile.ZipFile( join(self.root_dir, zip_name), "w", zipfile.ZIP_DEFLATED) as zf:
                for file in Path(zip_dir).rglob("*"):
                    zf.write(file, file.relative_to(zip_dir))
                    
            shutil.rmtree(zip_dir)
            
    def genarate_backup_file(self, module):
        output_dir  = CONFIG[module]["output_dir"]
        output_file = CONFIG[module]["output_file"]
        
        if self.write_mode != "overwrite" or self.manual:
            output_file = f"{Path(output_file).stem}_{self._date}.csv"
            
        backup_dir  = join(self.root_dir, self._date)
        backup_file = f"BK_{Path(output_file).stem}_T{self._time}.csv"
        
        try:
            full_output = join(output_dir, output_file)
            full_backup = join(backup_dir, backup_file)
            ## move outpur file to backup file
            shutil.copy2(full_output, full_backup)
        except OSError:
            pass
                
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
