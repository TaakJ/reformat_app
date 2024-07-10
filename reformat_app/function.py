from abc import ABC, abstractmethod
import os
import glob
import shutil
import zipfile
from datetime import timedelta
import logging
from pathlib import Path
from os.path import join
from .module import Convert2File
from .setup import Folder

class CollectLog(ABC):
    def __init__(self) -> None:
        self._log = []

    @property
    def logging(self) -> list:
        return self._log

    @logging.setter
    def logging(self, log: list) -> None:
        self.logSetter(log)

    @abstractmethod
    def logSetter(self, log: list): ...

class CollectParams(ABC):
    def get_extract_data(self, i: int, format_file: any) -> dict:
        logging.info("Extract Data Each Module")
        data = self.collect_data(i, format_file)
        return data

    @abstractmethod
    def collect_params(self):
        pass

    @abstractmethod
    def collect_data(self, i: int, format_file: any):
        pass
    
class BackupAndClear:
    
    def backup(self):
        
        self.backup_dir = join(Folder.BACKUP, self.module)
        
        if not os.path.exists(self.backup_dir):
            try:
                os.makedirs(self.backup_dir)
            except OSError:
                pass
            
        for date_dir in os.listdir(self.backup_dir):
            if not date_dir.endswith(".zip"):
                self.backup_zip_file(date_dir)
        
        self.genarate_backup_file()
        
    def backup_zip_file(self, date_dir):
        
        # now = self.date - timedelta(days=1)
        
        if date_dir < self.date.strftime("%Y%m%d"):
            date_dir = join(self.backup_dir, date_dir)
            zip_name = join(self.backup_dir, f'{date_dir}.zip')
            
            with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                for file in [file for file in Path(date_dir).rglob("*") if file.exists()]:
                    zf.write(file, file.relative_to(self.backup_dir))

            shutil.rmtree(date_dir)
            
            state = "succeed"
            logging.info(f'Zip file {zip_name} status: {state}' )
    
    def genarate_backup_file(self):
        
        if glob.glob(self.full_target, recursive=True):
            date_dir = join(self.backup_dir, self.date.strftime("%Y%m%d"))
            if not os.path.exists(date_dir):
                try:
                    os.makedirs(date_dir)
                except OSError:
                    pass
            
            backup_file = f"BK_{Path(self.full_target).stem}_T{self.time}.csv"
            full_backup = join(date_dir, backup_file)
            
            status = "failed"
            try:
                shutil.copy2(self.full_target, full_backup)
                status = "succeed"
            except OSError:
                pass
            
            logging.info(f'Backup file from {self.full_target} to {full_backup} status: {status}')
                
    def clear_tmp(self):
        try:
            tmp_dir = join(Folder.TMP, self.module)
            
            for date_dir in os.listdir(tmp_dir):
                if date_dir < self.date.strftime("%Y%m%d"):
                    tmp_file = join(tmp_dir, date_dir)
                    shutil.rmtree(tmp_file)
                    
                    state = "succeed"
                    logging.info(f'Clear Tmp file: {tmp_file} status: {state}')
                    
        except OSError:
            pass
        
    def clear_backup(self):
        try:
            backup_dir = join(Folder.BACKUP, self.module)
            bk_date = self.bk_date.strftime("%Y%m%d")
            
            for date_dir in os.listdir(backup_dir):
                if date_dir <= bk_date:
                    zip_dir = join(backup_dir, date_dir)
                    os.remove(zip_dir)
                    
                    state = "succeed"
                    logging.info(f'Clear Zip file: {zip_dir} status: {state}')
                    
        except OSError:
            pass

class CallFunction(Convert2File, CollectLog, CollectParams, BackupAndClear):
    pass
