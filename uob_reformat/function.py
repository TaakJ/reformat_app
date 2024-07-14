from abc import ABC, abstractmethod
import os
import re
import glob
import shutil
import zipfile
import logging
from datetime import timedelta
import time
from pathlib import Path
from os.path import join
from itertools import chain
from .module import Convert2File
from .setup import Folder, CONFIG
from .exception import CustomException

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
    def logSetter(self, log: list):
        pass

class CollectParams(ABC):
    
    def __init__(self) -> None:
        self._full_input = ""
        
    @property
    def full_input(self) -> list:
        return self._full_input
    
    @full_input.setter
    def full_input(self, file: list) -> list:
        
        add_file = []
        for new_file in [f.strip() for f in re.split(r',', self._full_input) if f.strip() != ""]:
            if new_file not in add_file:
                add_file.append(new_file)
            else:
                continue
            
        self._full_input =  list(chain(file, add_file))
        return self._full_input
    
    def collect_params(self) -> None:
        
        logging.info(f'Set parameter from config file for module: {self.module}')
        
        status = "failed"
        record = {"module": self.module, "function": "collect_params", "status": status} 
        try:
            ## setup input dir / input file
            input_dir   = CONFIG[self.module]["input_dir"]
            input_file  = CONFIG[self.module]["input_file"]
            
            self.full_input = [join(input_dir, input_file)]
            
            ## setup output dir / output file             
            output_dir  = CONFIG[self.module]["output_dir"]
            output_file = CONFIG[self.module]["output_file"]
            
            suffix = f"{self.batch_date.strftime('%Y%m%d')}"
            file = lambda file: file if (self.write_mode == "overwrite" or self.manual) else f"{Path(file).stem}_{suffix}.csv"
            self.full_target = join(output_dir, file(output_file))
            
            status = "succeed"
            record.update({"status": status})
            
        except Exception as err:
            record.update({"err": err})
        
        self.logSetter([record])
        
        if "err" in record:
            raise CustomException(err=self.logging)
        
    def get_extract_data(self, i: int, format_file: any) -> dict:
        
        logging.info("Get Extract Data From File")
        
        data = self.collect_data(i, format_file)
        return data
    
    @abstractmethod
    def collect_data(self, i: int, format_file: any):
        pass
    
class BackupAndClear:
    
    def backup(self) -> None:
        
        self.root_dir = join(Folder.BACKUP, self.module)
        self._date    = self.date.strftime("%Y%m%d")
        self.time     = time.strftime("%H%M%S")
        
        ## clear backup / zip file
        status = self.backup_zip_file()
        
        logging.info("Backup file")
        
        self.backup_dir = join(self.root_dir, self._date)
        list_of_files = glob.glob(f'{self.backup_dir}/*')
        if list_of_files != []:
            
            backup_file = max(list_of_files, key=os.path.getctime)
            try:
                ## read csv file
                bk_df = self.read_csv_file(backup_file)
                bk_df = self.initial_data_type(bk_df)
                df  = self.read_csv_file(self.full_target)
                df = self.initial_data_type(df)
                
                ## check value change
                cmp_df = self.compare_data(bk_df, df)
                if (cmp_df['count'] >= 0).all():
                    print("OK")
                    # status = self.genarate_backup_file()
                else:
                    print("NOK")
                    # logging.info("No Backup file because no have change data")
                    
            except Exception:
                pass
        else:
            status = self.genarate_backup_file()
        
        logging.info(f'Backup file status {status}')
    
    def genarate_backup_file(self) -> str:
        
        status = "failed"
        if glob.glob(self.full_target, recursive=True):

            if not os.path.exists(self.backup_dir):
                try:
                    os.makedirs(self.backup_dir)
                except OSError:
                    pass
            try:
                backup_file = f"BK_{Path(self.full_target).stem}_T{self.time}.csv"
                full_backup = join(self.backup_dir, backup_file)
                
                shutil.copy2(self.full_target, full_backup)
                status = "succeed"
                
                logging.info(f'Backup file from {self.full_target} to {full_backup}')
            
            except Exception:
                pass
        else:
            status = "skipped"
            
        return status
    
    def backup_zip_file(self) -> str:
        
        self.bk_date = self.date - timedelta(days=7)
        self._bk_date = self.bk_date.strftime("%Y%m%d")
        
        status = "failed"
        try:
            status = "skipped"
            for date_dir in os.listdir(self.root_dir):
                
                zip_dir = join(self.root_dir, date_dir)
                zip_name = join(self.root_dir, f"{date_dir}.zip")
                
                if date_dir <= f'{self._bk_date}.zip':    
                    os.remove(zip_dir)
                    status = "succeed"
                    
                    logging.info(f'Clear Zip file: {zip_dir} status: {status}')
                
                else:
                    if not zip_dir.endswith(".zip") and date_dir < self._date:
                        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                            for file in Path(zip_dir).rglob("*"):
                                zf.write(file, file.relative_to(self.root_dir))
                        shutil.rmtree(zip_dir)
                        
                        status = "succeed"
                        logging.info(f'Zip file: {zip_name} status: {status}')
                    else:
                        continue
        except OSError:
            pass
        
        return status
                
    def clear_tmp(self) -> None:
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

class CallFunction(Convert2File, CollectLog, CollectParams, BackupAndClear):
    pass
