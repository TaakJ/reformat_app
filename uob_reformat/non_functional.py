from abc import ABC, abstractmethod
import os
import glob
import shutil
import zipfile
import logging
from datetime import timedelta
import time
from pathlib import Path
from os.path import join
from .functional import Convert2File
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

    def full_input(self) -> any:
        input_dir = CONFIG[self.module]['input_dir']
        input_file = CONFIG[self.module]['input_file']
        
        set_dir = lambda dir, file: [join(dir, x.strip()) for x in file.split(",")]
        full_input = set_dir(input_dir, input_file)
        
        full_depend = []
        depend_file = CONFIG[self.module].get('depend_file')
        if depend_file is not None:
            full_depend  = set_dir(input_dir, depend_file)
        
        return full_input, full_depend

    def full_target(self) -> list:
        output_dir = CONFIG[self.module]['output_dir']
        output_file = CONFIG[self.module]['output_file']

        suffix = self.batch_date.strftime('%Y%m%d')
        set_dir = lambda dir, file: [(join(dir, x.strip()) if self.write_mode == 'overwrite' or self.manual else join(dir, f"{Path(x.strip()).stem}_{suffix}.csv")) for x in file.split(',')]
        
        full_target = set_dir(output_dir, output_file)
        
        return full_target

    def colloct_setup(self) -> None:

        logging.info(f'Setup params/logging for module: {self.module}')
        
        log = []
        status = 'failed'
        record = {'module': self.module, 'function': 'colloct_setup', 'status': status}
        
        try:
            full_input, full_depend = self.full_input()
            full_target = self.full_target()
            
            if len(full_input) == len(full_target):
                mapping_list = list(zip(full_input, full_target))
            else:
                mapping_list = [(input, target) for input in full_input for target in full_target]
            
            # 0: input file
            # 1: target file
            for i, files in enumerate(mapping_list,1):
                for select_num in [num for num in self.select_files if i == num]:
                    
                    status = 'succeed'
                    if set(('full_input', 'full_target')).issubset(record):
                        copy_record = record.copy()
                        copy_record.update(
                            {
                                'full_input': files[0], 
                                'full_target': files[1],
                                'package': 'USER' if select_num == 1 else 'PARAM', 
                                'status': status,
                            }
                        )
                        if full_depend != []:
                            copy_record.setdefault('full_depend', full_depend)
                            
                        log += [copy_record]
                        
                    else:
                        record.update(
                            {
                                'full_input': files[0], 
                                'full_target': files[1],
                                'package': 'USER' if select_num == 1 else 'PARAM',
                                'status': status,
                            }
                        )
                        if full_depend != []:
                            record.setdefault('full_depend', full_depend)
                            
                        log = [record]
                        
        except Exception as err:
            record.update({'err': err})
            log += [record]

        self.logSetter(log)

        if 'err' in record:
            raise CustomException(err=self.logging)

    @abstractmethod
    def collect_user_file(self, i: int, format_file: any) -> None:
        pass
    
    @abstractmethod
    def collect_param_file(self, i: int, format_file: any) -> None:
        pass

class BackupAndClear:
    
    def clear_tmp(self) -> None:
        try:
            tmp_dir = join(Folder.TMP, self.module)

            for date_dir in os.listdir(tmp_dir):
                if date_dir < self.date.strftime('%Y%m%d'):
                    tmp_file = join(tmp_dir, date_dir)
                    shutil.rmtree(tmp_file)

                    state = "succeed"
                    logging.info(f"Clear Tmp file: {tmp_file} status: {state}")

        except OSError:
            pass
    
    def clear_target(self) -> None:
        
        logging.info('Clear target file')
        
        for i, record in enumerate(self.logging):
            full_target = record['full_target']
            
            if os.path.exists(full_target):    
                self.achieve_backup(i, full_target)
            else:
                print("File does not exist")
    
    def achieve_backup(self, i, full_target:str) -> None:
        
        if self.backup is True:
            try:
                logging.info("Genarate backup file")
                
                root_dir = join(Folder.BACKUP, self.module)
                self.backup_dir = join(root_dir, self.date.strftime('%Y%m%d'))
                if not os.path.exists(self.backup_dir):
                    os.makedirs(self.backup_dir)
                
                ## read backup file
                full_backup = join(self.backup_dir, f"BK_{Path(full_target).stem}.csv")
                backup_df = self.read_csv_file(i, full_backup)
                
                ## read target file
                target_df  = self.read_csv_file(i, full_target)
                
            except FileNotFoundError:
                self.genarate_backup_file(full_target, full_backup)
        else:
            print('ok')
            
    def genarate_backup_file(self, full_target, full_backup) -> None:
        
        status = "skipped"
        try:
            shutil.copy2(full_target, full_backup)
            status = "succeed"
            logging.info(f"Backup file from {full_target} to {full_backup}, status {status}")
            
        except Exception:
            logging.info(f"No target file {full_target}, status {status}")
        
        
class CallFunction(Convert2File, CollectLog, CollectParams, BackupAndClear):
    pass
