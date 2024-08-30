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
        set_dir = lambda dir, file: [(join(dir, x.strip()) if self.write_mode == 'overwrite' or self.manual else join(dir, f"{Path(x.strip()).stem}_{suffix}.csv"))\
            for x in file.split(',')]
        
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
                for select_num in [n for n in self.select_files if i == n]:
                    
                    status = 'succeed'
                    if set(('full_input', 'full_target')).issubset(record):
                        copy_record = record.copy()
                        copy_record.update(
                            {
                                'full_input': [files[0]], 
                                'full_target': files[1],
                                'program': 'USER' if select_num == 1 else 'PARAM', 
                                'status': status,
                            }
                        )
                        if full_depend != []:
                            copy_record.setdefault('full_depend', full_depend)
                        
                        log += [copy_record]
                        
                    else:
                        record.update(
                            {
                                'full_input': [files[0]], 
                                'full_target': files[1],
                                'program': 'USER' if select_num == 1 else 'PARAM',
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
    def collect_user(self, i: int, format_file: any):
        pass
    
    @abstractmethod
    def collect_param(self, i: int, format_file: any):
        pass

class BackupAndClear:

    def achieve_backup(self) -> None:

        self.root_dir = join(Folder.BACKUP, self.module)
        self._date = self.date.strftime('%Y%m%d')
        self.time = time.strftime('%H%M%S')

        logging.info("Genarate backup file")
        self.backup_zip_file()
        self.backup_dir = join(self.root_dir, self._date)
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        
        list_of_files = glob.glob(f'{self.backup_dir}/*')
        for i, record in enumerate(self.logging):
            if list_of_files != []:
                try:
                    ## read target file
                    df  = self.read_csv_file(i, record['full_target'])
                    
                    ## read backup file
                    full_backup = join(self.backup_dir, f"BK_{Path(record['full_target']).stem}.csv")
                    bk_df = self.read_csv_file(i, full_backup)
                    
                    # Validate data change row by row
                    cmp_df = self.comparing_dataframes(i, bk_df, df)
                    if (cmp_df['count'] >= 1).any():
                        self.genarate_backup_file(record)
                    else:
                        logging.info("No backup file because no data was changed")
                        
                except FileNotFoundError:
                    self.genarate_backup_file(record)
            else:
                self.genarate_backup_file(record)
            
    def genarate_backup_file(self, record):
        status = "skipped"
        if glob.glob(record['full_target'], recursive=True):
            try:        
                backup_file = f"BK_{Path(record['full_target']).stem}.csv"
                full_backup = join(self.backup_dir, backup_file)
                shutil.copy2(record['full_target'], full_backup)
                
                record.update({'full_backup': full_backup})
                
                status = "succeed"
                logging.info(f"Backup file from {record['full_target']} to {full_backup}, status {status}")
                
            except Exception:
                pass
        else:
            logging.info(f"No target file {record['full_target']}, status {status}")

    def backup_zip_file(self):

        self.bk_date = self.date - timedelta(days=1)
        self._bk_date = self.bk_date.strftime('%Y%m%d')
        try:
            for date_dir in os.listdir(self.root_dir):
                zip_dir = join(self.root_dir, date_dir)
                
                if self._bk_date < date_dir:
                    ## zip backup file
                    if date_dir < self._date and not zip_dir.endswith(".zip"):
                        zip_name = join(self.root_dir, f"{date_dir}.zip")
                        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                            for file in Path(zip_dir).rglob("*"):
                                zf.write(file, file.relative_to(self.root_dir))
                        
                        shutil.rmtree(zip_dir)
                        logging.info(f"Zip file: {zip_name}")
                else:
                    ## clear backup file
                    if os.path.isfile(zip_dir):
                        os.remove(zip_dir)
                    else:
                        shutil.rmtree(zip_dir)
                    logging.info(f"Clear Zip file: {zip_dir}")
                    
        except OSError:
            pass
        
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

class CallFunction(Convert2File, CollectLog, CollectParams, BackupAndClear):
    pass
