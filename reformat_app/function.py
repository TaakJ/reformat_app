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
from .setup import Folder
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
    def logSetter(self, log: list): ...

class CollectParams(ABC):

    def __init__(self, module):
        print("Initialzing the module")
        self.module = module
        print(self.module)
        
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

class CollectBackup:

    def backup(self):

        print(self.manual)

        # self.root_dir = join(Folder.BACKUP, self.module)
        # self._date = self.date.strftime("%Y%m%d")
        # self._time = time.strftime("%H%M")

        # ## start backup
        # state = self.create_date_dir()
        # if state == "succeed":
        #     for date_dir in os.listdir(self.root_dir):
        #         if not date_dir.endswith(".zip"):
        #             self.zip_backup(date_dir)

        #     self.genarate_backup_file()

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

    def zip_backup(self, date_dir):
        if date_dir < self._date:
            zip_dir = join(self.root_dir, date_dir)
            zip_name = join(self.root_dir, f"{date_dir}.zip")

            with zipfile.ZipFile(join(self.root_dir, zip_name), "w", zipfile.ZIP_DEFLATED) as zf:
                for file in Path(zip_dir).rglob("*"):
                    if file.exists():
                        zf.write(file, file.relative_to(zip_dir))

            shutil.rmtree(zip_dir)
            state = "succeed"
            logging.info(f'Zip file name: "{zip_name}" from "{zip_dir}" status: "{state}"' )

    def genarate_backup_file(self):
        logging.info(f'Backup file from "{self.full_target}"')

        if glob.glob(self.full_target, recursive=True):
            backup_dir = join(self.root_dir, self._date)
            backup_file = f"BK_{Path(self.full_target).stem}_T{self._time}.csv"
            full_backup = join(backup_dir, backup_file)

            ## move output file to backup file
            shutil.copy2(self.full_target, full_backup)

            state = "succeed"
            logging.info(f'Backup file to "{full_backup}" status: "{state}"')


class ClearUp:

    # loaded = {}
    # def __new__(cls, module: str):
    #     if (params:= cls.loaded.get(module)) is not None:
    #         return params
    #     params = super().__new__(cls)
    #     cls.loaded[module] = params

    #     ## call function
    #     params.param_setter(module)
    #     return params

    def clear_log(self):
        for date_dir in os.listdir(Folder.LOG):
            if date_dir < self._date:
                log_dir = join(Folder.LOG, date_dir)
                shutil.rmtree(log_dir)

                state = "succeed"
                logging.info(f'Clear Log file: "{log_dir}" status: "{state}"')

    def clear_tmp(self):
        try:
            tmp_dir = join(Folder.TMP, self.module)
            for date_dir in os.listdir(tmp_dir):
                if date_dir < self._date:
                    tmp_file = join(tmp_dir, date_dir)
                    shutil.rmtree(tmp_file)

                    state = "succeed"
                    logging.info(f'Clear Tmp file: "{tmp_file}" status: "{state}"')
        except OSError:
            pass

    def clear_backup(self):
        try:
            backup_dir = join(Folder.BACKUP, self.module)
            for date_dir in os.listdir(backup_dir):
                if date_dir < self._date:
                    zip_dir = join(backup_dir, date_dir)
                    os.remove(zip_dir)

                    state = "succeed"
                    logging.info(f'Clear Zip file: "{zip_dir}" status: "{state}"')
        except OSError:
            pass


class CallFunction(Convert2File, CollectLog, CollectParams, CollectBackup, ClearUp):
    pass
