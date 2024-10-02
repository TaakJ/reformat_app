from abc import ABC, abstractmethod
import os
import shutil
import logging
from datetime import timedelta
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
        input_dir = CONFIG[self.module]["input_dir"]
        input_file = CONFIG[self.module]["input_file"]

        def set_dir(dir, file):
            return [join(dir, x.strip()) for x in file.split(",")]
        full_input = set_dir(input_dir, input_file)

        full_depend = []
        depend_file = CONFIG[self.module].get("depend_file")
        if depend_file is not None:
            full_depend = set_dir(input_dir, depend_file)

        return full_input, full_depend

    def full_target(self) -> list:
        output_dir = CONFIG[self.module]["output_dir"]
        output_file = CONFIG[self.module]["output_file"]

        suffix = self.batch_date.strftime("%Y%m%d")
        def set_dir(dir, file):
            return [join(dir, x.strip()) if self.write_mode == "overwrite" or self.manual else join(dir, f"{Path(x.strip()).stem}_{suffix}.csv") for x in file.split(",")]
        full_target = set_dir(output_dir, output_file)

        return full_target

    def collect_setup(self) -> None:
        logging.info(f"Setup params/logging for module: {self.module}")

        log = []
        status = "failed"
        record = {"module": self.module, "function": "collect_setup", "status": status}

        try:
            full_input, full_depend = self.full_input()
            full_target = self.full_target()

            if len(full_input) == len(full_target):
                mapping_list = list(zip(full_input, full_target))
            else:
                mapping_list = [(input, target) for input in full_input for target in full_target]

            # 0: input file
            # 1: target file
            for i, files in enumerate(mapping_list, 1):
                for select_num in [num for num in self.select_files if i == num]:
                    status = "succeed"
                    if set(("full_input", "full_target")).issubset(record):
                        copy_record = record.copy()
                        copy_record.update(
                            {
                                "full_input": files[0],
                                "full_target": files[1],
                                "package": "USER" if select_num == 1 else "PARAM",
                                "status": status,
                            }
                        )
                        if full_depend != []:
                            copy_record.setdefault("full_depend", full_depend)

                        log += [copy_record]

                    else:
                        record.update(
                            {
                                "full_input": files[0],
                                "full_target": files[1],
                                "package": "USER" if select_num == 1 else "PARAM",
                                "status": status,
                            }
                        )
                        if full_depend != []:
                            record.setdefault("full_depend", full_depend)

                        log = [record]

        except Exception as err:
            record.update({"err": err})
            log += [record]

        self.logSetter(log)

        if "err" in record:
            raise CustomException(err=self.logging)

    @abstractmethod
    def collect_user_file(self, i: int, format_file: any) -> None:
        pass

    @abstractmethod
    def collect_param_file(self, i: int, format_file: any) -> None:
        pass


class BackupAndClear:
    def clear_target_file(self) -> None:
        status = "skipped"
        for i, record in enumerate(self.logging):
            if os.path.exists(record["full_target"]):
                try:
                    if self.backup is True:
                        self.achieve_backup(i, record["full_target"])

                    os.remove(record["full_target"])
                    status = "succeed"

                except Exception:
                    pass

            logging.info(f"Clear target file {record["full_target"]}, status {status}")

    def achieve_backup(self, i, full_target: str) -> None:
        try:
            try:
                root_dir = join(Folder.BACKUP, self.module)
                bk_date = self.date - timedelta(days=1)

                for date_dir in os.listdir(root_dir):
                    if date_dir <= bk_date.strftime("%Y%m%d"):
                        del_backup = join(root_dir, date_dir)
                        shutil.rmtree(del_backup)

            except OSError:
                pass

            backup_dir = join(root_dir, self.date.strftime("%Y%m%d"))
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            ## read backup file
            full_backup = join(backup_dir, f"BK_{Path(full_target).stem}.csv")
            backup_df = self.read_csv_file(i, full_backup)

            ## read target file
            target_df = self.read_csv_file(i, full_target)

            ## Validate data change row by row
            cmp_df = self.comparing_dataframe(i, backup_df, target_df)
            if (cmp_df["count"] >= 1).any():
                self.genarate_backup_file(full_target, full_backup)
            else:
                logging.info(f"No backup file {full_target} because no data was changed")

        except FileNotFoundError:
            self.genarate_backup_file(full_target, full_backup)

    def genarate_backup_file(self, full_target, full_backup) -> None:
        status = "skipped"
        try:
            shutil.copy2(full_target, full_backup)
            status = "succeed"
            logging.info(f"Backup file from {full_target} to {full_backup}, status {status}")

        except Exception:
            logging.info(f"No target file {full_target}, status {status}")

    def clear_tmp_file(self) -> None:
        try:
            tmp_dir = join(Folder.TMP, self.module)
            for date_dir in os.listdir(tmp_dir):
                if date_dir < self.date.strftime("%Y%m%d"):
                    full_tmp = join(tmp_dir, date_dir)

                    shutil.rmtree(full_tmp)
                    status = "succeed"

                    logging.info(f"Clear Tmp file: {full_tmp} status: {status}")

        except OSError:
            pass


class CallFunction(Convert2File, CollectLog, CollectParams, BackupAndClear):
    pass
