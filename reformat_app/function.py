from abc import ABC, abstractmethod
import re
import logging
from datetime import datetime
from os.path import join
from .module import Convert2File
from .setup import CONFIG, PARAMS, Folder

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

class SetterParams(ABC):
    
    @abstractmethod
    def paramsSetter(self, module: str):
        pass

    def get_extract_data(self, i: int, format_file: any) -> dict:

        logging.info("Extract Data Each Module")
        module = self.logging[i]["module"]

        if module == "ADM":
            data = self.extract_adm(i, format_file)
            return data
        elif module == "DOC":
            data = self.extract_doc(i, format_file)
            return data
        elif module == "LDS":
            data = self.extract_lds(i, format_file)
            return data
        elif module == "BOS":
            data = self.extract_bos(i, format_file)
            return data
        elif module == "CUM":
            data = self.extract_cum(i, format_file)
            return data
        elif module == "ICA":
            data = self.extract_ica(i, format_file)
            return data
        elif module == "IIC":
            data = self.extract_iic(i, format_file)
            return data
        elif module == "LMT":
            data = self.extract_lmt(i, format_file)
            return data
        elif module == "MOC":
            data = self.extract_moc(i, format_file)
            return data

    def extract_adm(self, i: int, format_file: any) -> dict:

        logging.info("Data for ADM")

        state = "failed"
        self.logging[i].update({"function": "extract_adm", "state": state})

        data = []
        for line in format_file:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(line)
            if find_word != []:
                data += [
                    re.sub(r"\W\s+","||","".join(find_word).strip()).split("||")]

        state = "succeed"
        self.logging[i].update({"state": state})

        return {"ADM": data}

    def extract_doc(self, i: int, format_file: any) -> dict:

        logging.info("Data for DOC")

        state = "failed"
        self.logging[i].update({"function": "extract_doc", "state": state})

        data = []
        for line in format_file:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(line)
            if find_word != []:
                data += [
                    re.sub(r"\W\s+","||","".join(find_word).strip()).split("||")]

        fix_data = []
        for rows, value in enumerate(data):
            if rows == 0:
                continue
            elif rows == 1:
                ## header
                fix_data += [" ".join(value).split(" ")]
            else:
                ## value
                fix_column = []
                for idx, column in enumerate(value, 1):
                    if idx == 4:
                        l = re.sub(r"\s+", ",", column).split(",")
                        fix_column.extend(l)
                    else:
                        fix_column.append(column)
                fix_data.append(fix_column)

        state = "succeed"
        self.logging[i].update({"state": state})
        return {"DOC": fix_data}

    def extract_lds(self, i: int, format_file: any) -> dict:

        logging.info("Data for LDS")

        state = "failed"
        self.logging[i].update({"function": "extract_lds", "state": state})

        data = []
        for line in format_file:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(line)
            if find_word != []:
                data += [re.sub(r"\W\s+", ",", "".join(find_word).strip()).split(",")]

        fix_data = []
        for rows, value in enumerate(data):
            if rows == 0:
                ## header
                fix_data += [" ".join(value).split(" ")]
            else:
                ## value
                fix_column = []
                for idx, column in enumerate(value, 1):
                    if idx == 1:
                        l = re.sub(r"\s+", ",", column).split(",")
                        fix_column.extend(l)
                    elif idx == 32:
                        continue
                    else:
                        fix_column.append(column)
                fix_data.append(fix_column)

        state = "succeed"
        self.logging[i].update({"state": state})
        return {"LDS": fix_data}

    def extract_bos(self, i: int, format_file: any) -> dict:

        logging.info("Data for BOS")

        state = "failed"
        self.logging[i].update({"function": "extract_bos", "state": state})

        sheet_list = [sheet for sheet in format_file.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_cum(self, i: int, format_file: any) -> dict:

        logging.info("Data for CUM")

        state = "failed"
        self.logging[i].update({"function": "extract_cum", "state": state})

        sheet_list = [sheet for sheet in format_file.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)][
                    1:
                ]
                if not all(empty == "" for empty in by_sheets):
                    if sheets not in data:
                        data[sheets] = [by_sheets]
                    else:
                        data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_ica(self, i: int, format_file: any) -> dict:

        logging.info("Data for ICA")

        state = "failed"
        self.logging[i].update({"function": "extract_ica", "state": state})

        sheet_list = [sheet for sheet in format_file.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_iic(self, i: int, format_file: any) -> dict:

        logging.info("Data for IIC")

        state = "failed"
        self.logging[i].update({"function": "extract_iic", "state": state})

        sheet_list = [sheet for sheet in format_file.sheet_names() if sheet != "StyleSheet"]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)]
                if not all(empty == "" for empty in by_sheets):
                    if sheets not in data:
                        data[sheets] = [by_sheets]
                    else:
                        data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_lmt(self, i: int, format_file: any) -> dict:

        logging.info("Data for LMT")

        state = "failed"
        self.logging[i].update({"function": "extract_lmt", "state": state})

        sheet_list = [sheet for sheet in format_file.sheet_names() if sheet != "StyleSheet"]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_moc(self, i: int, format_file: any) -> dict:

        logging.info("Data for MOC")

        state = "failed"
        self.logging[i].update({"function": "extract_moc", "state": state})

        sheet_list = [sheet for sheet in format_file.sheet_names() if sheet != "StyleSheet"]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)]
                if not all(empty == "" for empty in by_sheets):
                    if sheets not in data:
                        data[sheets] = [by_sheets]
                    else:
                        data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

class CollectParams(SetterParams):
    pass

# import schedule
import os
import tarfile
import time
class CollectBackup:
    def __init__(self) -> None:
        print(self)
        
    def backup_folder(self):
        
        date = self.date.date().strftime("%Y%m%d")
        hour = time.strftime("%H")
        
        _folder = Folder.BACKUP + join(date, hour)
        print(_folder)
        
        # if not os.path.exists(os.path.dirname(filename)):
        #     try:
        #         os.makedirs(os.path.dirname(filename))
        #     except OSError:
        #         pass
class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
