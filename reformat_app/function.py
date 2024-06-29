from abc import ABC,abstractmethod
import re
import logging
from datetime import datetime
from os.path import join
from .module import Convert2File
from .setup import CONFIG, PARAMS

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

class SetParams:
    
    def get_params(self, module) -> None:
        for key, value in PARAMS.items():
            setattr(self, key, value)
            
        self.module         = module
        self.fmt_batch_date = self.batch_date
        self.date           = datetime.now()
        self.input_dir      = [join(CONFIG[module]["input_dir"], CONFIG[module]["input_file"])]
        self.output_dir     = CONFIG[module]["output_dir"]
        self.output_file    = CONFIG[module]["output_file"]
        
    def get_function(self, i:int, line:any):
        module = self.logging[i]["module"]
        
        if module == "ADM":
            self.extract_adm_data(i, line)
        elif module == "DOC":
            self.extract_doc_data(i, line)
        
    def extract_adm_data(self, i:int, line:any) -> dict:

        logging.info("Extract Data for ADM Module.")

        state = "failed"
        self.logging[i].update({"function": "extract_adm_data","state": state})

        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [re.sub(r"\W\s+","||","".join(find_word).strip(),).split("||")]

        state = "succeed"
        self.logging[i].update({"state": state})
        
        return {"ADM": data}
    
    def extract_doc_data(self, i:int, line:any) -> dict:

        logging.info("Extract Data for DOC Module.")

        state = "failed"
        self.logging[i].update({"function": "extract_doc_data","state": state})

        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [re.sub(r"\W\s+","||","".join(find_word).strip(),).split("||")]

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
                for idx, column in enumerate(value,1):
                    if idx == 4:
                        l = re.sub(r"\s+",",",column).split(",")
                        fix_column.extend(l)
                    else:
                        fix_column.append(column)
                fix_data.append(fix_column)

        state = "succeed"
        self.logging[i].update({"state": state})
        
        return {"DOC": fix_data}

class CollectParams(SetParams):
    pass

class CallFunction(Convert2File, CollectLog, CollectParams):
    pass
