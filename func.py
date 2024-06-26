from abc import ABC, abstractmethod
from datetime import datetime
from os.path import join
from module import convert_2_files
from setup import CONFIG, PARAMS
import logging
import re
class collect_log(ABC):
    def __init__(self):
        self._log = []

    @property
    def logging(self) -> list:
        return self._log

    @logging.setter
    def logging(self, log: list) -> None:
        self.log_setter(log)

    @abstractmethod
    def log_setter(self, log: list):
        pass

class collect_params:
    def params_setter(self, module:str) -> None:
        
        for key, value in PARAMS.items():
            setattr(self, key, value)

        self.module = module
        self.fmt_batch_date = self.batch_date
        self.date = datetime.now()
        self.input_dir = [join(CONFIG[self.module]["input_dir"], CONFIG[self.module]["input_file"])]
        # for i in CONFIG[self.module]["require"]:
        #     self.input_dir += [join(CONFIG[i]["input_dir"], CONFIG[i]["input_file"])]
        self.output_dir = CONFIG[self.module]["output_dir"]
        self.output_file = CONFIG[self.module]["output_file"]
    

class collect_data:
    
    def extract_adm_data(self, i, line):
        
        logging.info("Extract Data for ADM Module.")
        
        state = "failed"
        self.logging[i].update({"function": "extract_adm_data", "state": state})
        
        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [re.sub(r"\W\s+", "||", "".join(find_word).strip()).split("||")]
        
        state = "succeed"
        self.logging[i].update({"state": state})
        return  {"ADM": data}
    
    def extract_doc_data(self, i, line):
        
        logging.info("Extract Data for DOC Module.")
        
        state = "failed"
        self.logging[i].update({"function": "extract_doc_data", "state": state})
        
        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            print(find_word)
            # if find_word != []:
            #    data += [re.sub(r"\W\s+", "||", "".join(find_word).strip()).split("||")]
        
        # state = "succeed"
        # self.logging[i].update({"state": state})
        # return  {"DOC": data}
    
    def extract_lds_data(self, i, line):
        
        logging.info("Extract Data for LDS Module.")
        
        state = "failed"
        self.logging[i].update({"function": "extract_lds_data", "state": state})
        
        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [re.sub(r"\W\s+", ",", "".join(find_word).strip()).split(",")]
        
        fix_data = []
        for rows, value in enumerate(data):
            if rows == 0:
                fix_data += ["".join(value).split(" ")]
            else:
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
        return  {"LDS": fix_data}
        
        
class call_function(convert_2_files, collect_log, collect_params, collect_data):
    pass