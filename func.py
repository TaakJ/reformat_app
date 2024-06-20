from abc import ABC, abstractmethod
from datetime import datetime
from os.path import join
from module import convert_2_files


class record_log(ABC):
    def __init__(self):
        self._log = []

    @property
    def logging(self) -> list:
        return self._log

    @logging.setter
    def logging(self, log: list) -> None:
        self._log_setter(log)

    @abstractmethod
    def _log_setter(self, log: list):
        pass

class parameter: 
    def _params_setter(self, module:str, _params:dict) -> None:
        for key, value in _params.items():
            setattr(self, key, value)

        self.module = module      
        self.input_dir = [join(self.config[self.module]["input_dir"], self.config[self.module]["input_file"])]
        
        for i in self.config[self.module]["require"]:
            self.input_dir += [join(self.config[i]["input_dir"], self.config[i]["input_file"])]
            
        self.output_dir = self.config[self.module]["output_dir"]
        self.output_file = self.config[self.module]["output_file"]
        
        self.fmt_batch_date = self.batch_date
        self.date = datetime.now()
    

class call_function(convert_2_files, record_log, parameter):
    pass