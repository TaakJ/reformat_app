from setup import Folder
import logging
import os
from os.path import join
from datetime import datetime

class CustomException(Exception):
    def __init__(self, *args:tuple, **kwargs:dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
        
        self.x = args[0]
        self._logging = self.setup_err()
        
        self.err_msg = self.generate_error()
        
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.err_msg)
            
    def setup_err(self,
                log_format =  "%(asctime)s.%(msecs)03d | %(module)s | %(levelname)s | %(funcName)s::%(lineno)d | %(message)s",
                log_name  = '',
                file = "_error"
                ):
        
        date = datetime.today().strftime("%Y%m%d")
        filename = Folder.LOG + join(date, file)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError:
                pass
        
        log  = logging.getLogger(log_name)
        formatter = logging.Formatter(fmt=log_format,
                                    datefmt="%Y/%m/%d %H:%M:%S")
        
        file_handler = logging.FileHandler(filename, mode="w")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.CRITICAL)
        log.addHandler(file_handler)
        
        log.setLevel(logging.INFO)
        return log
    
    
    def generate_error(self) -> any:
        yield self.x
        
        # try:
        #     for i in range(len(self.err)):
        #         err_msg = f'''Module::{self.err[i]["module"]} 
        #                     , Path::{self.err[i]["input_dir"]}
        #                     , Function::{self.err[i]["function"]}
        #                     , Status::{self.err[i]["state"]}
        #                     , Error::{self.err[i].get("errors", "No Error")}
        #                     '''
        #         yield err_msg
        # except:
        #     pass