import os
import types
from os.path import join
import logging
from datetime import datetime
import time
from .setup import Folder

class CustomException(Exception):
    
    def __init__(self,*args: tuple,**kwargs: dict,) -> None:
        self.__dict__.update(kwargs)

        for key, value in self.__dict__.items():
            setattr(self,key,value)
        
        self.date = datetime.now().strftime("%Y%m%d")
        self.time = time.strftime("%H%M%S")
        self.err_msg = self.generate_error()
        
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.err_msg)
    
    def setup_errorlog(self,
        log_format="%(asctime)s.%(msecs)03d | %(module)10s | %(levelname)8s | %(funcName)20s | %(message)s",
        log_name="", 
        file="log_error.log") -> any:
        
        log_name = log_name + "." + self.time
        filename = Folder.LOG + join(self.date, file)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError:
                pass
        
        file_handler = logging.FileHandler(filename, mode="a")
        file_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter(fmt=log_format,datefmt="%Y/%m/%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(file_handler)
            
        logger.file_handler = file_handler
        return logger

    def generate_error(self) -> any:
        for i in range(len(self.err)):
            module      = self.err[i]["module"]
            full_input  = self.err[i].get("input_dir")
            full_target = self.err[i].get("full_target")
            status      = self.err[i]["status"]
            func        = self.err[i]["function"]
            err         = self.err[i].get("err")
            if err is not None:
                err_msg = f'Module: "{module}"; File: "{full_input}"; Function: "{func}"; ERROR: {err}'
                yield err_msg
