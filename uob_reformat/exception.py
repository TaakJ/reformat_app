import os
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
        
        self.date = datetime.now().strftime('%Y%m%d')
        self.time = time.strftime('%H%M%S')
        self.err_msg = self.generate_error()
        
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.err_msg)
    
    def setup_errorlog(self,
        log_format="%(asctime)s.%(msecs)03d | %(module)15s | %(levelname)8s | %(funcName)20s | %(message)s",
        log_name="", 
        file="log_error.log") -> any:
        
        log_name = log_name + '.' + self.time
        filename = Folder.LOG + join(self.date, file)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError:
                pass
            
        # Configure both console and file logging
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.ERROR)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(logging.Formatter(fmt=log_format, datefmt='%Y/%m/%d %H:%M:%S'))
        logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler(filename, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(logging.Formatter(fmt=log_format, datefmt='%Y/%m/%d %H:%M:%S'))
        logger.addHandler(file_handler)
        
        return logger

    def generate_error(self) -> any:
        for i in range(len(self.err)):
            err_msg = self.err[i].get('err')
            if err_msg is not None:
                yield err_msg
