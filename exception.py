import logging
from datetime import datetime
import os
from os.path import join
from setup import Folder

class CustomException(Exception):
    def __init__(self, *args:tuple, **kwargs:dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
        
        self.err_msg = self.generate_error()
        
    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self.err_msg)
    
    def generate_error(self) -> any:
        try:
            for i in range(len(self.err)):
                if self.err[i].get("err") is not None:
                    err_msg = f'Module::"{self.err[i]["module"]}", Path::"{self.err[i]["input_dir"]}", Function::"{self.err[i]["function"]}", Status::"{self.err[i]["state"]}", Errors::"{self.err[i]["err"]}"'
                    yield err_msg
        except:
            raise