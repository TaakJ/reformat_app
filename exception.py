from setup import Folder
import logging
import logging.config
import yaml
import os
from os.path import join
from datetime import datetime

class CustomException(Exception):
    def __init__(self,*args: tuple, **kwargs: dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
        
        self.x = args[0]
        self.setup_error()
        self.err_msg = self.generate_error()
        
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.err_msg)
    
    def setup_error(self) -> None:
        config_yaml  = None
        date = datetime.today().strftime("%d%m%y")
        file = "_error"
        filename = Folder.LOG + join(date, file)
        
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError:
                pass
            
        if os.path.exists(Folder._LOGGER_CONFIG_DIR):
            with open(Folder._LOGGER_CONFIG_DIR, 'rb') as logger:
                config_yaml  = yaml.safe_load(logger.read())
                
                for i in (config_yaml["handlers"].keys()):
                    if "filename" in config_yaml['handlers'][i]:
                        config_yaml["handlers"][i]["filename"] = filename
                
                config_yaml["root"]["level"] = "ERROR"
                logging.config.dictConfig(config_yaml)
        else:
            raise Exception(f"Yaml file file_path: '{Folder._LOGGER_CONFIG_DIR}' doesn't exist.")
            

    def generate_error(self) -> any:
        print(self.x)
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


# def setup_log() -> None:
#     config_yaml  = None
#     date = datetime.today().strftime("%d%m%y")
#     file = "_success"
    
#     filename = Folder.LOG + join(date, file)
#     if not os.path.exists(os.path.dirname(filename)):
#         try:
#             os.makedirs(os.path.dirname(filename))
#         except OSError as err:
#             pass

#     if os.path.exists(Folder._LOGGER_CONFIG_DIR):
#         with open(Folder._LOGGER_CONFIG_DIR, 'rb') as logger:
#             config_yaml  = yaml.safe_load(logger.read())
            
#             for i in (config_yaml["handlers"].keys()):
#                 if "filename" in config_yaml['handlers'][i]:
#                     config_yaml["handlers"][i]["filename"] = filename
#             logging.config.dictConfig(config_yaml)
#     else:
#         raise Exception(f"Yaml file file_path: '{Folder._LOGGER_CONFIG_DIR}' doesn't exist.")