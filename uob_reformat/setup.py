import argparse
import logging
import logging.config
import yaml
import os
from os.path import join
import shutil
from datetime import datetime, timedelta
import time

class ArgumentParams:
    SHORT_NAME = "short_name"
    NAME = "name"
    DESCRIPTION = "description"
    REQUIRED = "required"
    DEFAULT = "default"
    ISFLAG = "flag"
    TYPE = "type"
    CHOICES = "choices"

class Folder:
    _CURRENT_DIR        = os.getcwd()
    CONFIG              = join(_CURRENT_DIR,"config/")
    TEMPLATE            = join(_CURRENT_DIR,"template/")
    BACKUP              = join(_CURRENT_DIR,"backup/")
    TMP                 = join(_CURRENT_DIR,"tmp/")
    LOG                 = join(_CURRENT_DIR,"log/")
    _CONFIG_DIR         = join(CONFIG,"config.yaml")
    _LOGGER_CONFIG_DIR  = join(CONFIG,"logging_config.yaml")

def setup_folder() -> None:
    _folders = [value for name, value in vars(Folder).items() if isinstance(value,str) and not name.startswith("_")]
    for folder in _folders:
        os.makedirs(folder,exist_ok=True)

def setup_config() -> dict:
    config_yaml = None
    config_dir = Folder._CONFIG_DIR

    if os.path.exists(config_dir):
        with open(config_dir,"rb") as conf:
            config_yaml = yaml.safe_load(conf.read())
    else:
        raise FileNotFoundError(f"Yaml config file path: '{config_dir}' doesn't exist.")
    return config_yaml

def setup_log() -> None:
    config_yaml = None
    date = datetime.now().strftime("%Y%m%d")
    # _time = time.strftime("%H%M%S")
    # file = f"log_status_T{_time}.log"
    file = "log_status.log"
    
    filename = Folder.LOG + join(date, file)
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError:
            pass

    if os.path.exists(Folder._LOGGER_CONFIG_DIR):
        with open(Folder._LOGGER_CONFIG_DIR,"rb") as logger:
            config_yaml = yaml.safe_load(logger.read())

            for i in config_yaml["handlers"].keys():
                if "filename" in config_yaml["handlers"][i]:
                    config_yaml["handlers"][i]["filename"] = filename
            logging.config.dictConfig(config_yaml)
    else:
        raise FileNotFoundError(f"Yaml file file_path: '{Folder._LOGGER_CONFIG_DIR}' doesn't exist.")
    
def clear_log() -> None:
    bk_date = datetime.now() - timedelta(days=7) 
    
    for date_dir in os.listdir(Folder.LOG):
        if date_dir <= bk_date.strftime("%Y%m%d"):
            log_dir = join(Folder.LOG, date_dir)
            shutil.rmtree(log_dir)
            
            state = "succeed"
            logging.info(f'Clear Log file: {log_dir} status: {state}')

class SetupParser:
    def __init__(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.set_arguments()
        self.parsed_params = self.parser.parse_args()

    @staticmethod
    def get_args_list() -> list[dict]:
        return [
            {
                ArgumentParams.SHORT_NAME: "-s",
                ArgumentParams.NAME: "--source",
                ArgumentParams.DESCRIPTION: "-s: source",
                ArgumentParams.REQUIRED: False,
                ArgumentParams.ISFLAG: False,
                ArgumentParams.TYPE: lambda s: [x.strip().upper() for x in s.split(",")],
                ArgumentParams.DEFAULT: "adm,bos,cum,ica,iic,lds,lmt,moc",
            },
            {
                ArgumentParams.SHORT_NAME: "-m",
                ArgumentParams.NAME: "--manual",
                ArgumentParams.DESCRIPTION: "-m: manual",
                ArgumentParams.REQUIRED: False,
                ArgumentParams.ISFLAG: True,
                ArgumentParams.DEFAULT: False,
            },
            {
                ArgumentParams.SHORT_NAME: "-b",
                ArgumentParams.NAME: "--batch_date",
                ArgumentParams.DESCRIPTION: "format YYYY-MM-DD",
                ArgumentParams.REQUIRED: False,
                ArgumentParams.ISFLAG: False,
                ArgumentParams.TYPE: lambda d: datetime.strptime(d,"%Y-%m-%d").date(),
                ArgumentParams.DEFAULT: datetime.today().date(),
            },
            {
                ArgumentParams.SHORT_NAME: "-t",
                ArgumentParams.NAME: "--store_tmp",
                ArgumentParams.DESCRIPTION: "-t: not clear tmp",
                ArgumentParams.REQUIRED: False,
                ArgumentParams.ISFLAG: True,
                ArgumentParams.DEFAULT: False,
            },
            {
                ArgumentParams.SHORT_NAME: "-w",
                ArgumentParams.NAME: "--write_mode",
                ArgumentParams.DESCRIPTION: "-w: new",
                ArgumentParams.REQUIRED: False,
                ArgumentParams.ISFLAG: True,
                ArgumentParams.DEFAULT: "overwrite",
            },
            {
                ArgumentParams.SHORT_NAME: "-f",
                ArgumentParams.NAME: "--select_files",
                ArgumentParams.DESCRIPTION: "-f: select_files",
                ArgumentParams.REQUIRED: False,
                ArgumentParams.TYPE: lambda f: [x.strip() for x in f.split(",")],
                ArgumentParams.ISFLAG: False,
                ArgumentParams.DEFAULT: "1,2",
            },
        ]

    def set_arguments(self) -> None:
        # set arguments
        for args in self.get_args_list():
            short_name = args.get(ArgumentParams.SHORT_NAME)
            name = args.get(ArgumentParams.NAME)
            description = args.get(ArgumentParams.DESCRIPTION)
            required = args.get(ArgumentParams.REQUIRED)
            default = args.get(ArgumentParams.DEFAULT)
            choices = args.get(ArgumentParams.CHOICES)
            _type = args.get(ArgumentParams.TYPE)
            action = "store_true" if args.get(ArgumentParams.ISFLAG) else "store"

            if _type:
                self.parser.add_argument(short_name,name,help=description,required=required,default=default,type=_type)
            else:
                if action == "store_true":
                    self.parser.add_argument(short_name,name,help=description,required=required,default=default,action=action)
                else:
                    self.parser.add_argument(short_name,name,help=description,required=required,default=default,action=action,choices=choices)

class Utility:
    global PARAMS, CONFIG
    PARAMS       = vars(SetupParser().parsed_params)
    CONFIG       = setup_config()
