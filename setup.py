import argparse
import logging.config
import yaml
import os
from os.path import join
from datetime import datetime

CURRENT_DIR = os.getcwd()
LOGGER_CONFIG = join(CURRENT_DIR, "logging_config.yaml")
class Folder:
    RAW         = join(CURRENT_DIR, "RAW/")
    EXPORT      = join(CURRENT_DIR, "EXPORT/")
    TEMPLATE    = join(CURRENT_DIR, "TEMPLATE/")
    TMP         = join(CURRENT_DIR, "TMP/")
    LOG         = join(CURRENT_DIR, "LOG/")
    LIST_FILE   = ['ADM.txt', 'BOS.xlsx', 'CUM.xls', 'DocImage.txt', 'ICAS-NCR.xlsx', 'IIC.xlsx', 'LDS-P_UserDetail.txt', 'Lead-Management.xlsx', 'MOC.xlsx']
    _FILE       = "schedule_export.xlsx"
class ArgumentParams:
    SHORT_NAME = 'short_name'
    NAME = 'name'
    DESCRIPTION = 'description'
    REQUIRED = 'required'
    DEFAULT = 'default'
    ISFLAG = 'flag'
    TYPE = 'type'
    CHOICES = 'choices'

class setup_parser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.set_arguments()
        self.parsed_params = self.parser.parse_args()

    @staticmethod
    def get_args_list() -> list[dict]:
        return [
            {
                ArgumentParams.SHORT_NAME : "-m",
                ArgumentParams.NAME : "--manual",
                ArgumentParams.DESCRIPTION : "-m: manual",
                ArgumentParams.REQUIRED : False,
                ArgumentParams.ISFLAG : True,
                ArgumentParams.DEFAULT: False
            },
            {
                ArgumentParams.SHORT_NAME : "-b",
                ArgumentParams.NAME : "--batch_date",
                ArgumentParams.DESCRIPTION : "format YYYY-MM-DD",
                ArgumentParams.REQUIRED : False,
                ArgumentParams.ISFLAG : False,
                ArgumentParams.TYPE : lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
                ArgumentParams.DEFAULT : datetime.today().date()
            },
            {
                ArgumentParams.SHORT_NAME : "-t",
                ArgumentParams.NAME : "--store_tmp",
                ArgumentParams.DESCRIPTION : "-t: not clear tmp",
                ArgumentParams.REQUIRED : False,
                ArgumentParams.ISFLAG : True,
                ArgumentParams.DEFAULT: False
            },
            {
                ArgumentParams.SHORT_NAME : "-w",
                ArgumentParams.NAME : "--write_mode",
                ArgumentParams.DESCRIPTION : "-w: new",
                ArgumentParams.REQUIRED : False,
                ArgumentParams.ISFLAG : True,
                ArgumentParams.DEFAULT: "overwrite"
            }
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
                self.parser.add_argument(short_name, name, help=description, required=required,
                                    default=default, type=_type)
            else:
                if action == "store_true":
                    self.parser.add_argument(short_name, name, help=description, required=required,
                                        default=default, action=action)
                else:
                    self.parser.add_argument(short_name, name, help=description, required=required,
                                        default=default, action=action, choices=choices)

def setup_log() -> None:
    config_yaml  = None
    date = datetime.today().strftime("%d%m%Y")
    log_name = f'log_{date}.log'

    if os.path.exists(LOGGER_CONFIG):
        with open(LOGGER_CONFIG, 'rb') as logger:
            config_yaml  = yaml.safe_load(logger.read())
            for i in (config_yaml["handlers"].keys()):
                if "filename" in config_yaml['handlers'][i]:
                    log_path = config_yaml["handlers"][i]["filename"]
                    log_file = log_path + log_name
            config_yaml["handlers"][i]["filename"] = log_file

            logging.config.dictConfig(config_yaml)
    else:
        raise Exception(f"Yaml file file_path: '{LOGGER_CONFIG}' doesn't exist.")

def setup_folder() -> None:
    _folders = [name for name, value in vars(Folder).items() if isinstance(value, str) and not name.startswith("_") and name]
    for folder in _folders:
        os.makedirs(folder, exist_ok=True)

def clear_tmp() -> None:
    _folders = [value for name, value in vars(Folder).items() if isinstance(value, str) and not name.startswith("_") and value.endswith("TMP/")]
    for file_path in [join(folder, files) for folder in _folders for files in os.listdir(folder) if os.path.isfile(join(folder, files))]:
        os.remove(file_path)