from pathlib import Path
from os.path import join
import re
import pandas as pd
from functools import reduce
import logging
from .function import CallFunction
from .exception import CustomException
from .setup import setup_errorlog, CONFIG


class ModuleBOS(CallFunction):

    def __init__(self, params) -> None:
        self.module = "BOS"
        self.date = params.date
        self.manual = params.manual
        self.batch_date = params.batch_date
        self.store_tmp = params.store_tmp
        self.write_mode = params.write_mode
        self.clear = params.clear

    async def step_run(self) -> dict:

        logging.info(f'Module: "{self.module}", Manual: "{self.manual}", Batch Date: "{self.batch_date}", Store Tmp: "{self.store_tmp}", Write Mode: "{self.write_mode}"')

        result = {"module": self.module, "task": "Completed"}
        try:
            ## set params from confog file
            self.collect_params()

            ## clear
            if self.clear:
                self.clear_log()
                self.clear_backup()
                self.clear_tmp()

            ## backup
            # self.backup()

            ## run_process
            await self.check_source_file()
            await self.retrieve_data_from_source_file()
            await self.mock_data()
            if self.store_tmp is True:
                await self.write_data_to_tmp_file()
            await self.write_data_to_target_file()

        except CustomException as err:
            logging.error('See Error Details in "_error.log"')

            logger = setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.error(next(err))
                except StopIteration:
                    break

            result.update({"task": "Uncompleted"})

        logging.info("Stop Run Module\n")
        return result

    def logSetter(self, log: list) -> None:
        self._log = log

    def collect_params(self) -> None:

        status = "failed"
        record = {"module": self.module, "function": "collect_params", "status": status}

        logging.info(f'Set Params from config file for "{self.module}"')

        _log = []
        try:
            ## setup input dir / input file
            input_dir = CONFIG[self.module]["input_dir"]
            input_file = CONFIG[self.module]["input_file"]
                        
            specify_dir = "" ## x, y
            specify_dir = specify_dir.split(", ")
            add_dir = lambda x, y: x + specify_dir if y != [''] else x
            self.full_input = reduce(add_dir, [[join(input_dir, input_file)], specify_dir])
            
            ## setup output dir / output file 
            output_dir = CONFIG[self.module]["output_dir"]
            output_file = CONFIG[self.module]["output_file"]
            
            suffix = f"{self.batch_date.strftime('%Y%m%d')}"
            change_name =  lambda x: x if (self.write_mode == "overwrite" or self.manual) else f"{Path(x).stem}_{suffix}.csv"
            self.full_target = join(output_dir, change_name(output_file))

            status = "succeed"
            record.update({"status": status})

        except KeyError as err:
            err = f"Not found module: {err} in config file"
            record.update({"err": err})

        _log.append(record)
        self.logSetter(_log)

        if "err" in record:
            raise CustomException(err=self.logging)

    def collect_data(self, i: int, format_file: any) -> dict:

        status = "failed"
        module = self.logging[i]["module"]
        logging.info(f'Collect Data for "{module}"')

        self.logging[i].update({"function": "collect_data", "status": status})
        sheet_list = [sheet for sheet in format_file.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = format_file.sheet_by_name(sheets)
            for row in range(0, cells.nrows):
                by_sheets = [cells.cell(row, col).value for col in range(cells.ncols)]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        status = "succeed"
        self.logging[i].update({"status": status})

        return data

    async def mock_data(self) -> None:
        mock_data = [
            [
                "ApplicationCode",
                "AccountOwner",
                "AccountName",
                "AccountType",
                "EntitlementName",
                "SecondEntitlementName",
                "ThirdEntitlementName",
                "AccountStatus",
                "IsPrivileged",
                "AccountDescription",
                "CreateDate",
                "LastLogin",
                "LastUpdatedDate",
                "AdditionalAttribute",
                "Country",
            ],
            [
                "MOCK1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10",
                self.batch_date,
                self.date,
                self.batch_date,
                "14",
                "TH",
            ],
            [
                "MOCK2",
                "16",
                "17",
                "18",
                "19",
                "20",
                "21",
                "22",
                "23",
                "24",
                self.batch_date,
                self.date,
                self.batch_date,
                "28",
                "TH",
            ],
        ]
        df = pd.DataFrame(mock_data)
        df.columns = df.iloc[0].values
        df = df[1:]
        df = df.reset_index(drop=True)
        self.logging.append({"module": "Target_file", "data": df.to_dict("list")})
