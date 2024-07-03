from os.path import join
from datetime import datetime
import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException
from .setup import PARAMS, CONFIG, setup_errorlog

class ModuleBOS(CallFunction):

    def logSetter(self, log: list):
        self._log = log
        
    def paramsSetter(self, module: str) -> None:
        ## setup params
        for key, value in PARAMS.items():
            setattr(self, key, value)
        self.module = module
        self.date = datetime.now()
        self.input_dir = [join(CONFIG[module]["input_dir"], CONFIG[module]["input_file"])]
        self.output_dir = CONFIG[module]["output_dir"]
        self.output_file = CONFIG[module]["output_file"]

    async def Run(self, module: str) -> dict:
        self.paramsSetter(module)
        
        logging.info(f'Module: "{self.module}", Manual: "{self.manual}", Batch Date: "{self.batch_date}", Store Tmp: "{self.store_tmp}", Write Mode: "{self.write_mode}"')
        result = {"module": self.module, "task": "Completed"}
        
        try:
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
    
    def collect_data(self, i: int, format_file: any) -> dict:

        state = "failed"
        module = self.logging[i]["module"]
        logging.info(f"Collect Data for {module}")
        
        self.logging[i].update({"function": "collect_data",
                                "state": state})
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

        state = "succeed"
        self.logging[i].update({"state": state})
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
