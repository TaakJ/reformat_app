import re
import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException

class ModuleADM(CallFunction):
    
    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)
            
    def logSetter(self, log: list) -> None:
        self._log = log
        
    async def run_process(self) -> dict:
        
        logging.info(f'Module:"{self.module}"; Manual: "{self.manual}"; Run Date: "{self.batch_date}"; Store Tmp: "{self.store_tmp}"; Write Mode: "{self.write_mode}";')
        
        result = {"module": self.module, "task": "Completed"}
        try:
            ## set params from confog file
            self._full_input = ""
            self.collect_params()
            
            ## backup file
            self.backup()
            
            ## step run function
            await self.check_source_file()
            await self.separate_data_file()
            await self.mock_data()
            if self.store_tmp is True:
                await self.genarate_tmp_file()
            # await self.genarate_target_file()

        except CustomException as err:
            logging.error('See Error Details: log_error.log')
            
            logger = err.setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.exception(next(err))
                    logger.newline()
                except StopIteration:
                    break
            
            result.update({"task": "Uncompleted"})
            
        logging.info(f'Stop Run Module "{self.module}"\r\n')
        
        return result
    
    def collect_data(self, i: int, format_file: any) -> dict:
        
        status = "failed"
        module = self.logging[i]["module"]
        logging.info(f'Collect Data for module: {module}')
        
        self.logging[i].update({"function": "collect_data","status": status})

        data = []
        for line in format_file:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(line)
            if find_word != []:
                data += [re.sub(r"\W\s+","||","".join(find_word).strip()).split("||")]

        status = "succeed"
        self.logging[i].update({"status": status})
        
        return {module: data}

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
                "ADM",
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
                "ADM",
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
