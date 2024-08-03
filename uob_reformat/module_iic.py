import re
import logging
from .function import CallFunction
from .exception import CustomException
import pandas as pd

class ModuleIIC(CallFunction):

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
            self.collect_params()
            
            ## backup file
            # self.backup()
            
            ## step run function
            await self.check_source_file()
            await self.separate_data_file()
            # if self.store_tmp is True:
            #     await self.genarate_tmp_file()
            # await self.genarate_target_file()

        except CustomException as err:
            logging.error('See Error Details: log_error.log')

            logger = err.setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.exception(next(err))
                except StopIteration:
                    break

            result.update({"task": "Uncompleted"})

        logging.info(f'Stop Run Module "{self.module}"\r\n')
        
        return result

    def collect_data(self, i: int, format_file: any) -> dict:

        module = self.logging[i]["module"]
        logging.info(f'Collect Data for module: {module}')
        
        self.logging[i].update({"function": "collect_data"})
        
        data = []
        for line in format_file:
            line = line.strip().replace('"','')
            data += [re.sub(r'(?<!\w),', ",", line).split(",")]
        
        df = pd.DataFrame(data)
        df.columns = df.iloc[0].values
        df = df[1:]
        df = df.reset_index(drop=True)
        print(df)
        # change_df = self.initial_data_type(df)
        
        # self.logging[i].update({"status": status})
        
        # return {module: data}
