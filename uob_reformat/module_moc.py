import re
import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException

class ModuleMOC(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)
            
    def logSetter(self, log: list) -> None:
        self._log = log
    
    async def run_process(self) -> dict:

        logging.info(f'Module:"{self.module}"; Manual: "{self.manual}"; Run date: "{self.batch_date}"; Store tmp: "{self.store_tmp}"; Write mode: "{self.write_mode}";')

        result = {"module": self.module, "task": "Completed"}
        try:
            self.colloct_setup()
            
            if self.backup is True:
                self.achieve_backup()
            
            await self.check_source_file()
            await self.separate_data_file()
            if self.store_tmp is True:
                await self.genarate_tmp_file()
            await self.genarate_target_file()

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
        
        status = "failed"
        self.logging[i].update({"function": "collect_data", "status": status})
        
        try:
            data = []
            for line in format_file:
                find_word = line.strip().replace('"','')
                data += [re.sub(r'(?<!\w),', ",", find_word).split(",")]
            
            ## set dataframe
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            df = self.set_initial_data_type(i, df)
            
        except Exception as err:
            raise Exception(err)

        status = "succeed"
        self.logging[i].update({"data": df.to_dict("list"), "status": status})
        logging.info(f'Collect data from file: {self.logging[i]["full_input"]}, status: {status}')