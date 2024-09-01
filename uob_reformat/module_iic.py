import re
import logging
from .non_functional import CallFunction
from .exception import CustomException
import pandas as pd

class ModuleIIC(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)

    def logSetter(self, log: list) -> None:
        self._log = log

    async def run_process(self) -> dict:

        logging.info(f"Module:'{self.module}'; Manual: '{self.manual}'; Run date: '{self.batch_date}'; Store tmp: '{self.store_tmp}'; Write mode: '{self.write_mode}';")

        result = {'module': self.module, 'task': 'Completed'}
        try:
            self.colloct_setup()
            self.clear_target_file()

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

            result.update({'task': 'Uncompleted'})

        logging.info(f"Stop Run Module '{self.module}'\r\n")

        return result

    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})

        try:
            data = []
            for line in format_file:
                find_word = line.strip().replace('"', '')
                data += [re.sub(r'(?<!\.),', ',', ''.join(find_word)).split(',')]

            ## set dataframe
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': user_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            data = []
            for line in format_file:
                find_word = line.strip().replace('"', '')
                data += [re.sub(r'(?<!\.),', ',', ''.join(find_word)).split(',')]

            ## set dataframe
            param_df = pd.DataFrame(data)
            param_df.columns = param_df.iloc[0].values
            param_df = param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': param_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user param, status: {status}')
