import re
import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException

class ModuleBOS(CallFunction):

    def __init__(self, params) -> None:
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
            
            # await self.check_source_file()
            # await self.separate_data_file()
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
    
    def read_mutiple_file(self, i:int):
        print(self.logging[i])
        
        
    def collect_user(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user', 'status': status})

        try:
            data = []
            for line in format_file:
                data += [re.sub(r'(?<!\.),', '||', line.strip()).split('||')]
            
            ## set dataframe
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## mapping data to column
            df['branch_code'] = df['branch_code'].apply(lambda row: '{:0>3}'.format(row))
            df[['Domain', 'user_name']] = df['user_name'].str.extract(r'^(.*?)\\(.*)$')
            
            self.read_mutiple_file(i)
            # df.apply(self.split_column, axis=1, result_type='expand')
            
            # set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            # set_value.update({
            #     'ApplicationCode': 'BOS',
            #     'AccountOwner': df['user_name'],
            #     'AccountName': df['user_name'],
            #     'AccountType': 'USR',
            #     # 'EntitlementName': df[[4, 6, 5]].apply(lambda row: ';'.join(row), axis=1),
            #     'AccountStatus': 'A',
            #     'IsPrivileged': 'N',
            #     'AccountDescription': df['employee_display_name'],
            #     'AdditionalAttribute': df['branch_code'],
            #     'Country': 'TH',
            # })
            # df = df.assign(**set_value).fillna('NA')
            # df = df.drop(df.iloc[:, :7].columns, axis=1)
            # print(df)
            
        except Exception as err:
            raise Exception(err)

        # status = 'succeed'
        # self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        # logging.info(f'Collect user data, status: {status}')
    
    def collect_param(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param', 'status': status})
        columns = self.logging[i]['columns']