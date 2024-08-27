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

        logging.info(f"Module:'{self.module}'; Manual: '{self.manual}'; Run date: '{self.batch_date}'; Store tmp: '{self.store_tmp}'; Write mode: '{self.write_mode}';")

        result = {'module': self.module, 'task': 'Completed'}
        try:
            self.colloct_setup()

            if self.backup is True:
                self.achieve_backup()

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

            result.update({'task': 'Uncompleted'})

        logging.info(f"Stop Run Module '{self.module}'\r\n")

        return result

    def collect_user(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({'function': 'collect_user', 'status': status})
        
        try:
            data = []
            for line in format_file:
                data += [re.sub(r'(?<!\.)\|\|', '||', line.strip()).split("||")]
            
            ## set dataframe
            df = pd.DataFrame(data)
            df = df.apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## mapping data to column
            # 0:
            # 1:
            # 2:
            # 3:
            # 4:
            # 5:
            # 6:
            df = df.groupby(0)
            df = df.agg(lambda row: '+'.join(row.unique())).reset_index()
            
            set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            set_value.update({
                'ApplicationCode': 'ADM', 
                'AccountOwner': df[0],
                'AccountName': df[0],
                'AccountType': 'USR',
                'EntitlementName': df[[4, 5, 6]].apply(lambda row: ';'.join(row), axis=1),
                'AccountStatus': 'A',
                'IsPrivileged': 'N',
                'AccountDescription': df[1],
                'AdditionalAttribute': df[2],
                'Country': 'TH'
            })
            df = df.assign(**set_value).fillna('NA') 
            df = df.drop(df.iloc[:,:7].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f"Collect user from file: {self.logging[i]['full_input']}, status: {status}")
        
    def collect_param(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param', 'status': status})
        
        try:
            data = []
            for line in format_file:
                data += [re.sub(r'(?<!\.)\|\|', '||', line.strip()).split("||")]
            
            ## set dataframe
            df = pd.DataFrame(data)
            
            ## mapping data to column
            # 0:
            # 1:
            # 2:
            # 3:
            # 4:
            # 5:
            # 6:
            df = df.groupby(4)
            df = df.agg(lambda row: ','.join(row.unique())).reset_index()
            print(df[[4,5,6]])
            
            ## mapping data
            # set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            # set_value.update({
            #     'Parameter Name': df[4].unique(), 
            #     'Code value': df[5].unique(),
            #     'Decode value': df[6].unique(),
            # })
            

            # df = df.from_dict(set_value, orient='index')
            # print(df)
            # df = df.drop(df.iloc[:,:7].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f"Collect param from file: {self.logging[i]['full_input']}, status: {status}")
