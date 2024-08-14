import re
import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException

class ModuleLDS(CallFunction):

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

        status = 'failed'
        self.logging[i].update({'function': 'collect_user', 'status': status})
        
        try:
            data = []
            for line in format_file:
                regex = re.compile(r'\w+.*')
                find_word = regex.findall(line.strip())
                if find_word != []:
                    data += [re.sub(r'(?<!\.),', '||', ''.join(find_word)).split('||')]
            
            clean_data = []
            for rows, _data in enumerate(data):
                if rows == 0:
                    clean_data += [re.sub(r'\s+', ',', ','.join(_data)).split(',')]
                else:
                    fix_value = []
                    for idx, value in enumerate(_data, 1):
                        if idx == 1:
                            value = re.sub(r'\s+', ',', value).split(',')
                            fix_value.extend(value)
                        else:
                            fix_value.append(value)
                    clean_data.append(fix_value)
            
            ## set dataframe
            df = pd.DataFrame(clean_data)
            df.columns = df.iloc[0].values
            df = df.iloc[1:,:-1].apply(lambda x: x.str.strip()).reset_index(drop=True)
            
            ## mapping data
            # df = df[df['User_Active'] == 'Active'].reset_index()
            # set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            # set_value.update({
            #     'ApplicationCode': 'LDS', 
            #     'AccountOwner': df['UserName'], 
            #     'AccountName': df['FullName'],
            #     'AccountType': 'USR',
            #     'AccountStatus': 'A',
            #     'IsPrivileged': 'N',
            #     'LastLogin': pd.to_datetime(df['LastLogin_Date'].apply(lambda x: x[:19])).dt.strftime('%Y%m%d%H%M%S'),
            #     'LastUpdatedDate': pd.to_datetime(df['edit_date'].apply(lambda x: x.replace('NULL','')[:19])).dt.strftime('%Y%m%d%H%M%S'),
            #     'AdditionalAttribute': df[['CostCenterName','CostCenterCode']].apply(lambda x: '#'.join(x), axis=1),
            #     'Country': "TH"
            # })
            # df = df.assign(**set_value).fillna('NA')
            # df = df.drop(df.iloc[:,:32].columns, axis=1)
            # print(df)
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f"Collect user from file: {self.logging[i]['full_input']}, status: {status}")
        
    def collect_param(self, i: int, format_file: any) -> dict:
        
        status = "failed"
        self.logging[i].update({'function': 'collect_param', 'status': status})
        columns = self.logging[i]['columns']