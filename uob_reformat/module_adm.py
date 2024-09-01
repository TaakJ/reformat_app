import re
import pandas as pd
import logging
from .non_functional import CallFunction
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
                data += [re.sub(r'(?<!\.)\|\|', '||', line.strip()).split('||')]

            ## set dataframe
            columns = ['User-ID','User Full Name','Department code','Employee ID','Group','Zone','Role']
            user_df = pd.DataFrame(data, columns=columns)
            user_df = user_df.apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # group by column
            user_df = user_df.groupby('User-ID', sort=False)
            user_df = user_df.agg(lambda row: '+'.join(row.unique())).reset_index()

            ## mapping data to column
            set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            set_value.update(
                {
                    'ApplicationCode': 'ADM',
                    'AccountOwner': user_df['User-ID'],
                    'AccountName': user_df['User-ID'],
                    'AccountType': 'USR',
                    'EntitlementName': user_df[['Group', 'Role', 'Zone']].apply(lambda row: ';'.join(row), axis=1),
                    'AccountStatus': 'A',
                    'IsPrivileged': 'N',
                    'AccountDescription': user_df['User Full Name'],
                    'AdditionalAttribute': user_df['Department code'],
                    'Country': 'TH',
                }
            )
            user_df = user_df.assign(**set_value)
            user_df = user_df.drop(user_df.iloc[:, :7].columns, axis=1)

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
                data += [re.sub(r'(?<!\.)\|\|', '||', line.strip()).split('||')]

            ## set dataframe
            columns = ['User-ID','User Full Name','Department code','Employee ID','Group','Zone','Role']
            param_df = pd.DataFrame(data, columns=columns)
            param_df = param_df.apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## mapping data to column
            set_value = [
                {
                    'Parameter Name': 'GroupDetail', 
                    'Code value': param_df['Group'].unique(), 
                    'Decode value': param_df['Group'].unique()
                },
                {
                    'Parameter Name': 'RoleDetail', 
                    'Code value': param_df['Role'].unique(), 
                    'Decode value': param_df['Role'].unique()
                },
                {
                    'Parameter Name': 'Zone', 
                    'Code value': param_df['Zone'].unique(), 
                    'Decode value': param_df['Zone'].unique()
                },
                {
                    'Parameter Name': 'Department', 
                    'Code value': param_df['Department code'].unique(), 
                    'Decode value': param_df['Department code'].unique()
                }
            ]
            param_df = pd.DataFrame(set_value)
            param_df = param_df.explode(['Code value', 'Decode value']).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': param_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')
