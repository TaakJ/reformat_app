import re
import glob
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

        logging.info(f"Module:'{self.module}'; Manual: '{self.manual}'; Run date: '{self.batch_date}'; Store tmp: '{self.store_tmp}'; Write mode: '{self.write_mode}';")

        result = {'module': self.module, 'task': 'Completed'}
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

            result.update({'task': 'Uncompleted'})

        logging.info(f'Stop Run Module "{self.module}"\r\n')
        
        return result
    
    def lookup_depend_file(self, i: int) -> pd.DataFrame:
        
        logging.info('Lookup depend file')
        
        data = []
        for full_depend in self.logging[i]['full_depend']:
            if glob.glob(full_depend, recursive=True):
                format_file = self.read_file(i, full_depend)
                for line in format_file:
                    data += [re.sub(r'(?<!\.),', '||', line.strip()).split('||')]
                
            else:
                self.logging[i].update({'err': f'File not found {full_depend}'})
                
            if 'err' in self.logging[i]:
                raise CustomException(err=self.logging)
        
        status = 'failed'
        self.logging[i].update({'function': 'lookup_depend_file', 'status': status})
        
        try:
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            df[['Domain', 'username']] = df['username'].str.extract(r'^(.*?)\\(.*)$')
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'status': status})
        
        return df
        
    def collect_user(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user', 'status': status})

        try:
            data = []
            for line in format_file:
                data += [re.sub(r'(?<!\.),', '||', line.strip()).split('||')]
            
            ## set dataframe on main dataframe
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            df['branch_code'] = df['branch_code'].apply(lambda row: '{:0>3}'.format(row))
            df[['Domain', 'username']] = df['user_name'].str.extract(r'^(.*?)\\(.*)$')
            
            ## set dataframe on depend dataframe
            depend_df = self.lookup_depend_file(i)
            
            ## mapping data to column (continue function)
            self.logging[i].update({'function': 'collect_user', 'status': status})
            merge_df = pd.merge(df, depend_df, on='username', how='left', validate='m:m').replace([None],[''])
            merge_df = merge_df.groupby('username', sort=False)
            merge_df = merge_df.agg(lambda row: '+'.join(row.unique())).reset_index()
            
            set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            set_value.update(
                {
                    'ApplicationCode': 'BOS',
                    'AccountOwner': merge_df['username'],
                    'AccountName': merge_df['username'],
                    'AccountType': 'USR',
                    'EntitlementName': merge_df[['rolename_y', 'rolename_x']].apply(lambda row: ';'.join(row), axis=1),
                    'AccountStatus': 'A',
                    'IsPrivileged': 'N',
                    'AccountDescription': merge_df['employee_display_name'],
                    'AdditionalAttribute': merge_df['branch_code'],
                    'Country': 'TH',
                }
            )
            merge_df = merge_df.assign(**set_value)
            merge_df = merge_df.drop(merge_df.iloc[:, :14].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')
    
    def collect_param(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param', 'status': status})
        
        try:
            data = []
            for line in format_file:
                data += [re.sub(r'(?<!\.),', '||', line.strip()).split('||')]
            
            ## set dataframe on main dataframe
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            df['branch_code'] = df['branch_code'].apply(lambda row: '{:0>3}'.format(row))
            df = df.groupby('branch_code', sort=False)
            df = df.agg(lambda row: '+'.join(row.unique())).reset_index()
            
            ## set dataframe on depend dataframe
            depend_df = self.lookup_depend_file(i)
            
            ## mapping data to column (continue function)
            self.logging[i].update({'function': 'collect_param', 'status': status})
            set_value = [
                {
                    'Parameter Name': 'Security roles', 
                    'Code value': depend_df['rolename'].unique(), 
                    'Decode value': depend_df['rolename'].unique()
                },
                {
                    'Parameter Name': 'Application roles', 
                    'Code value': df['rolename'].unique(), 
                    'Decode value': df['rolename'].unique()
                },
                {
                    'Parameter Name': 'Department Code', 
                    'Code value': df['branch_code'].unique(),  
                    'Decode value': df['branch_name'].unique()
                },
            ]
            
            df = pd.DataFrame(set_value)
            df = df.explode(['Code value', 'Decode value']).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')