import re
import glob
import pandas as pd
from functools import reduce
import logging
from .non_functional import CallFunction
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
            self.clear_target_file()
            
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
    
    def split_column(self, row: any) -> any:
        comma = row['NAME'].count(',')
        if comma == 2:
            name, department, _ = row['NAME'].split(',')
        else:
            name, department = row['NAME'].split(',')
            
        return name, department
    
    def collect_depend_file(self, i: int) -> pd.DataFrame:
        
        logging.info('Lookup depend file')
        
        for full_depend in self.logging[i]['full_depend']:
            
            data = []
            if glob.glob(full_depend, recursive=True):
                format_file = self.read_file(i, full_depend)
                
                # clean and split the data
                data = [re.sub(r'(?<!\.),', '||', line.strip()).split('||') for line in format_file]
                
            else:
                self.logging[i].update({'err': f'File not found {full_depend}'})
                
            if 'err' in self.logging[i]:
                raise CustomException(err=self.logging)
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_depend_file', 'status': status})
        
        try:
            # set dataframe
            param_df = pd.DataFrame(data)
            param_df.columns = param_df.iloc[0].values
            param_df = param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            Role_BOS_PARAM = param_df[['employee_no','rolename','username',]]
            Concat_Role_BOS_PARAM = Role_BOS_PARAM.groupby(['employee_no','username',])['rolename'].agg(lambda x: '+'.join(f"sec_{value}" for value in x)).reset_index()
            
            ## set dataframe
            # param_df = pd.DataFrame(data)
            # param_df.columns = param_df.iloc[0].values
            # param_df = param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## extracting column
            # param_df[['domain', 'username']] = param_df['username'].str.extract(r'^(.*?)\\(.*)$', expand=False)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'status': status})
        
        return Concat_Role_BOS_PARAM
        
    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})

        try:
            ## clean and split the data
            data = [re.sub(r'(?<!\.),', '||', line.strip()).split('||') for line in format_file]
                        
            ## FILE: BOSTH 
            user_df = pd.DataFrame(data)
            # user_df.columns = user_df.iloc[0].values
            # user_df = user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
                        
            Role_BOS = user_df[['employee_no','rolename','user_name','branch_code','employee_display_name']]
            Concat_Role_BOS = Role_BOS.groupby(['employee_no','user_name','branch_code','employee_display_name'])['rolename'].agg(lambda x: '+'.join(f"app_{value}" for value in x)).reset_index()
            
            ## FILE: BOSTH_Param
            Concat_Role_BOS_PARAM = self.collect_depend_file(i)
            Entitlement_name = pd.merge(Concat_Role_BOS_PARAM,Concat_Role_BOS,on="employee_no",how="right",suffixes=('_PARAM','_BOS'))
            Entitlement_name['rolename'] = Entitlement_name[['rolename_PARAM', 'rolename_BOS']].apply(lambda x: ';'.join([str(val) for val in x if pd.notna(val)]), axis=1)
            Result_Entitlement_name = Entitlement_name[['employee_no','user_name','employee_display_name','branch_code','rolename']]
            Result_Entitlement_name['branch_code'] = Result_Entitlement_name['branch_code'].astype(str).str.zfill(3)
            Result_Entitlement_name['user_name'] = Result_Entitlement_name['user_name'].apply(lambda x: x.replace('NTTHPDOM\\', '') if isinstance(x, str) else x) 
            Rename_Entitlement_name = Result_Entitlement_name.rename(columns={
                'user_name' : 'AccountName',
                'employee_display_name' : 'AccountDescription',
                'branch_code' : 'AdditionalAttribute​'
            })
            Reformat_BOS_COL=self.logging[i]['columns']
            Reformat_BOS_DATA = pd.DataFrame(columns=self.logging[i]['columns'])
            static_value = {
                'Applicaiton Code' : 'BOS',
                'AccountType' : 'USR',
                'SecondEntitlementName' : 'NA',
                'ThirdEntitlementName' : 'NA',
                'AccountStatus' : 'A',
                'IsPrivileged' : 'N',
                'CreateDate' : 'NA',
                'LastLogin' : 'NA',
                'LastUpdatedDate' : 'NA',
                'Country' : 'TH',
            }
            Final_BOS = pd.concat([Rename_Entitlement_name,Reformat_BOS_DATA],ignore_index=True)
            Final_BOS['AccountOwner'] = Final_BOS['AccountName']
            Final_BOS = Final_BOS.fillna(static_value)
            Final_BOS = Final_BOS.drop(columns='employee_no')
            Final_BOS = Final_BOS[Reformat_BOS_COL].sort_values(by='AccountOwner',ignore_index=True)
            print(Final_BOS)
                        
            # # FILE: BOSTH 
            # user_df = pd.DataFrame(data)
            # user_df.columns = user_df.iloc[0].values
            # user_df =  user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            # # leading zeros to make it 3 digits
            # user_df['branch_code'] = user_df['branch_code'].apply(lambda row: '{:0>3}'.format(row))
            
            # # extracting column
            # user_df[['domain', 'username']] = user_df['user_name'].str.extract(r'^(.*?)\\(.*)$', expand=False)
            
            # # FILE: BOSTH_Param
            # param_df = self.collect_depend_file(i)
            
            # # merge 2 file BOSTH / BOSTH_Param
            # self.logging[i].update({'function': 'collect_user_file', 'status': status})
            # merge_df = reduce(lambda left, right: pd.merge(left, right, on='username', how='left', validate='m:m'), [user_df, param_df])
            
            # # group by column
            # merge_df = merge_df.groupby('username', sort=False).agg(lambda row: '+'.join(map(str, row.replace([None], ['NA']).unique()))).reset_index()
            
            # # mapping data to column
            # set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            # set_value.update(
            #     {
            #         'ApplicationCode': 'BOS',
            #         'AccountOwner': merge_df['username'],
            #         'AccountName': merge_df['username'],
            #         'AccountType': 'USR',
            #         'EntitlementName': merge_df[['rolename_y', 'rolename_x']].apply(lambda row: ';'.join(row), axis=1),
            #         'AccountStatus': 'A',
            #         'IsPrivileged': 'N',
            #         'AccountDescription': merge_df['employee_display_name'],
            #         'AdditionalAttribute': merge_df['branch_code'],
            #         'Country': 'TH',
            #     }
            # )
            # merge_df = merge_df.assign(**set_value)
            # merge_df = merge_df.drop(merge_df.iloc[:,:14].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)

        # status = 'succeed'
        # self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        # logging.info(f'Collect user data, status: {status}')
    
    def collect_param_file(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            # clean and split the data
            data = [re.sub(r'(?<!\.),', '||', line.strip()).split('||') for line in format_file]
            
            # FILE: BOSTH
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # leading zeros to make it 3 digits
            user_df['branch_code'] = user_df['branch_code'].apply(lambda row: '{:0>3}'.format(row))
            
            # group by column
            user_df = user_df.groupby('branch_code', sort=False).agg(lambda row: '+'.join(map(str, row.replace([None], ['NA']).unique()))).reset_index()
            
            # FILE: BOSTH_Param
            param_df = self.collect_depend_file(i)
            
            # mapping data to column
            self.logging[i].update({'function': 'collect_param', 'status': status})
            set_value = [
                {
                    'Parameter Name': 'Security roles', 
                    'Code value': param_df['rolename'].unique(), 
                    'Decode value': param_df['rolename'].unique()
                },
                {
                    'Parameter Name': 'Application roles', 
                    'Code value': user_df['rolename'].unique(), 
                    'Decode value': user_df['rolename'].unique()
                },
                {
                    'Parameter Name': 'Department Code', 
                    'Code value': user_df['branch_code'].unique(),  
                    'Decode value': user_df['branch_name'].unique()
                },
            ]
            merge_df = pd.DataFrame(set_value)
            merge_df = merge_df.explode(['Code value', 'Decode value']).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')