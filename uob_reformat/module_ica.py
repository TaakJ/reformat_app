import re
import glob
from pathlib import Path
import pandas as pd
from functools import reduce
import logging
from .non_functional import CallFunction
from .exception import CustomException


class ModuleICA(CallFunction):

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
    
    def collect_depend_file(self, i: int) -> pd.DataFrame:
        
        logging.info('Lookup depend file')
        
        table = {}
        for full_depend in self.logging[i]['full_depend']:
            
            data = []
            if glob.glob(full_depend, recursive=True):
                
                format_file = self.read_file(i, full_depend)
                for line in format_file:
                    data += [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||')]
                    
                table_name = Path(full_depend).name
                if table_name in table:
                    table[table_name].extend(data)
                else:
                    table[table_name] = data
            else:
                self.logging[i].update({'err': f'File not found {full_depend}'})
                
            if 'err' in self.logging[i]:
                raise CustomException(err=self.logging)
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_depend_file', 'status': status})
        
        try:
            ## FILE: ICAS_TBL_USER_GROUP
            columns = ['Record_Type', 'GROUP_ID','USER_ID','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID','LAST_UPDATE_DTM']
            tbl_user_group_df = pd.DataFrame(table['ICAS_TBL_USER_GROUP'], columns=columns)
            tbl_user_group_df = tbl_user_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## FILE: ICAS_TBL_USER_BANK_BRANCH
            columns = ['Record_Type','USER_ID','BANK_CODE','BRANCH_CODE','SUB_SYSTEM_ID','ACCESS_ALL_BRANCH_IN_HUB','DEFAULT_BRANCH_FLAG','CREATE_USER_ID','CREATE_DTM']
            tbl_user_bank_df = pd.DataFrame(table['ICAS_TBL_USER_BANK_BRANCH'], columns=columns)
            tbl_user_bank_df = tbl_user_bank_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## TABLE: ICAS_TBL_GROUP
            columns = ['Record_Type','GROUP_ID','SUB_SYSTEM_ID','GROUP_NAME','RESTRICTION','ABLE_TO_REVERIFY_FLAG','DESCRIPTION','DEFAULT_FINAL_RESULT','DELETE_FLAG','CREATE_USER_ID',
                        'CREATE_DTM','LAST_UPDATE_USER_ID','LAST_UPDATE_DTM','DELETE_DTM']
            tbl_tbl_group_df = pd.DataFrame(table['ICAS_TBL_GROUP'], columns=columns)
            tbl_tbl_group_df = tbl_tbl_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'status': status})
        
        return tbl_user_group_df, tbl_user_bank_df, tbl_tbl_group_df

    def collect_user_file(self, i: int, format_file: any) -> str:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})

        try:
            ## clean and split the data
            data = [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||') for line in format_file]
                
            ## FILE: ICAS_TBL_USER
            columns = ['Record_Type','USER_ID','LOGIN_NAME','FULL_NAME','PASSWORD','LOCKED_FLAG','FIRST_LOGIN_FLAG','LAST_ACTION_TYPE','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID',
                        'LAST_UPDATE_DTM','LAST_LOGIN_ATTEMPT','ACCESS_ALL_BRANCH_FLAG','HOME_BRANCH','HOME_BANK','LOGIN_RETRY_COUNT','LAST_CHANGE_PASSWORD','DELETE_FLAG',
                        'LAST_LOGIN_SUCCESS','LAST_LOGIN_FAILED']
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## FILE: ICAS_TBL_USER_GROUP, ICAS_TBL_USER_BANK_BRANCH, ICAS_TBL_GROUP
            tbl_user_group_df, tbl_user_bank_df, _ = self.collect_depend_file(i)
            
            # merge 3 file ICAS_TBL_USER / ICAS_TBL_USER_GROUP
            self.logging[i].update({'function': 'collect_user_file', 'status': status})
            # merge_df = reduce(lambda left, right: pd.merge(left, right, on='USER_ID', how='inner', validate='m:m'), [tbl_user_df, tbl_user_group_df, tbl_user_bank_df])
            
            merge_df = reduce(lambda left, right: pd.merge(left, right, on='USER_ID', how='left', validate='m:m'), [tbl_user_df, tbl_user_group_df, tbl_user_bank_df])
            
            # group by column
            merge_df = merge_df.groupby('USER_ID', sort=False).agg(lambda row: '+'.join(map(str, row.replace([None], ['NA']).unique()))).reset_index()
            print(merge_df)
            
            ## mapping data to column
            # set_value = dict.fromkeys(self.logging[i]['columns'], 'NA')
            # set_value.update(
            #     {
            #         'ApplicationCode': 'ICA',
            #         'AccountOwner': merge_df['LOGIN_NAME'],
            #         'AccountName': merge_df['LOGIN_NAME'],
            #         'AccountType': 'USR',
            #         'EntitlementName': merge_df['GROUP_ID'],
            #         'AccountStatus': merge_df['LOCKED_FLAG'].apply(lambda x: 'A' if x == '0' else 'D'),
            #         'IsPrivileged': 'N',
            #         'AccountDescription': merge_df['FULL_NAME'],
            #         'CreateDate': pd.to_datetime(merge_df['CREATE_DTM'], errors='coerce').dt.strftime('%Y%m%d%H%M%S'), 
            #         'LastLogin': pd.to_datetime(merge_df['LAST_LOGIN_ATTEMPT'], errors='coerce').dt.strftime('%Y%m%d%H%M%S'),
            #         'LastUpdatedDate': pd.to_datetime(merge_df['LAST_UPDATE_DTM_x'], errors='coerce').dt.strftime('%Y%m%d%H%M%S'),
            #         'AdditionalAttribute': merge_df[['HOME_BANK', 'HOME_BRANCH', 'BRANCH_CODE']].apply(lambda row: '#'.join(row), axis=1),
            #         'Country': 'TH',
            #     }
            # )
            # merge_df = merge_df.assign(**set_value)
            # merge_df = merge_df.drop(merge_df.iloc[:, :35].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            ## clean and split the data
            data = [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||') for line in format_file]
                
            ## FILE: ICAS_TBL_USER
            columns = ['Record_Type','USER_ID','LOGIN_NAME','FULL_NAME','PASSWORD','LOCKED_FLAG','FIRST_LOGIN_FLAG','LAST_ACTION_TYPE','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID',
                        'LAST_UPDATE_DTM','LAST_LOGIN_ATTEMPT','ACCESS_ALL_BRANCH_FLAG','HOME_BRANCH','HOME_BANK','LOGIN_RETRY_COUNT','LAST_CHANGE_PASSWORD','DELETE_FLAG',
                        'LAST_LOGIN_SUCCESS','LAST_LOGIN_FAILED']
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## FILE: ICAS_TBL_USER_GROUP, ICAS_TBL_USER_BANK_BRANCH, ICAS_TBL_GROUP
            _, tbl_user_bank_df, tbl_tbl_group_df = self.collect_depend_file(i)
            
            ## mapping data to column
            self.logging[i].update({'function': 'collect_param_file', 'status': status})
            set_value = [
                {
                    'Parameter Name': 'User Group',
                    'Code value': tbl_tbl_group_df['GROUP_ID'].unique(),
                    'Decode value': tbl_tbl_group_df['GROUP_NAME'].unique(),
                },
                {
                    'Parameter Name': 'HOME_BANK',
                    'Code value': '024',
                    'Decode value': 'UOBT',
                },
                {
                    'Parameter Name': 'HOME_BRANCH',
                    'Code value': tbl_user_df['HOME_BRANCH'].unique(),
                    'Decode value': tbl_user_df['HOME_BRANCH'].unique(),
                },
                {
                    'Parameter Name': 'Department',
                    'Code value': tbl_user_bank_df['BRANCH_CODE'].unique(),
                    'Decode value': tbl_user_bank_df['BRANCH_CODE'].unique(),
                },
            ]
            
            merge_df = pd.DataFrame(set_value)
            merge_df = merge_df.explode(['Code value', 'Decode value']).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')
