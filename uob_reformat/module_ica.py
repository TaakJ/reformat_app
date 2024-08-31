import re
import glob
from pathlib import Path
import pandas as pd
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
    
    def lookup_depend_file(self, i: int) -> pd.DataFrame:
        
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
        self.logging[i].update({'function': 'lookup_depend_file', 'status': status})
        try:
            ## TABLE: ICAS_TBL_USER_GROUP
            columns = ['Record_Type', 'GROUP_ID','USER_ID','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID','LAST_UPDATE_DTM']
            tbl_user_group_df = pd.DataFrame(table['ICAS_TBL_USER_GROUP'], columns=columns)
            print(tbl_user_group_df)
            # tbl_user_group_df = tbl_user_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## TABLE: ICAS_TBL_USER_BANK_BRANCH
            # 0: Record_Type
            # 1: USER_ID
            # 2: BANK_CODE
            # 3: BRANCH_CODE
            # 4: SUB_SYSTEM_ID
            # 5: ACCESS_ALL_BRANCH_IN_HUB
            # 6: DEFAULT_BRANCH_FLAG
            # 7: CREATE_USER_ID
            # 8: CREATE_DTM
            tbl_user_bank_df = pd.DataFrame(table['ICAS_TBL_USER_BANK_BRANCH'])
            tbl_user_bank_df = tbl_user_bank_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## TABLE: ICAS_TBL_USER_GROUP
            # 0: Record_Type
            # 1: GROUP_ID
            # 2: SUB_SYSTEM_ID
            # 3: GROUP_NAME
            # 4: RESTRICTION
            # 5: ABLE_TO_REVERIFY_FLAG
            # 6: DESCRIPTION
            # 7: DEFAULT_FINAL_RESULT
            # 8: DELETE_FLAG
            # 9: CREATE_USER_ID
            # 10:CREATE_DTM
            # 11:LAST_UPDATE_USER_ID
            # 12:LAST_UPDATE_DTM
            # 13:DELETE_DTM
            tbl_tbl_group_df = pd.DataFrame(table['ICAS_TBL_GROUP'])
            tbl_tbl_group_df = tbl_tbl_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'status': status})
        
        return tbl_user_group_df, tbl_user_bank_df, tbl_tbl_group_df

    def collect_user(self, i: int, format_file: any) -> str:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user', 'status': status})

        try:
            data = []
            for line in format_file:
                data += [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||')]
                
            ## FILE: ICAS_TBL_USER
            columns = ['Record_Type', 'USER_ID', 'LOGIN_NAME', 'FULL_NAME', 'PASSWORD', 'LOCKED_FLAG', 'FIRST_LOGIN_FLAG', 'LAST_ACTION_TYPE', 'CREATE_USER_ID', 'CREATE_DTM', 
                        'LAST_UPDATE_USER_ID', 'LAST_UPDATE_DTM', 'LAST_LOGIN_ATTEMPT', 'ACCESS_ALL_BRANCH_FLAG', 'HOME_BRANCH',' HOME_BANK', 'LOGIN_RETRY_COUNT' ,'LAST_CHANGE_PASSWORD',
                        'DELETE_FLAG', 'LAST_LOGIN_SUCCESS', 'LAST_LOGIN_FAILED']
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            tbl_user_group_df, tbl_user_bank_df, tbl_tbl_group_df = self.lookup_depend_file(i)
            
            ## mapping data to column (continue function)
            # self.logging[i].update({'function': 'collect_user', 'status': status})
            # merge_user_df = pd.merge(tbl_user_df, tbl_user_group_df, on='USER_ID', how='left', validate='m:m').replace([None],[''])
            
            #  print(merge_user_df[1_y])


        except Exception as err:
            raise Exception(err)

        status = "succeed"
        # self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        # logging.info(f"Collect user data, status: {status}")

    def collect_param(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param', 'status': status})
