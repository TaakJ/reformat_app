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
            if self.store_tmp is True:
                await self.genarate_tmp_file()
            await self.genarate_target_file()

        except CustomException as err:
            logging.error('See Error Details: log_error.log')

            logger = err.setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.error(next(err))
                except StopIteration:
                    break

            result.update({'task': 'Uncompleted'})

        logging.info(f"Stop Run Module '{self.module}'\r\n")

        return result
    
    def parse_and_format_datetime(self, date_str):
        formats = ['%y/%m/%d %H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%d/%m/%y %H:%M']
        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt).strftime('%Y%m%d%H%M%S')
            except ValueError:
                continue
        return 'Invalid Format'  # In case none of the formats match

    def clean_fullname(self, name):
        name = name.replace(',', '')
        name = re.sub(r'\d+', '', name)
        name = name.strip()
        return name
    
    def validate_row_length(self, rows_list: list[list], valid_lengths: list[int]) -> None:
        errors = []
        for i, rows in enumerate(rows_list):
            try:
                assert len(rows) in valid_lengths, f"Row {i} does not match elements: {rows}"
            except AssertionError as err:
                errors.append(str(err))
                    
        if errors:
            raise Exception("Assertion errors:\n" + "\n".join(errors))
    
    def collect_depend_file(self, i: int) -> pd.DataFrame:
        
        logging.info('Lookup depend file')
        
        tbl = {}
        for full_depend in self.logging[i]['full_depend']:
            
            data = []
            if glob.glob(full_depend, recursive=True):
                
                format_file = self.read_file(i, full_depend)
                for line in format_file:
                    data += [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||')]
                    
                tbl_name = Path(full_depend).name
                if tbl_name in tbl:
                    tbl[tbl_name].extend(data)
                else:
                    tbl[tbl_name] = data
            else:
                self.logging[i].update({'err': f'File not found {full_depend}'})
                
            if 'err' in self.logging[i]:
                raise CustomException(err=self.logging)
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_depend_file', 'status': status})
        
        try:
            # FILE: ICAS_TBL_USER_GROUP
            self.validate_row_length(tbl['ICAS_TBL_USER_GROUP'], [3, 5, 7])
            columns = ['Record_Type', 'GROUP_ID','USER_ID','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID','LAST_UPDATE_DTM']
            tbl_user_group_df = pd.DataFrame(tbl['ICAS_TBL_USER_GROUP'], columns=columns)
            tbl_user_group_df = tbl_user_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # FILE: ICAS_TBL_USER_BANK_BRANCH
            self.validate_row_length(tbl['ICAS_TBL_USER_BANK_BRANCH'], [3,5,9])
            columns = ['Record_Type','USER_ID','BANK_CODE','BRANCH_CODE','SUB_SYSTEM_ID','ACCESS_ALL_BRANCH_IN_HUB','DEFAULT_BRANCH_FLAG','CREATE_USER_ID','CREATE_DTM']
            tbl_user_bank_df = pd.DataFrame(tbl['ICAS_TBL_USER_BANK_BRANCH'], columns=columns)
            tbl_user_bank_df = tbl_user_bank_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # FILE: ICAS_TBL_GROUP
            self.validate_row_length(tbl['ICAS_TBL_GROUP'], [3, 5,14])
            columns = ['Record_Type','GROUP_ID','SUB_SYSTEM_ID','GROUP_NAME','RESTRICTION','ABLE_TO_REVERIFY_FLAG','DESCRIPTION','DEFAULT_FINAL_RESULT','DELETE_FLAG','CREATE_USER_ID',
                        'CREATE_DTM','LAST_UPDATE_USER_ID','LAST_UPDATE_DTM','DELETE_DTM']
            tbl_group_df = pd.DataFrame(tbl['ICAS_TBL_GROUP'], columns=columns)
            tbl_group_df = tbl_group_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
        except:
            raise
        
        status = 'succeed'
        self.logging[i].update({'status': status})
        
        return tbl_user_group_df, tbl_user_bank_df, tbl_group_df

    def collect_user_file(self, i: int, format_file: any) -> str:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})

        try:
            # clean and split the data
            data = [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||') for line in format_file]
                
            # FILE: ICAS_TBL_USER
            self.validate_row_length(data, [3,5, 21])
            columns = ['Record_Type','USER_ID','LOGIN_NAME','FULL_NAME','PASSWORD','LOCKED_FLAG','FIRST_LOGIN_FLAG','LAST_ACTION_TYPE','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID',
                        'LAST_UPDATE_DTM','LAST_LOGIN_ATTEMPT','ACCESS_ALL_BRANCH_FLAG','HOME_BRANCH','HOME_BANK','LOGIN_RETRY_COUNT','LAST_CHANGE_PASSWORD','DELETE_FLAG',
                        'LAST_LOGIN_SUCCESS','LAST_LOGIN_FAILED']
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # FILE: ICAS_TBL_USER_GROUP, ICAS_TBL_USER_BANK_BRANCH, ICAS_TBL_GROUP
            tbl_user_group_df, tbl_user_bank_df, _ = self.collect_depend_file(i)
            
            entitlement_name = pd.merge(tbl_user_df, tbl_user_group_df,on='USER_ID',how='left')
            entitlement_name = entitlement_name.fillna('NA')
            entitlement_name_group = entitlement_name.groupby('USER_ID')['GROUP_ID'].apply(lambda row: '+'.join(map(str, sorted(set(row))))).reset_index()
            entitlement_name_group = entitlement_name_group.replace(to_replace=r'NA\+|\+NA(?!;)', value='', regex=True)
            
            # merge file: ICAS_TBL_USER with ICAS_TBL_USER_GROUP
            result_ica = pd.merge(entitlement_name_group, tbl_user_df,on='USER_ID')
            result_ica = result_ica[['USER_ID','LOGIN_NAME','GROUP_ID','LOCKED_FLAG','FULL_NAME','CREATE_DTM','LAST_LOGIN_ATTEMPT','LAST_UPDATE_DTM']]
            
            # merge file: ICAS_TBL_USER with ICAS_TBL_USER_BANK_BRANCH
            branch_code = pd.merge(tbl_user_df, tbl_user_bank_df,on='USER_ID',how='left')
            branch_code = branch_code[['USER_ID','HOME_BANK','HOME_BRANCH','BRANCH_CODE']]
            branch_code['HOME_BANK'] = branch_code['HOME_BANK'].fillna('NA')
            branch_code['HOME_BRANCH'] = branch_code['HOME_BRANCH'].fillna('NA')
            branch_code['BRANCH_CODE'] = branch_code['BRANCH_CODE'].fillna('NA')
            branch_code['BANK+BRANCH'] = branch_code[['HOME_BANK','HOME_BRANCH','BRANCH_CODE']].astype(str).agg('#'.join, axis=1)
            final_branch_code = branch_code[['USER_ID','BANK+BRANCH']]
            final_ica = pd.merge(result_ica,final_branch_code,on='USER_ID',how='left')
            
            date_time_col = ['CREATE_DTM','LAST_LOGIN_ATTEMPT','LAST_UPDATE_DTM']
            for col in date_time_col:
                final_ica[col] = final_ica[col].apply(self.parse_and_format_datetime)
                
            final_ica['FULL_NAME'] = final_ica['FULL_NAME'].apply(self.clean_fullname)
            final_ica['LOCKED_FLAG'] = final_ica['LOCKED_FLAG'].apply(lambda row: 'D' if row == '1' else 'A')
            
            ## merge dataframe
            columns = self.logging[i]['columns']
            merge_df = pd.DataFrame(columns=columns)
            static_value = {
                'ApplicationCode': 'ICA',
                'AccountType': 'USR',
                'SecondEntitlementName': 'NA',
                'ThirdEntitlementName': 'NA',
                'IsPrivileged' : 'N',
                'Country' : 'TH'
            }
            
            column_mapping = {
                'LOGIN_NAME' : 'AccountOwner',
                'GROUP_ID' : 'EntitlementName',
                'LOCKED_FLAG' : 'AccountStatus',
                'FULL_NAME' : 'AccountDescription',
                'CREATE_DTM' : 'CreateDate',
                'LAST_LOGIN_ATTEMPT' : 'LastLogin',
                'LAST_UPDATE_DTM' : 'LastUpdatedDate',
                'BANK+BRANCH' : 'AdditionalAttribute'
            }
            final_ica = final_ica.rename(columns=column_mapping)
            merge_df = pd.concat([merge_df,final_ica],ignore_index=True)
            merge_df = merge_df.drop(columns='USER_ID')
            merge_df['AccountName'] = merge_df['AccountOwner']
            merge_df = merge_df.fillna(static_value)
            merge_df['CreateDate'] = merge_df['CreateDate'].astype(str)
            merge_df['LastLogin'] = merge_df['LastLogin'].astype(str)
            merge_df['LastUpdatedDate'] = merge_df['LastUpdatedDate'].astype(str)
            
        except:
            raise

        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            # clean and split the data
            data = [re.sub(r'(?<!\.)\x07', '||', line.strip()).split('||') for line in format_file]
                
            # FILE: ICAS_TBL_USER
            columns = ['Record_Type','USER_ID','LOGIN_NAME','FULL_NAME','PASSWORD','LOCKED_FLAG','FIRST_LOGIN_FLAG','LAST_ACTION_TYPE','CREATE_USER_ID','CREATE_DTM','LAST_UPDATE_USER_ID',
                        'LAST_UPDATE_DTM','LAST_LOGIN_ATTEMPT','ACCESS_ALL_BRANCH_FLAG','HOME_BRANCH','HOME_BANK','LOGIN_RETRY_COUNT','LAST_CHANGE_PASSWORD','DELETE_FLAG',
                        'LAST_LOGIN_SUCCESS','LAST_LOGIN_FAILED']
            tbl_user_df = pd.DataFrame(data, columns=columns)
            tbl_user_df = tbl_user_df.iloc[1:-1].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            # FILE: ICAS_TBL_USER_GROUP, ICAS_TBL_USER_BANK_BRANCH, ICAS_TBL_GROUP
            _, tbl_user_bank_df, tbl_group_df = self.collect_depend_file(i)
            
            # merge dataframe
            columns = self.logging[i]['columns']
            merge_df = pd.DataFrame(columns=columns)
            
            home_bank = {'Parameter Name':'HOME_BANK','Code values':'024','Decode value': 'UOBT'}
            param_home_bank = pd.DataFrame([home_bank])
            
            # merge column: group_id
            param_group_unique = tbl_group_df['GROUP_ID'].unique()
            filter_param_group = tbl_group_df[tbl_group_df['GROUP_ID'].isin(param_group_unique)]
            filter_param_group = filter_param_group[['GROUP_ID','GROUP_NAME']]
            filter_param_group.insert(0,'Parameter Name','User Group')
            filter_param_group.rename(columns={
                'GROUP_ID' : 'Code values',
                'GROUP_NAME' : 'Decode value'
            },inplace=True)
            merge_df = pd.concat([merge_df, filter_param_group],ignore_index=True)
            
            
            # merge column: home_bank
            merge_df = pd.concat([merge_df, param_home_bank],ignore_index=True)
            
            # merge column: home_branch
            param_home_branch_list = pd.DataFrame(columns=('Parameter Name','Code values','Decode value'))
            param_home_branch_uni = tbl_user_df['HOME_BRANCH'].unique()
            param_home_branch_list['Code values'] = param_home_branch_uni
            param_home_branch_list['Decode value'] = param_home_branch_uni
            param_home_branch_list['Parameter Name'] = 'HOME_BRANCH'
            merge_df = pd.concat([merge_df, param_home_branch_list],ignore_index=True)
            
            # merge column: home_branch
            param_dept_list = pd.DataFrame(columns=('Parameter Name','Code values','Decode value'))
            param_dept_uni = tbl_user_bank_df['BRANCH_CODE'].unique()
            param_dept_list['Code values'] = param_dept_uni
            param_dept_list['Decode value'] = param_dept_uni
            param_dept_list['Parameter Name'] = 'Department'
            merge_df = pd.concat([merge_df,param_dept_list],ignore_index=True)
            # merge_df = merge_df.sort_values(by=['Parameter Name','Code values'],ignore_index=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')
