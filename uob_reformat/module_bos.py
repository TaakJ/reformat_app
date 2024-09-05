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
        
    def collect_user_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_user_file', 'status': status})

        try:
            ## FILE: BOSTH.csv
            BOS_PATH = self.logging[i]['full_input']
            BOS_ORIGINAL = pd.read_csv(BOS_PATH)
            
            ## FILE: BOSTH_Param.csv
            for BOS_PARAM_PATH in self.logging[i]['full_depend']:
                BOS_PARAM_ORIGINAL = pd.read_csv(BOS_PARAM_PATH)
            
            Role_BOS = BOS_ORIGINAL[['employee_no','rolename','user_name','branch_code','employee_display_name']]
            Concat_Role_BOS = Role_BOS.groupby(['employee_no','user_name','branch_code','employee_display_name'])['rolename'].agg(lambda x: '+'.join(f"app_{value}" for value in x)).reset_index()

            Role_BOS_PARAM = BOS_PARAM_ORIGINAL[['employee_no','rolename','username',]]

            Concat_Role_BOS_PARAM = Role_BOS_PARAM.groupby(['employee_no','username',])['rolename'].agg(lambda x: '+'.join(f"sec_{value}" for value in x)).reset_index()

            Entitlement_name = pd.merge(Concat_Role_BOS_PARAM,Concat_Role_BOS,on="employee_no",how="right",suffixes=('_PARAM','_BOS'))
            #print(Entitlement_name)
            Entitlement_name['rolename'] = Entitlement_name[['rolename_PARAM', 'rolename_BOS']].apply(lambda x: ';'.join([str(val) for val in x if pd.notna(val)]), axis=1)
            Result_Entitlement_name = Entitlement_name[['employee_no','user_name','employee_display_name','branch_code','rolename']]
            Result_Entitlement_name['branch_code'] = Result_Entitlement_name['branch_code'].astype(str).str.zfill(3)
            Result_Entitlement_name['user_name'] = Result_Entitlement_name['user_name'].apply(lambda x: x.replace('NTTHPDOM\\', '') if isinstance(x, str) else x) 
            
            Rename_Entitlement_name = Result_Entitlement_name.rename(columns={
                'user_name' : 'AccountName',
                'employee_display_name' : 'AccountDescription',
                'branch_code' : 'AdditionalAttribute'
            })
            
            Reformat_BOS_COL = ["ApplicationCode","AccountOwner","AccountName","AccountType","EntitlementName","SecondEntitlementName","ThirdEntitlementName","AccountStatus","IsPrivileged","AccountDescription","CreateDate","LastLogin","LastUpdatedDate","AdditionalAttribute","Country"]
            Reformat_BOS_DATA = pd.DataFrame(columns=Reformat_BOS_COL)
            static_value = {
                'ApplicationCode' : 'BOS',
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
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': Final_BOS.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')
    
    def collect_param_file(self, i: int, format_file: any) -> dict:
        
        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})
        
        try:
            BOS_PATH = self.logging[i]['full_input']
            BOS_ORIGINAL = pd.read_csv(BOS_PATH)
            
            for BOS_PARAM_PATH in self.logging[i]['full_depend']:
                BOS_PARAM_ORIGINAL = pd.read_csv(BOS_PARAM_PATH)
            
            ## PARAM LIST
            SEC_PARAM_LIST = pd.DataFrame(columns=('Parameter Name','Code value','Decode value'))
            SEC_PARAM_UNI = BOS_PARAM_ORIGINAL['rolename'].unique()
            SEC_PARAM_LIST['Code value'] = SEC_PARAM_UNI
            SEC_PARAM_LIST['Decode value'] = SEC_PARAM_UNI
            SEC_PARAM_LIST['Parameter Name'] = 'Security roles'


            APP_PARAM_LIST = BOS_ORIGINAL.iloc[:,[1,2]]
            APP_PARAM_LIST = APP_PARAM_LIST.drop_duplicates()
            APP_PARAM_LIST.insert(0,'Parameter Name','Application roles')
            APP_PARAM_LIST = APP_PARAM_LIST.rename(columns={
                'branch_code' : 'Code value',
                'branch_name' : 'Decode value'
            })
            
            DEPT_PARAM_LIST = pd.DataFrame(columns=('Parameter Name','Code value','Decode value'))
            DEPT_PARAM_UNI = BOS_PARAM_ORIGINAL['rolename'].unique()
            DEPT_PARAM_LIST['Code value'] = DEPT_PARAM_UNI
            DEPT_PARAM_LIST['Decode value'] = DEPT_PARAM_UNI
            DEPT_PARAM_LIST['Parameter Name'] = 'Department roles'

            PARAM_LIST = pd.concat([APP_PARAM_LIST,SEC_PARAM_LIST],ignore_index=True)
            PARAM_LIST = pd.concat([PARAM_LIST,DEPT_PARAM_LIST],ignore_index=True)
            
        except Exception as err:
            raise Exception(err)
        
        status = 'succeed'
        self.logging[i].update({'data': PARAM_LIST.to_dict('list'), 'status': status})
        logging.info(f'Collect param data, status: {status}')