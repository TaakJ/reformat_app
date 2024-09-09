import re
import pandas as pd
import logging
from .non_functional import CallFunction
from .exception import CustomException

class ModuleLMT(CallFunction):

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
            logging.error("See Error Details: log_error.log")

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
            # clean and split the data
            data = [re.sub(r'(?<!\.),', ',', line.strip().replace('"', '')).split(',') for line in format_file]

            # set dataframe
            user_df = pd.DataFrame(data)
            user_df.columns = user_df.iloc[0].values
            user_df = user_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            user_df.iloc[:,[4,5,6]] = user_df.iloc[:,[4,5,6]].fillna('NA')
            user_df = user_df.drop_duplicates().reset_index()
            
            # group by column
            group_user_df = user_df.groupby(['DisplayName','EmployeeNo','Username','Department']).agg(lambda x: '+'.join(map(str, sorted(set(x))))).reset_index()
            group_user_df['Roles'] = group_user_df.iloc[:,[4,5,6]].apply(lambda x: ';'.join([str(val) for val in x if pd.notna(val)]), axis=1)
            group_user_df['Username'] = group_user_df['Username'].apply(lambda x: x.replace('NTTHPDOM\\', '') if isinstance(x, str) else x)
            group_user_df = group_user_df.drop(group_user_df.iloc[:,[4,5,6]],axis=1)
            group_user_df = group_user_df.rename(columns={
                'Username' : 'AccountOwner',
                'Roles' : 'EntitlementName',
                'DisplayName' : 'AccountDescription',
                'Department' : 'AdditionalAttribute',
            })
            
            # merge dataframe
            columns = self.logging[i]['columns']
            merge_df = pd.DataFrame(columns=columns)
            merge_df = pd.merge(group_user_df, merge_df, on=['AccountOwner','EntitlementName','AccountDescription','AdditionalAttribute'],how='left')
            merge_df['AccountName'] = merge_df['AccountOwner']
            static_values = {
                'ApplicationCode' : 'LMT',
                'AccountType' : 'USR',
                'SecondEntitlementName' : 'NA',
                'ThirdEntitlementName' : 'NA',
                'AccountStatus' : 'A',
                'IsPrivileged' : 'N',
                'CreateDate' : 'NA',
                'LastLogin' : 'NA',
                'LastUpdatedDate' : 'NA',
                'Country' : 'TH'
            }
            merge_df = merge_df.fillna(static_values)
            merge_df = merge_df[columns]
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user data, status: {status}')

    def collect_param_file(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param_file', 'status': status})

        try:
            # clean and split the data
            data = [re.sub(r'(?<!\.),', ',', line.strip().replace('"', '')).split(',') for line in format_file]

            # set dataframe
            param_df = pd.DataFrame(data)
            param_df.columns = param_df.iloc[0].values
            param_df = param_df.iloc[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            param_df = param_df.iloc[:,[4,5,6]]
            
            # adjust dataframe
            param_transpose = param_df.T
            merge_df = param_transpose.reset_index().melt(id_vars='index')
            merge_df.columns = ['Parameter Name', 'Value', 'Code values']            
            merge_df = merge_df.drop(columns=['Value'])
            merge_df = merge_df.drop_duplicates(subset=['Code values'])
            merge_df = merge_df.dropna().reset_index(drop=True)
            merge_df = merge_df[merge_df["Code values"] != 'NA']
            merge_df['Decode value'] = merge_df['Code values']
            merge_df = merge_df.sort_values(by=['Parameter Name','Code values']).reset_index(drop=True)
            
        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': merge_df.to_dict('list'), 'status': status})
        logging.info(f'Collect user param, status: {status}')
