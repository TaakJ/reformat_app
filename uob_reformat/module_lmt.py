import re
import pandas as pd
import logging
from .function import CallFunction
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

            if self.backup is True:
                self.achieve_backup()

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
            
        logging.info(f'Stop Run Module "{self.module}"\r\n')

        return result

    def collect_user(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({"function": "collect_user", "status": status})

        try:
            data = []
            for line in format_file:
                find_word = line.strip().replace('"', "")
                data += [re.sub(r"(?<!\.),", ",", "".join(find_word)).split(",")]

            ## set dataframe
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)

            ## mapping data to column
            df[['Domain', 'Username']] = df['Username'].str.extract(r'^(.*?)\\(.*)$')
            df = df.groupby('Username', sort=False)
            df = df.agg(lambda row: '+'.join(row.unique())).reset_index()

            set_value = dict.fromkeys(self.logging[i]['columns'], "NA")
            set_value.update(
                {
                    'ApplicationCode': 'LMT',
                    'AccountOwner': df['Username'],
                    'AccountName': df['Username'],
                    'AccountType': 'USR',
                    'EntitlementName': df[['SecurityRoles', 'ApplicationRoles', 'ProgramTemplate']].apply(lambda row: ';'.join(row), axis=1),
                    'AccountStatus': 'A',
                    'IsPrivileged': 'N',
                    'AccountDescription': df['DisplayName'],
                    'AdditionalAttribute': df['Department'],
                    'Country': 'TH',
                }
            )
            df = df.assign(**set_value).fillna('NA')
            df = df.drop(df.iloc[:, :8].columns, axis=1)

        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f"Collect user data, status: {status}")

    def collect_param(self, i: int, format_file: any) -> dict:

        status = 'failed'
        self.logging[i].update({'function': 'collect_param', 'status': status})

        try:
            data = []
            for line in format_file:
                find_word = line.strip().replace('"', '')
                data += [re.sub(r'(?<!\.),', ',', ''.join(find_word)).split(',')]

            ## set dataframe
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:].apply(lambda row: row.str.strip()).reset_index(drop=True)
            
            ## mapping data to column
            set_value = [
                {
                    'Parameter Name': 'Security Roles',
                    'Code value': df['SecurityRoles'].unique(),
                    'Decode value': df['SecurityRoles'].unique(),
                },
                {
                    'Parameter Name': 'Application Roles',
                    'Code value': df['ApplicationRoles'].unique(),
                    'Decode value': df['ApplicationRoles'].unique(),
                },
                {
                    'Parameter Name': 'Program Template',
                    'Code value': df['ProgramTemplate'].unique(),
                    'Decode value': df['ProgramTemplate'].unique(),
                },
            ]
            df = pd.DataFrame(set_value)
            df = df.explode(['Code value', 'Decode value']).reset_index(drop=True)

        except Exception as err:
            raise Exception(err)

        status = 'succeed'
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f'Collect user param, status: {status}')
