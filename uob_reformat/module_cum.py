import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException

class ModuleCUM(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)

    def logSetter(self, log: list) -> None:
        self._log = log

    async def run_process(self) -> dict:

        logging.info(f'Module:"{self.module}"; Manual: "{self.manual}"; Run date: "{self.batch_date}"; Store tmp: "{self.store_tmp}"; Write mode: "{self.write_mode}";')

        result = {'module': self.module, 'task': "Completed"}
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
            logging.error("See Error Details: log_error.log")

            logger = err.setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.exception(next(err))
                except StopIteration:
                    break

            result.update({'task': "Uncompleted"})

        logging.info(f'Stop Run Module "{self.module}"\r\n')

        return result

    def collect_user(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({'function': "collect_user", 'status': status})

        set_value = {columns: "NA" for columns in self.logging[i]['columns']}
        try:
            data = []
            sheet_list = [sheet for sheet in format_file.sheet_names()]
            for sheets in sheet_list:
                cells = format_file.sheet_by_name(sheets)
                for row in range(0, cells.nrows):
                    by_sheets = [cells.cell(row, col).value.strip() for col in range(cells.ncols)][1:]
                    if not all(empty == "" for empty in by_sheets):
                        data.append(by_sheets)
                        
            df = pd.DataFrame(data)
            df.columns = df.iloc[0].values
            df = df[1:]
            
            ## mapping data
            df = df.groupby('USER_ID')
            df = df.agg(lambda x: '+'.join(x.unique())).reset_index()
            set_value.update({
                'ApplicationCode': "CUM", 
                'AccountOwner': df['USER_ID'], 
                'AccountName': df['USER_ID'],
                'AccountType': "USR",
                'EntitlementName': df[['USER_ID', 'DEPARTMENT', 'GROUP_NO']].apply(lambda x: '#'.join(x), axis=1),
                'AccountStatus': "A",
                'IsPrivileged': "N",
                'CreateDate': df["VALID_FROM"].apply(pd.to_datetime, dayfirst=True).dt.strftime('%Y%m%d%H%M%S'), 
                'LastLogin': df["Last Usage"].apply(pd.to_datetime, dayfirst=True).dt.strftime('%Y%m%d%H%M%S'),
                'LastUpdatedDate': df["Last Change PWD"].apply(pd.to_datetime, dayfirst=True).dt.strftime('%Y%m%d%H%M%S'),
                'AdditionalAttribute': df[['USER_ID', 'DEPARTMENT']].apply(lambda x: ';'.join(x), axis=1),
                'Country': "TH"
            })
            df = df.assign(**set_value)
            df = df.drop(df.iloc[:,:14].columns, axis=1)
            
        except Exception as err:
            raise Exception(err)

        status = "succeed"
        self.logging[i].update({'data': df.to_dict('list'), 'status': status})
        logging.info(f"Collect data from file: {self.logging[i]['full_input']}, status: {status}")

    def collect_param(self, i: int, format_file: any) -> dict:

        status = "failed"
        self.logging[i].update({'function': "collect_param", 'status': status})
        columns = self.logging[i]['columns']
