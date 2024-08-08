import re
import pandas as pd
import logging
from .function import CallFunction
from .exception import CustomException


class ModuleADM(CallFunction):

    def __init__(self, params: any) -> None:
        for key, value in vars(params).items():
            setattr(self, key, value)

    def logSetter(self, log: list) -> None:
        self._log = log

    async def run_process(self) -> dict:

        logging.info(f'Module:"{self.module}"; Manual: "{self.manual}"; Run date: "{self.batch_date}"; Store tmp: "{self.store_tmp}"; Write mode: "{self.write_mode}";')

        result = {"module": self.module, "task": "Completed"}
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

            result.update({"task": "Uncompleted"})

        logging.info(f'Stop Run Module "{self.module}"\r\n')

        return result

    def collect_data(self, i: int, format_file: any) -> dict:

        status = "failed"
        module = self.logging[i]["module"]
        logging.info(f"Collect Data for module: {module}")

        self.logging[i].update({"function": "collect_data", "status": status})

        try:
            data = []
            for line in format_file:
                regex = re.compile(r"\w+.*")
                find_word = "".join(regex.findall(line)).strip()
                data += [re.sub(r"\W\s+", "||", find_word).split("||")]

            ## set dataframe
            df = pd.DataFrame(data)
            df = df.groupby(0)
            df = df.agg(lambda x: ",".join(x.unique())).reset_index()
            df = df.assign(**{"ApplicationCode": "ADM",
                            "AccountOwner": "",
                            "AccountName": "",
                            "AccountType": "USR",
                            "EntitlementName": "",
                            "SecondEntitlementName": "",
                            "ThirdEntitlementName": "",
                            "AccountStatus": "A",
                            "IsPrivileged": "N",
                            "AccountDescription": "",
                            "CreateDate": "",
                            "LastLogin": "",
                            "LastUpdatedDate": "",
                            "AdditionalAttribute": "",
                            "Country": "TH",})
            df["AccountOwner"] = df.loc[:,0]
            df["AccountName"] = df.loc[:,1]
            df["AdditionalAttribute"] = df[[4,5,6]].apply(lambda x: "#".join(x), axis =1)
            df = df.drop(df.loc[:, 0:6].columns, axis=1)
            df = self.set_initial_data_type(i, df)

        except Exception as err:
            raise Exception(err)
        
        status = "succeed"
        self.logging[i].update({"data": df.to_dict("list"), "status": status})
        logging.info(f'Collect data from file: {self.logging[i]["full_input"]}, status: {status}')
