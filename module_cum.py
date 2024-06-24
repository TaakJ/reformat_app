from func import call_function
from exception import CustomException
from setup import setup_errorlog
import pandas as pd
import logging

class module_cum(call_function):
    
    def _params_setter(self, module:str) -> None:
        return super()._params_setter(module)
    
    def _log_setter(self, log) -> None:
        self._log = log
    
    async def run(self, module) -> dict:
        self._params_setter(module)
        
        logging.info(f'Module: "{self.module}", Manual: "{self.manual}", Batch Date: "{self.batch_date}", Store Tmp: "{self.store_tmp}", Write Mode: "{self.write_mode}"')
        
        result = {"module": self.module, "task": "Completed"}
        try:
            await self.check_source_files()
            await self.retrieve_data_from_source_files()
            # await self.mapping_column()
            await self.mock_data()
            if self.store_tmp is True:
                await self.write_data_to_tmp_file()
            await self.write_data_to_target_file()
        
        except CustomException as err:
            
            logging.error('See Error Details in "_error.log"')
            
            logger = setup_errorlog(log_name=__name__)
            while True:
                try:
                    logger.error(next(err))
                except StopIteration:
                    break
            
            result.update({"task": "Uncompleted"})
            
        logging.info("Stop Run Module\n")
        return result
        
        
    async def mapping_column(self) -> None:
        
        state = "failed"
        for record in self.logging:
            record.update({"function": "mapping_module_cum", "state": state})
            try:
                for sheet, data in record["data"].items():
                    logging.info(f'Mapping Column From Sheet: "{sheet}"')
        
                    if "USER REPORT" in sheet:
                        df = pd.DataFrame(data)
                        df.columns = df.iloc[0].values
                        df = df[1:]
                        df = df.reset_index(drop=True)
                
            except Exception as err:
                record.update({'err': err})

                
    async def mock_data(self) -> None:
            mock_data = [['ApplicationCode',	'AccountOwner', 'AccountName',	'AccountType',	'EntitlementName',	'SecondEntitlementName','ThirdEntitlementName', 'AccountStatus',	'IsPrivileged',	'AccountDescription',
                        'CreateDate','LastLogin','LastUpdatedDate',	'AdditionalAttribute'],
                        ["MOCK1","2","3","4","5","6","7","8","9","10",self.fmt_batch_date, self.date, self.fmt_batch_date,"14"],
                        ["MOCK2","16","17","18","19","20","21","22","23","24",self.fmt_batch_date, self.date, self.fmt_batch_date,"28"],
                        ]
            df = pd.DataFrame(mock_data)
            df.columns = df.iloc[0].values
            df = df[1:]
            df = df.reset_index(drop=True)
            self.logging.append({"module": "Target_file", "data": df.to_dict('list')})