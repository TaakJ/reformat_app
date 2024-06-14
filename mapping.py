from log import record_log
from exception import CustomException
from setup import setup_log, Folder, clear_tmp
import pandas as pd
import logging

# """
                    # expect_data = [['ApplicationCode','AccountOwner', 'AccountName','AccountType','EntitlementName','SecondEntitlementName','ThirdEntitlementName', 
                    # 'AccountStatus','IsPrivileged',	'AccountDescription','CreateDate','LastLogin','LastUpdatedDate',	'AdditionalAttribute'],
                    # [1,2,3,4,5,6,7,8,9,10,args[0].batch_date.strftime('%Y-%m-%d'),12, args[0].date,14],
                    # [15,16,17,18,19,20,21,22,23,24,args[0].batch_date.strftime('%Y-%m-%d'),26, args[0].date,28],]
                    # """
class module_adm(record_log):
    
    def _log_setter(self, log):
        self._log = log
    
    async def run_module_adm(self, param):  
        
        for key, value in param.items():
            setattr(self, key, value)
        
        setup_log()
        self.state = True
        logging.info(f'Run Module: "ADM", manual: "{self.manual}", batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        
        try:
            self.require_source = ["ADM"]
            # await self.check_source_files()
            # await self.retrieve_data_from_source_files()
            # await self.mapping_module_adm()
                
        except CustomException as error: 
            logging.error("Error Exception")
            self.state = False
            
            while True:
                try:
                    logging.error(next(error))
                except StopIteration:
                    break
        logging.info("Stop Run Module\n##### End #####\n")
    
    async def mapping_module_adm(self):
        
        state = "failed"
        for record in self.logging:

            record.update({"function": "mapping_module_adm", "state": state})
            try:
                for sheet, data in record["data"].items():
                    logging.info(f'Mapping Column From Sheet: "{sheet}"')
                    
                    if "ADM" in sheet:
                        print(data)
                        
                        
            except Exception as err:
                print(err)
            
    
    
class module_bos(record_log):
    def _log_setter(self, log):
        self._log = log
        
    async def run_module_bos(self, param):  
        for key, value in param.items():
            setattr(self, key, value)
        
        self.state = True
        logging.info(f'Run Module: "BOS", manual: "{self.manual}", batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        try:
            self.require_source = ["BOS"]
            await self.check_source_files()
            await self.retrieve_data_from_source_files()
            await self.mapping_module_bos()
                
        except CustomException as error: 
            logging.error("Error Exception")
            self.state = False
            
            while True:
                try:
                    logging.error(next(error))
                except StopIteration:
                    break
        logging.info("Stop Run Module\n##### End #####\n")
        
    async def mapping_module_bos(self):
        
        state = "failed"
        for record in self.logging:

            record.update({"function": "mapping_module_bos", "state": state})
            try:
                for sheet, data in record["data"].items():
                    logging.info(f'Mapping Column From Sheet: "{sheet}"')
                    
                    if "BOS_export_BrUser" in sheet:
                        print(data)
                        
                    elif "BOS_export_role" in sheet:
                        raise Exception("raise Exception")
                        
            except Exception as err:
                print(err)
                # record.update({'errors': err})
        

        
class module_cum(record_log):
    
    async def run_module_cum(self, param):  
        for key, value in param.items():
            setattr(self, key, value)
        
        self.state = True
        logging.info(f'Run Module: "CUM", manual: "{self.manual}", batch_date: "{self.batch_date}", store_tmp: "{self.store_tmp}, write_mode: "{self.write_mode}"')
        
        try:
            self.require_source = ["CUM"]              
            await self.check_source_files()
            await self.retrieve_data_from_source_files()
            await self.mapping_module_cum()
                
        except CustomException as error: 
            logging.error("Error Exception")
            self.state = False
            
            while True:
                try:
                    logging.error(next(error))
                except StopIteration:
                    break
        logging.info("Stop Run Module\n##### End #####\n")
        
    async def mapping_module_cum(self):
        
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
                        print(df)
                
            except Exception as err:
                record.update({'errors': err})