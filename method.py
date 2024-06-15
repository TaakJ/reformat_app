import asyncio
from module import convert_2_files
from verify import method_files
from module_adm import module_adm
from module_bos import module_bos
from module_cum import module_cum


class start:
    def __init__(self, params:dict):
        self.params = params
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.mapping_module())
        

    async def mapping_module(self):
        for module in self.params["source"]:
            if module == "ADM":
                func  = module_adm()
                func._parameter(module, self.params)
                task = func.run()
                
            elif module == "BOS":
                func  = module_bos()
                func._parameter(module, self.params)
                task = func.run()
                
            elif module == "CUM":
                func  = module_cum()
                func._parameter(module, self.params)
                task = func.run()
                
            asyncio.create_task(task)
        
class run_module(start):
    pass
            
            