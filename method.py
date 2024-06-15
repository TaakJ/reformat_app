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
                m  = module_adm()
                m._parameter(module, self.params)
                task = m.run()
                
            elif module == "BOS":
                m  = module_bos()
                m._parameter(module, self.params)
                task = m.run()
                
            elif module == "CUM":
                m  = module_cum()
                m._parameter(module, self.params)
                task = m.run()
                
            asyncio.create_task(task)
        
class run_module(start):
    pass
            
            