import asyncio
from module_adm import module_adm
from module_bos import module_bos
from module_cum import module_cum
from setup import PARAMS


class run_module:
    
    def __init__(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.results = loop.run_until_complete(self.mapping_module())
    
    async def mapping_module(self):
        coros = []
        for module in PARAMS["source"]:
            
            if module == "ADM":
                tasks  = module_adm().run(module)
                coros.append(asyncio.create_task(tasks))
                
            elif module == "BOS":
                tasks  = module_bos().run(module)
                coros.append(asyncio.create_task(tasks))
                
            elif module == "CUM":
                tasks  = module_cum().run(module)
                coros.append(asyncio.create_task(tasks))
                
        return await asyncio.wait(coros)
        
class start_app(run_module):
    pass
            
            