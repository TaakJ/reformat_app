import asyncio
from .module_adm import ModuleADM
from .module_bos import ModuleBOS
from .module_cum import ModuleCUM
from .module_doc import ModuleDOC
from .module_ica import ModuleICA
from .module_iic import ModuleIIC
from .module_lds import ModuleLDS
from .module_lmt import ModuleLMT
from .module_moc import ModuleMOC
from .setup import PARAMS, setup_folder, setup_log
from .function import CollectBackup, ClearUp

class RunModule:
    def __init__(self) -> None:
        setup_folder()
        setup_log()
        CollectBackup()
        ClearUp()
        
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.results = self.loop.run_until_complete(self.mapping_module())
        
    async def mapping_module(self):
        coros = []
        for module in PARAMS["source"]:
            if module == "ADM":
                tasks = ModuleADM().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "BOS":
                tasks = ModuleBOS().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "CUM":
                tasks = ModuleCUM().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "DOC":
                tasks = ModuleDOC().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "ICA":
                tasks = ModuleICA().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "IIC":
                tasks = ModuleIIC().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "LDS":
                tasks = ModuleLDS().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "LMT":
                tasks = ModuleLMT().Run(module)
                coros.append(asyncio.create_task(tasks))

            elif module == "MOC":
                tasks = ModuleMOC().Run(module)
                coros.append(asyncio.create_task(tasks))

        return await asyncio.wait(coros)

class StartApp(RunModule):
    pass
