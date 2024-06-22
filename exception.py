class CustomException(Exception):
    def __init__(self,*args: tuple, **kwargs: dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
            
        self.err_msg = self.generate_error()
        
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.msg_err)

    def generate_error(self):
        try:
            for i in range(len(self.err)):
                err_msg = f'''Module::{self.err[i]["module"]} 
                            , Path::{self.err[i]["input_dir"]}
                            , Function::{self.err[i]["function"]}
                            , Status::{self.err[i]["state"]}
                            , Error::{self.err[i].get("errors", "No Error")}
                            '''
                yield err_msg
        except:
            pass
