class CustomException(Exception):
    def __init__(self,*args: tuple, **kwargs: dict):
        self.__dict__.update(kwargs)
        
        for key, value in self.__dict__.items():
            setattr(self, key, value)
            
        print(self.__dict__)
        
        # self.msg_err = self.generate_error()
        # print(self.msg_err)
    
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.msg_err)

    def generate_error(self):
        a = self.err
        print(a)
        try:
            for i in range(len(self.err)):
                error_message = f'''Module::{self.errors[i]["module"]} 
                                , Path::{self.errors[i]["input_dir"]}
                                , Function::{self.errors[i]["function"]}
                                , Status::{self.errors[i]["state"]}
                                , Error::{self.errors[i].get("errors", "No Error")}
                                '''
                yield error_message
        except:
            pass
