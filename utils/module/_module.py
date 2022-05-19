from log import i_messages

class module(i_messages):
    
    def __init__(self ):
        i_messages.__init__(self)
        self.__modules=set()
        

     
    @property
    def modules(self):
        """Module List to add"""
        return self.__modules

    @modules.setter
    def modules(self, value):
        """You have to add value as LIST"""
        self.__modules.update([value] if type(value)==str else value)
        
    @modules.deleter
    def modules(self):
        self.print_messages(1011)
        self.__modules=set()
           
        
        
    def check(self):
            self.print_messages(1013)
            self.__global_imports('sys')
            self.__global_imports('subprocess')
            self.__global_imports('pkg_resources')

            installed = {pkg.key for pkg in pkg_resources.working_set}
            self.__missing = self.__modules - installed
            #print("---------------",self.__missing)

            if self.__missing:
                self.__install_modules()
                
    def __install_modules(self): 
            self.print_messages(1012,self.__missing)
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *self.__missing], stdout=subprocess.DEVNULL)                
            
    def __global_imports(self, modulename, shortname = None):
            if shortname is None: 
                shortname = modulename

            #print("\t","Import:",modulename,"as",shortname)
            if '.' not in modulename:
                globals()[shortname] = __import__(modulename)
            else:
                *modulename1, modulename2 = modulename.split('.')
                modulename1 = ".".join(modulename1)
                self.global_imports(modulename1)
                globals()[shortname] = eval(modulename1 + "." + modulename2)
