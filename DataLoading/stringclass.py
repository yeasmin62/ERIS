class stringClass:
    def truncf(self,fname):
        #print(self)
        st = str(fname)
        l = len(st)
        if(len(st)==6):
            if(st[0]=='-' and st[l-2]!='.'):
                return st[:1] + '0' + st[1:]         
            elif(st[0]=='-' and st[l-2]=='.'):
                return st[:] + '0'
            else:
                st_split1 = st[:0] + '+' + st[0:]   
                return st_split1
        elif(len(st)==5):
            if(st[0]=='-' and st[l-2] != '.'):
                return st[:1] + '00' + st[1:]         
            elif(st[0]=='-' and st[l-2] == '.'):
                return st[:1] + '0' + st[1:] + '0'
            elif(st[0].isdigit() and st[l-2] == '.'):
                return st[:0] + '+' + st[0:] + '0'
            else:
                st_split1 = st[:0] + '+0' + st[0:]   
                return st_split1
                
        elif(len(st)==4):
            if(st[0]=='-' and st[l-2] != '.'):
                return st[:1] + '00' + st[1:]         
            elif(st[0]=='-' and st[l-2] == '.'):
                return st[:1] + '00' + st[1:] + '0'
            elif(st[0].isdigit() and st[l-2] == '.'):
                return st[:0] + '+0' + st[0:] + '0'
            else:
                return st[:0] + '+00' + st[0:]   
        elif(len(st)==3):
            if(st[0].isdigit() and st[l-2] == '.'):
                return st[:0] + '+00' + st[0:] + '0'
        else:
            return fname
