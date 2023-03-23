from p4p import Type


class P4pType:
    def __init__(self, t):
        if isinstance(t, Type):
            self.subtypes = {}
            self.id = t.getID()
            for k in t.keys():
                self.subtypes.update({k: P4pType(t[k])})
        else:
            self.type = t

    def has_subtypes(self):
        if "subtypes" in self.__dict__:
            return True
        return False

    def get_id(self):
        if "id" in self.__dict__:
            return self.id
        return None
