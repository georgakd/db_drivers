import abc
from abc import ABCMeta


class Operation:    
    """Base class for all operations of all interfaces"""
    __metaclass__ = ABCMeta
    
    @abc.abstractmethod
    def operAdd(self):
        """Add subscribers"""
        
    @abc.abstractmethod
    def operModify(self, how_many):
        """Modify subscribers"""
        
    @abc.abstractmethod
    def operSearch(self, how_many):
        """Search subscribers"""
        
    @abc.abstractmethod
    def operDelete(self, how_many):
        """Delete subscribers"""
        
    @abc.abstractmethod    
    def clearDB(self):
        """Clear Database from all entries"""
