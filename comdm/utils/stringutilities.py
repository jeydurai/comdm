class BasicStringUtils:
    """Contains the methods to customized string manuplating functionalities"""


    @classmethod
    def to_upper(cls, s):
        """Checks if the string is a int and returns the uppercase of that string"""
        if not isinstance(s, int):
            return s.upper()
        return s
    
    @classmethod
    def to_lower(cls, s):
        """Checks if the string is a int and returns the lowercase of that string"""
        if not isinstance(s, int):
            return s.lower()
        return s
    
    
