import re

class hCommand:
    """! @brief Command  """
    def __init__(self,
                 command_name,
                 regExp,
                 permission=None,
                 arguments=[],
                 help=""):
        self.command_name = command_name
        self.re = re.compile(regExp)
        self.arguments = arguments
        self.permission = permission
        self.help = help

