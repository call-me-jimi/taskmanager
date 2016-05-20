import re

class hCommand( object ):
    """! @brief Command  """
    def __init__(self,
                 command_name,
                 regExp,
                 arguments=[],
                 groups = [],
                 permission=None,
                 help=""):
        self.command_name = command_name
        self.arguments = arguments
        self.groups = groups
        self.re = re.compile(regExp)
        self.permission = permission
        self.help = help

