class Parser_Error(Exception):
    pass



class ArgumentInputParserError(Parser_Error):
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
