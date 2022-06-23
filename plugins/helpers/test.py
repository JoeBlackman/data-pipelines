class Test:

    def __init__(self, sql_query, test_condition, pass_message, fail_message):
        self.sql_query = sql_query
        self.test_condition = test_condition
        self.pass_message = pass_message
        self.fail_message = fail_message