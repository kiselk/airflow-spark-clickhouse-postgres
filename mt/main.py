import pandas as pd
from base_test import BaseTest
from with_exception import with_excepion


class Test_1_2(BaseTest):

    @with_excepion
    def execute(self):
        """ Execute test"""

        common_logins = pd.read_sql(
            self.get_sql('get_logins'), self.dbConnection)
        minute_deals = pd.read_sql(self.get_sql(
            'get_all_minute_deals'), self.dbConnection)
        buy_vs_sell = pd.read_sql(
            self.get_sql('get_all_buy_vs_sell_half_minute_counts'), self.dbConnection)

        result = pd.merge(
            common_logins, minute_deals, on='login', how='left'
        )
        result = pd.merge(result, buy_vs_sell, on='login', how='left')

        result.to_csv(self.OUTPUT_PATH, index=False)


class Test3(BaseTest):

    @with_excepion
    def execute(self):
        """Execute test"""
        sql = self.get_sql('get_all_suspicious_trades')
        all_pairs = pd.read_sql(
            sql, self.dbConnection)

        all_pairs.to_csv(self.OUTPUT_PATH, index=False)


if __name__ == '__main__':
    Test_1_2(test_name="test_1_2").execute()
    Test3(test_name='test_3').execute()
