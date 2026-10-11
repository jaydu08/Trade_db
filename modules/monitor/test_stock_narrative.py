import unittest

from modules.monitor.stock_narrative import growth_clue, profile_highlight, _clean


class StockNarrativeTest(unittest.TestCase):
    def test_growth_only_quotes_explicit_direction(self):
        self.assertEqual(growth_clue('公司销售手机和电脑。'), '')
        self.assertEqual(growth_clue('公司销售手机和电脑。未来将拓展海外市场。'), '未来将拓展海外市场。')

    def test_profile_text_is_bounded_and_whitespace_normalized(self):
        self.assertEqual(_clean('  手机\n 电脑  ', 20), '手机 电脑')
        self.assertEqual(len(_clean('a' * 500, 120)), 120)

    def test_profile_highlight_skips_founding_boilerplate(self):
        text = '公司成立于1998年。公司主要为企业客户提供网络安全产品与服务。'
        self.assertEqual(profile_highlight(text), '公司主要为企业客户提供网络安全产品与服务。')
        text = '公司是世界领先的科技公司。公司提供社交服务与电子游戏产品。还提供云计算业务和企业软件服务。'
        self.assertEqual(profile_highlight(text), '公司提供社交服务与电子游戏产品。还提供云计算业务和企业软件服务。')


if __name__ == '__main__':
    unittest.main()
