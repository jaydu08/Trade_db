import unittest

from modules.monitor.stock_evidence import _company_name, _safe_link, _us_headline_matches, _priority


class StockEvidenceTest(unittest.TestCase):
    def test_links_are_scoped_and_safe(self):
        self.assertEqual(_safe_link('http://www.cninfo.com.cn/new/disclosure/detail?stockCode=00700', 'HK'),
                         'https://www.cninfo.com.cn/new/disclosure/detail?stockCode=00700')
        self.assertEqual(_safe_link('https://evil.example/new/disclosure/detail', 'CN'), '')
        self.assertEqual(_safe_link('javascript:alert(1)', 'US'), '')
        self.assertEqual(_safe_link('https://127.0.0.1/private', 'US'), '')

    def test_us_news_requires_company_identity_not_just_related_ticker(self):
        self.assertEqual(_company_name('Apple Inc. Common Stock'), 'Apple')
        self.assertTrue(_us_headline_matches('Apple announces new services', 'AAPL', 'Apple Inc. Common Stock'))
        self.assertFalse(_us_headline_matches('AT&T launches iPhone bundle', 'AAPL', 'Apple Inc. Common Stock'))
        self.assertFalse(_us_headline_matches('AAPL Stonks: Top Apple Meme Coins', 'AAPL', 'Apple Inc. Common Stock'))

    def test_material_filing_is_ranked_before_routine_report(self):
        self.assertGreater(_priority('2026年前三季度业绩预告'), _priority('翌日披露报表'))
        self.assertEqual(_priority('截至九月三十日之证券变动月报表'), 0)


if __name__ == '__main__':
    unittest.main()
