import datetime
import re
import unittest
from unittest.mock import MagicMock, patch

import requests

from eurovoc_miner.fetcher import get_json_response, get_sparql_query


class SparqlPaginationTests(unittest.TestCase):
    day = datetime.date(2026, 5, 31)

    def response(self, rows, headers=None):
        response = MagicMock()
        response.headers = headers or {}
        response.json.return_value = {'head': {'vars': ['work']}, 'results': {'bindings': rows}}
        return response

    def test_limit_is_only_on_final_results_with_stable_order(self):
        query = get_sparql_query(self.day, 'ENG', offset=1000)
        self.assertEqual(re.findall(r'\bLIMIT\s+(\d+)', query), ['1000'])
        self.assertIn('"2026-05-31"^^xsd:date', query)
        self.assertIn('"2026-06-01"^^xsd:date', query)
        self.assertRegex(query, r'GROUP BY.*\nORDER BY.*\nLIMIT 1000\nOFFSET 1000')

    def test_unsafe_page_sizes_are_rejected(self):
        for size in (0, 1001, 10000000):
            with self.assertRaises(ValueError):
                get_sparql_query(self.day, page_size=size)

    @patch('eurovoc_miner.fetcher.get_session')
    def test_full_page_fetches_next_page_and_combines_rows(self, factory):
        rows = [{'work': {'value': str(i)}} for i in range(1000)]
        tail = [{'work': {'value': '1000'}}]
        seen = []
        responses = iter([self.response(rows), self.response(tail)])
        def get(*args, **kwargs):
            seen.append(kwargs['params']['query'])
            return next(responses)
        factory.return_value.get.side_effect = get
        result = get_json_response(self.day)
        self.assertEqual(result['results']['bindings'], rows + tail)
        self.assertIn('OFFSET 0', seen[0])
        self.assertIn('OFFSET 1000', seen[1])

    @patch('eurovoc_miner.fetcher.get_session')
    def test_later_page_failure_is_not_accepted_as_partial_success(self, factory):
        factory.return_value.get.side_effect = [
            self.response([{'work': {'value': str(i)}} for i in range(1000)]),
            requests.ReadTimeout('second page timed out'),
        ]
        with self.assertRaises(requests.ReadTimeout):
            get_json_response(self.day)

    @patch('eurovoc_miner.fetcher.get_session')
    def test_partial_result_headers_fail_even_with_http_200(self, factory):
        for headers in ({'X-SQL-State': 'S1TAT'}, {'X-SPARQL-MaxRows': '1000'}):
            factory.return_value.get.return_value = self.response([], headers)
            with self.assertRaises(RuntimeError):
                get_json_response(self.day)

    @patch('eurovoc_miner.fetcher.get_session')
    def test_repeated_page_is_rejected(self, factory):
        rows = [{'work': {'value': str(i)}} for i in range(1000)]
        factory.return_value.get.side_effect = [self.response(rows), self.response(rows)]
        with self.assertRaisesRegex(RuntimeError, 'repeated a page'):
            get_json_response(self.day)

    @patch('eurovoc_miner.fetcher.get_session')
    def test_empty_day_is_valid(self, factory):
        factory.return_value.get.return_value = self.response([])
        self.assertEqual(get_json_response(self.day)['results']['bindings'], [])
        factory.return_value.get.assert_called_once()


if __name__ == '__main__':
    unittest.main()
