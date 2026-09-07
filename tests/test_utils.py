"""Tier-1 unit tests for parsing/serialization helpers in poly_utils.utils."""

import polars as pl
import poly_utils.utils as u
import pytest
from poly_utils.utils import _flatten_value, _market_cond_id, _token_exprs


class TestTokenExprs:
    def _parse(self, values):
        df = pl.DataFrame({"clobTokenIds": values}).with_columns(_token_exprs())
        return df.select(["token1", "token2"]).rows()

    def test_pair(self):
        assert self._parse(['["a", "b"]']) == [("a", "b")]

    def test_single(self):
        assert self._parse(['["a"]']) == [("a", None)]

    def test_empty_null_and_garbage(self):
        # empty string, null, and non-JSON all yield (None, None)-ish, never raise
        assert self._parse(["", None]) == [(None, None), (None, None)]

    def test_numeric_token_ids(self):
        assert self._parse(['["1001", "1002"]']) == [("1001", "1002")]


class TestFlattenValue:
    def test_none_becomes_empty(self):
        assert _flatten_value(None) == ""

    def test_scalar_passthrough(self):
        assert _flatten_value("x") == "x"

    def test_nested_json_encoded(self):
        assert _flatten_value([1, 2]) == "[1, 2]"


class TestMarketCondId:
    def test_prefers_condition_id(self):
        assert _market_cond_id({"conditionId": "0xabc", "id": 1}) == "0xabc"

    def test_falls_back_to_id(self):
        assert _market_cond_id({"id": 123}) == "123"

    def test_empty_when_neither(self):
        assert _market_cond_id({}) == ""


class TestGetLeanMarketsSlugFilter:
    """get_lean_markets(slug_re=...) keeps only matching-market rows."""

    @pytest.fixture(autouse=True)
    def _isolate(self, tmp_path, monkeypatch):
        # Point the loader at two tiny CSV fixtures (markets + missing_markets).
        # clobTokenIds contains a comma, so it must be quoted in the CSV.
        csv_text = (
            "id,clobTokenIds,market_slug\n"
            '0xAAA,"[""1001"",""1002""]",btc-updown-5m-1788374100\n'
            '0xBBB,"[""2001"",""2002""]",btc-updown-15m-1788373800\n'
            '0xCCC,"[""3001"",""3002""]",eth-something-else\n'
        )
        markets = tmp_path / "markets.csv"
        markets.write_text(csv_text, encoding="utf-8")
        missing = tmp_path / "missing_markets.csv"
        missing.write_text("", encoding="utf-8")
        monkeypatch.setattr(u, "MARKETS_CSV", str(markets))
        monkeypatch.setattr(u, "MISSING_MARKETS_CSV", str(missing))
        yield

    def test_no_filter_keeps_all(self):
        df = u.get_lean_markets()
        assert sorted(df["id"].to_list()) == ["0xAAA", "0xBBB", "0xCCC"]

    def test_filter_matches_slug_subset(self):
        df = u.get_lean_markets(slug_re=r"^(btc|eth|sol)-updown-(5m|15m)-\d+$")
        assert sorted(df["id"].to_list()) == ["0xAAA", "0xBBB"]
        # token1/token2 still derived from clobTokenIds
        assert sorted(df["token1"].to_list()) == ["1001", "2001"]

    def test_filter_no_match_empty(self):
        df = u.get_lean_markets(slug_re=r"^doge-updown-1m-\d+$")
        assert df.height == 0
