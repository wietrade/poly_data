"""Tier-1 unit tests for the CLOB market row-mapping in update_markets."""

import base64

from update_utils.update_markets import (
    _cursor,
    _flatten,
    _read_tail_ids,
    _row,
    _token_ids,
)


class TestReadTailIds:
    def test_last_ids(self, tmp_path):
        p = tmp_path / "m.csv"
        p.write_text("id,clobTokenIds\nc1,x\nc2,y\nc3,z\n")
        assert _read_tail_ids(str(p), 2) == {"c2", "c3"}

    def test_excludes_header(self, tmp_path):
        p = tmp_path / "m.csv"
        p.write_text("id,clobTokenIds\nc1,x\n")
        assert _read_tail_ids(str(p), 10) == {"c1"}

    def test_missing_file(self, tmp_path):
        assert _read_tail_ids(str(tmp_path / "nope.csv"), 10) == set()


class TestCursor:
    def test_zero(self):
        assert _cursor(0) == base64.b64encode(b"0").decode()

    def test_offset(self):
        assert _cursor(1000) == "MTAwMA=="


class TestTokenIds:
    def test_pair(self):
        m = {"tokens": [{"token_id": "111"}, {"token_id": 222}]}
        assert _token_ids(m) == ["111", "222"]

    def test_missing_or_empty(self):
        assert _token_ids({}) == []
        assert _token_ids({"tokens": None}) == []
        assert _token_ids({"tokens": [{"outcome": "Yes"}]}) == []


class TestFlatten:
    def test_none_becomes_empty(self):
        assert _flatten(None) == ""

    def test_scalars_passthrough(self):
        assert _flatten("x") == "x"
        assert _flatten(5) == 5

    def test_nested_json_encoded(self):
        assert _flatten(["a", "b"]) == '["a", "b"]'
        assert _flatten({"k": 1}) == '{"k": 1}'


class TestRow:
    def test_maps_condition_id_and_tokens(self):
        m = {
            "condition_id": "0xabc",
            "tokens": [{"token_id": "1"}, {"token_id": "2"}],
            "question": "Q?",
            "closed": True,
        }
        row = _row(m, ["id", "clobTokenIds", "question", "closed"])
        assert row == ["0xabc", '["1", "2"]', "Q?", True]

    def test_missing_tokens_blank(self):
        row = _row({"condition_id": "0xdef"}, ["id", "clobTokenIds"])
        assert row == ["0xdef", ""]

    def test_lean_columns_skip_useless_fields(self):
        # Lean mode must NOT include image/icon/rewards/description-dupes.
        from update_utils.update_markets import LEAN_MARKET_COLUMNS

        assert "image" not in LEAN_MARKET_COLUMNS
        assert "icon" not in LEAN_MARKET_COLUMNS
        assert "rewards" not in LEAN_MARKET_COLUMNS
        assert "fpmm" not in LEAN_MARKET_COLUMNS
        assert "neg_risk_market_id" not in LEAN_MARKET_COLUMNS
        # And must keep what the pipeline reads.
        for required in ("id", "clobTokenIds", "market_slug", "condition_id"):
            assert required in LEAN_MARKET_COLUMNS

    def test_row_only_pulls_requested_columns(self):
        # _row respects the columns list — a lean column set just never asks
        # for the big/useless fields, even when the market dict has them.
        m = {
            "condition_id": "0xabc",
            "tokens": [{"token_id": "1"}, {"token_id": "2"}],
            "market_slug": "btc-updown-15m-1",
            "question": "Q?",
            "image": "https://x/y.png",
            "icon": "https://x/y.png",
            "description": "huge template text",
            "rewards": {"rates": None},
        }
        cols = ["id", "clobTokenIds", "market_slug", "question"]
        row = _row(m, cols)
        assert row == ["0xabc", '["1", "2"]', "btc-updown-15m-1", "Q?"]
        # image/icon/description/rewards are simply not requested -> not in row
        assert len(row) == len(cols)
