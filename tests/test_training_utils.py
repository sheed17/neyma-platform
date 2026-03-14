from backend.ml.training_utils import (
    group_split_rows,
    load_market_specs,
    market_key_from_row,
    normalize_market,
)


def test_load_market_specs_dedupes_and_normalizes(tmp_path):
    market_file = tmp_path / "markets.txt"
    market_file.write_text("Phoenix, az\n# comment\nSeattle, WA\nphoenix, AZ\n", encoding="utf-8")

    specs = load_market_specs(["Denver, co"], str(market_file))

    assert [item["raw"] for item in specs] == ["Denver, CO", "Phoenix, AZ", "Seattle, WA"]
    assert [item["market_key"] for item in specs] == ["denver|CO", "phoenix|AZ", "seattle|WA"]


def test_market_key_from_row_prefers_explicit_market_key():
    assert market_key_from_row({"market_key": "boston|MA", "city": "Phoenix", "state": "AZ"}) == "boston|ma"
    assert normalize_market("Phoenix", "AZ") == ("phoenix", "AZ", "phoenix|AZ")


def test_group_split_rows_respects_market_holdout():
    rows = [
        {"place_id": "p1", "city": "Phoenix", "state": "AZ", "market_key": "phoenix|AZ", "target": 1},
        {"place_id": "p2", "city": "Phoenix", "state": "AZ", "market_key": "phoenix|AZ", "target": 0},
        {"place_id": "p3", "city": "Denver", "state": "CO", "market_key": "denver|CO", "target": 1},
        {"place_id": "p4", "city": "Denver", "state": "CO", "market_key": "denver|CO", "target": 0},
        {"place_id": "p5", "city": "Seattle", "state": "WA", "market_key": "seattle|WA", "target": 1},
        {"place_id": "p6", "city": "Seattle", "state": "WA", "market_key": "seattle|WA", "target": 0},
        {"place_id": "p7", "city": "Miami", "state": "FL", "market_key": "miami|FL", "target": 1},
        {"place_id": "p8", "city": "Miami", "state": "FL", "market_key": "miami|FL", "target": 0},
    ]

    train_idx, val_idx, test_idx, split_meta = group_split_rows(
        rows,
        seed=42,
        target_column="target",
        group_by="market",
        holdout_groups=["seattle|wa"],
    )

    assert set(test_idx) == {4, 5}
    assert set(train_idx + val_idx + test_idx) == set(range(len(rows)))
    assert split_meta["group_by"] == "market"
    assert split_meta["holdout_groups"] == ["seattle|wa"]
