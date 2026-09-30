from market_execution_lab.benchmark_service import _percentile, _symbols_for_partitions
from market_execution_lab.pipeline import PARTITION_COUNT, partition_for_symbol


def test_benchmark_symbols_cover_fixed_unique_partitions() -> None:
    symbols = _symbols_for_partitions(PARTITION_COUNT)

    assert len(symbols) == PARTITION_COUNT
    assert len({partition_for_symbol(symbol) for symbol in symbols}) == PARTITION_COUNT


def test_percentile_selects_an_observed_p95_value() -> None:
    assert _percentile([1.0, 2.0, 3.0, 4.0], 0.95) == 4.0
