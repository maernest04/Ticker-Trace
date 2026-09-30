from market_execution_lab.validation_service import validate_duplicate_fills


def test_duplicate_fill_validation_counts_duplicate_events_without_duplicate_fills() -> None:
    result = validate_duplicate_fills(event_count=100, duplicate_every=10)

    assert result.received_events == 100
    assert result.duplicate_events == 10
    assert result.duplicate_fills == 0
    assert result.passed
