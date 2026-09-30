import asyncio

import pytest

from market_execution_lab.alpaca import AlpacaSettings
from market_execution_lab.live_peak_service import measure_live_peak


def test_live_peak_measurement_rejects_nonpositive_duration() -> None:
    settings = AlpacaSettings(api_key="key", api_secret="secret", symbols=("AAPL",))

    with pytest.raises(ValueError, match="duration_seconds must be positive"):
        asyncio.run(measure_live_peak(settings, 0))
