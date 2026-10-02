import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID
from pydantic import ValidationError

from market_execution_lab.models import MarketEvent, QuoteEvent, TradeEvent
from market_execution_lab.observability import log_event


ALPACA_IEX_URL = "wss://stream.data.alpaca.markets/v2/iex"
INGESTION_CONTROL_STREAM = "ingestion.control"


@dataclass(frozen=True)
class AlpacaSettings:
    api_key: str
    api_secret: str
    symbols: tuple[str, ...]
    url: str = ALPACA_IEX_URL

    @classmethod
    def from_environment(cls) -> "AlpacaSettings":
        api_key = os.getenv("ALPACA_API_KEY")
        api_secret = os.getenv("ALPACA_API_SECRET")
        symbols = tuple(symbol.strip().upper() for symbol in os.getenv("ALPACA_SYMBOLS", "").split(",") if symbol.strip())
        if not api_key or not api_secret or not symbols:
            raise ValueError("ALPACA_API_KEY, ALPACA_API_SECRET, and ALPACA_SYMBOLS are required")
        if len(set(symbols)) > 10:
            raise ValueError("private live mode supports at most ten symbols")
        return cls(api_key=api_key, api_secret=api_secret, symbols=symbols)


def normalize_alpaca_message(
    message: dict[str, object],
    run_id: UUID,
    partition: int,
    sequence: int,
    ingested_at: datetime,
) -> MarketEvent | None:
    message_type = message.get("T")
    if message_type not in {"q", "t"}:
        return None
    event_time = datetime.fromisoformat(str(message["t"]).replace("Z", "+00:00"))
    symbol = str(message["S"])
    if message_type == "q":
        return QuoteEvent(
            event_id=f"alpaca:q:{symbol}:{message['t']}:{message['bp']}:{message['ap']}:{message['bs']}:{message['as']}",
            run_id=run_id,
            symbol=symbol,
            event_time=event_time,
            ingested_at=ingested_at,
            sequence=sequence,
            partition=partition,
            bid_price=Decimal(str(message["bp"])),
            bid_size=int(message["bs"]) * 100,
            ask_price=Decimal(str(message["ap"])),
            ask_size=int(message["as"]) * 100,
        )
    return TradeEvent(
        event_id=f"alpaca:t:{symbol}:{message['i']}",
        run_id=run_id,
        symbol=symbol,
        event_time=event_time,
        ingested_at=ingested_at,
        sequence=sequence,
        partition=partition,
        price=Decimal(str(message["p"])),
        size=int(message["s"]),
    )


async def stream_alpaca(
    settings: AlpacaSettings,
    run_id: UUID,
    partition_for_symbol: Callable[[str], int],
    publish: Callable[[MarketEvent], Awaitable[None]],
    subscription_updates: AsyncIterator[tuple[str, ...]] | None = None,
    on_status: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed

    sequence = 0
    symbols = set(settings.symbols)
    delay = 1
    update_task = asyncio.create_task(subscription_updates.__anext__()) if subscription_updates is not None else None
    receive_task = None
    try:
        while True:
            try:
                if on_status:
                    await on_status("connecting")
                async with connect(settings.url, open_timeout=10, ping_interval=20, ping_timeout=10) as websocket:
                    await websocket.send(json.dumps({"action": "auth", "key": settings.api_key, "secret": settings.api_secret}))
                    authenticated = False
                    async with asyncio.timeout(10):
                        while not authenticated:
                            for message in json.loads(await websocket.recv()):
                                if message.get("T") == "error":
                                    raise PermissionError("Alpaca authentication rejected")
                                authenticated |= message.get("T") == "success" and message.get("msg") == "authenticated"
                    await websocket.send(json.dumps({"action": "subscribe", "quotes": sorted(symbols), "trades": sorted(symbols)}))
                    if on_status:
                        await on_status("connected")
                    while True:
                        receive_task = asyncio.create_task(websocket.recv())
                        tasks = {receive_task}
                        if update_task is not None:
                            tasks.add(update_task)
                        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        if update_task in done:
                            updated_symbols = set(update_task.result())
                            for action, changed_symbols in subscription_actions(symbols, updated_symbols):
                                await websocket.send(json.dumps({"action": action, "quotes": changed_symbols, "trades": changed_symbols}))
                            symbols = updated_symbols
                            update_task = asyncio.create_task(subscription_updates.__anext__())
                        if receive_task not in done:
                            receive_task.cancel()
                            await asyncio.gather(receive_task, return_exceptions=True)
                            continue
                        for message in json.loads(receive_task.result()):
                            if message.get("T") == "error":
                                raise PermissionError("Alpaca stream subscription rejected")
                            sequence += 1
                            try:
                                event = normalize_alpaca_message(message, run_id, partition_for_symbol(str(message.get("S", ""))), sequence, datetime.now(UTC))
                            except (KeyError, TypeError, ValueError, ValidationError):
                                log_event("alpaca_message_rejected", run_id=str(run_id))
                                continue
                            if event is not None:
                                await publish(event)
                                delay = 1
            except PermissionError:
                raise
            except (OSError, ConnectionClosed, TimeoutError):
                if on_status:
                    await on_status("disconnected")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
    finally:
        for task in (receive_task, update_task):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


def subscription_actions(current_symbols: set[str], updated_symbols: set[str]) -> tuple[tuple[str, list[str]], ...]:
    additions = sorted(updated_symbols - current_symbols)
    removals = sorted(current_symbols - updated_symbols)
    actions: list[tuple[str, list[str]]] = []
    if additions:
        actions.append(("subscribe", additions))
    if removals:
        actions.append(("unsubscribe", removals))
    return tuple(actions)


def require_private_live_mode() -> None:
    if os.getenv("APP_MODE") != "private_live":
        raise ValueError("Alpaca ingestion requires APP_MODE=private_live")
