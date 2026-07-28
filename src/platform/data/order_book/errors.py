from __future__ import annotations


class LocalOrderBookError(ValueError):
    """A snapshot or update cannot be applied without corrupting local state."""


class OrderBookSequenceGap(RuntimeError):
    def __init__(
        self,
        *,
        symbol: str,
        expected_prev_seq_id: int | None,
        received_prev_seq_id: int,
        received_seq_id: int,
        last_event_time_ms: int | None,
    ) -> None:
        self.symbol = symbol
        self.expected_prev_seq_id = expected_prev_seq_id
        self.received_prev_seq_id = received_prev_seq_id
        self.received_seq_id = received_seq_id
        self.last_event_time_ms = last_event_time_ms
        super().__init__(
            "OKX order book sequence gap | "
            f"symbol={symbol} "
            f"expected_prev_seq_id={expected_prev_seq_id} "
            f"received_prev_seq_id={received_prev_seq_id} "
            f"received_seq_id={received_seq_id} "
            f"last_event_time_ms={last_event_time_ms}"
        )


__all__ = ["LocalOrderBookError", "OrderBookSequenceGap"]
