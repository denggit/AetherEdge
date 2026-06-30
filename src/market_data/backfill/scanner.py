from __future__ import annotations

import time

from src.market_data.backfill.coverage import bucket_start_from_end, current_closed_bucket_end_ms
from src.market_data.backfill.models import BucketGap, RangeSpeedCoverage
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.warmup.gap_detector import interval_to_ms


class RangeBackfillScanner:
    def __init__(self, store: SqliteRangeCheckpointStore) -> None:
        self.store = store

    def scan(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_interval: str,
        required_buckets: int,
        lookback_buckets: int,
        now_ms: int | None = None,
        direction: str = "oldest-to-recent",
    ) -> RangeSpeedCoverage:
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        bucket_ms = interval_to_ms(bucket_interval)
        closed_end = current_closed_bucket_end_ms(now, bucket_interval)
        rows = self.store.load_complete_history(
            exchange=exchange,
            symbol=symbol,
            range_pct=range_pct,
            before_bucket_end_ms=closed_end + 1,
            limit=max(int(lookback_buckets), int(required_buckets), 1),
        )
        complete_ends = {row.bucket_end_ms for row in rows}
        latest_complete = max(complete_ends) if complete_ends else None
        count_window = max(int(lookback_buckets), int(required_buckets), 1)
        ends = [closed_end - offset * bucket_ms for offset in range(count_window)]
        gaps = [
            BucketGap(
                bucket_start_ms=bucket_start_from_end(end, bucket_interval),
                bucket_end_ms=end,
            )
            for end in ends
            if end not in complete_ends
        ]
        if direction in {"oldest-to-recent", "oldest_to_recent"}:
            gaps.sort(key=lambda item: item.bucket_end_ms)
        else:
            gaps.sort(key=lambda item: item.bucket_end_ms, reverse=True)
        return RangeSpeedCoverage(
            symbol=symbol,
            exchange=str(exchange).lower(),
            range_pct=str(range_pct),
            bucket_interval=bucket_interval,
            complete_history=len(complete_ends),
            required_buckets=int(required_buckets),
            missing_buckets=tuple(gaps),
            current_closed_bucket_end_ms=closed_end,
            latest_complete_bucket_end_ms=latest_complete,
        )
