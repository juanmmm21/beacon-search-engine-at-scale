"""Tests deterministas de `object_key_for_page`/`hash_shard_for_url`."""

from __future__ import annotations

from datetime import UTC, datetime

from beacon_scale_infra.crawl.partitioning import hash_shard_for_url, object_key_for_page


def test_hash_shard_for_url_is_deterministic_and_in_range() -> None:
    shard = hash_shard_for_url("https://example.com/a", 16)
    assert 0 <= shard < 16
    assert shard == hash_shard_for_url("https://example.com/a", 16)


def test_hash_shard_distributes_different_urls_across_shards() -> None:
    shards = {hash_shard_for_url(f"https://example.com/page-{i}", 16) for i in range(64)}
    # No garantiza uniformidad perfecta, pero 64 URLs distintas no deberían
    # colapsar todas en un único shard salvo un bug real en el hashing.
    assert len(shards) > 1


def test_object_key_for_page_has_date_and_shard_prefixes() -> None:
    fetched_at = datetime(2026, 7, 12, 10, 30, tzinfo=UTC)
    key = object_key_for_page(
        "https://example.com/a", fetched_at, prefix="crawl-pages", num_hash_shards=16
    )
    assert key.startswith("crawl-pages/date=2026-07-12/shard=")
    assert key.endswith(".json")


def test_object_key_for_page_is_deterministic_for_the_same_url_and_date() -> None:
    fetched_at = datetime(2026, 7, 12, 10, 30, tzinfo=UTC)
    key_a = object_key_for_page("https://example.com/a", fetched_at)
    key_b = object_key_for_page("https://example.com/a", fetched_at)
    assert key_a == key_b


def test_object_key_for_page_partitions_by_day_not_by_hour() -> None:
    key_morning = object_key_for_page(
        "https://example.com/a", datetime(2026, 7, 12, 1, 0, tzinfo=UTC)
    )
    key_evening = object_key_for_page(
        "https://example.com/a", datetime(2026, 7, 12, 23, 0, tzinfo=UTC)
    )
    assert key_morning == key_evening
