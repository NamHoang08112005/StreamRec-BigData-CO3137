"""Drop demo / pipeline noise from tags shown in feed, hot-tags, and affinity."""
from __future__ import annotations

# Tags injected by ETL mistakes, simulators, or legacy seeds — not from ShortVideo dataset.
NOISE_TAGS = frozenset(
    {
        "shortvideo",
        "short_video",
        "demo",
        "auto_upload",
        "coldstart",
        "cold_start",
    }
)

# FFmpeg heuristics — useful for ranking, not shown as consumer hashtags.
ENRICHED_INTERNAL = frozenset(
    {
        "short_form",
        "mid_form",
        "long_form",
        "vertical",
        "landscape",
        "square",
        "hd",
        "high_fps",
    }
)


def normalize_tag(tag: str) -> str:
    return str(tag or "").strip().lower().replace(" ", "_")


def is_noise_tag(tag: str) -> bool:
    t = normalize_tag(tag)
    return not t or t in NOISE_TAGS


def filter_content_tags(tags: list[str] | None) -> list[str]:
    """Creator/dataset tags used for affinity, content score, and UI."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in tags or []:
        t = normalize_tag(raw)
        if is_noise_tag(t) or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def filter_enriched_display(enriched: list[str] | None) -> list[str]:
    """Enriched tags kept in DB for ranking but hidden on Feed/Dashboard hot list."""
    return []


def filter_for_hot_tags(tags: list[str]) -> list[str]:
    cleaned = filter_content_tags(tags)
    return [t for t in cleaned if t not in ENRICHED_INTERNAL]