#!/usr/bin/env python3
"""
Polymarket Market Scanner & Arbitrage Finder

Usage:
    python3 tools/scan_markets.py                   # Full scan: top liquidity + arbitrage
    python3 tools/scan_markets.py --top 20           # Show top 20 markets by 24h volume
    python3 tools/scan_markets.py --search "bitcoin"  # Search by keyword
    python3 tools/scan_markets.py --arb              # Find arbitrage opportunities only
    python3 tools/scan_markets.py --event <slug>     # Show details for a specific event
    python3 tools/scan_markets.py --export           # Export all to markets.yaml format
"""

import argparse
import json
import sys
import os
from datetime import datetime, timezone
from dataclasses import dataclass

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

GAMMA_URL = "https://gamma-api.polymarket.com"


@dataclass
class MarketInfo:
    question: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    spread: float
    volume_24h: float
    total_volume: float
    liquidity: float
    end_date: str
    slug: str
    event_title: str
    group_item: str
    tick_size: float
    min_order: float


def fetch_all_markets(limit: int = 500) -> list[dict]:
    """Fetch all active markets from Gamma API."""
    all_markets = []
    for offset in range(0, limit, 100):
        try:
            resp = httpx.get(
                f"{GAMMA_URL}/markets",
                params={
                    "closed": "false",
                    "active": "true",
                    "limit": 100,
                    "offset": offset,
                },
                timeout=20,
            )
            data = resp.json()
            if not data:
                break
            all_markets.extend(data)
        except Exception as e:
            print(f"  [!] Page {offset} failed: {e}")
            break
    return all_markets


def fetch_all_events(limit: int = 200) -> list[dict]:
    """Fetch all active events from Gamma API."""
    all_events = []
    for offset in range(0, limit, 50):
        try:
            resp = httpx.get(
                f"{GAMMA_URL}/events",
                params={
                    "closed": "false",
                    "limit": 50,
                    "offset": offset,
                },
                timeout=20,
            )
            data = resp.json()
            if not data:
                break
            all_events.extend(data)
        except Exception as e:
            print(f"  [!] Events page {offset} failed: {e}")
            break
    return all_events


def parse_market(m: dict, event_title: str = "") -> MarketInfo:
    """Parse a raw market dict into MarketInfo."""
    prices = m.get("outcomePrices", "[]")
    if isinstance(prices, str):
        prices = json.loads(prices)
    yes_price = float(prices[0]) if len(prices) > 0 else 0
    no_price = float(prices[1]) if len(prices) > 1 else 0

    tokens = m.get("clobTokenIds", "[]")
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    yes_token = tokens[0] if len(tokens) > 0 else ""
    no_token = tokens[1] if len(tokens) > 1 else ""

    return MarketInfo(
        question=m.get("question", "?"),
        condition_id=m.get("conditionId", ""),
        yes_token_id=yes_token,
        no_token_id=no_token,
        yes_price=yes_price,
        no_price=no_price,
        spread=float(m.get("spread", 0) or 0),
        volume_24h=float(m.get("volume24hr", 0) or 0),
        total_volume=float(m.get("volumeNum", 0) or 0),
        liquidity=float(m.get("liquidity", 0) or 0),
        end_date=m.get("endDate", ""),
        slug=m.get("slug", ""),
        event_title=event_title,
        group_item=m.get("groupItemTitle", ""),
        tick_size=float(m.get("orderPriceMinTickSize", 0.01) or 0.01),
        min_order=float(m.get("orderMinSize", 5) or 5),
    )


# ============================================================
# Arbitrage Detection
# ============================================================

def find_binary_arb(markets: list[MarketInfo]) -> list[dict]:
    """
    Type 1: Binary market arbitrage.
    YES + NO should = $1.00.
    If YES + NO < 1: buy both → guaranteed profit.
    If YES + NO > 1: sell both (harder on Polymarket).
    """
    results = []
    for m in markets:
        total = m.yes_price + m.no_price
        gap = abs(total - 1.0)
        if gap > 0.02 and m.volume_24h > 1000:  # >2% gap and some volume
            direction = "BUY_BOTH" if total < 1.0 else "SELL_BOTH"
            profit_pct = gap * 100
            results.append({
                "type": "BINARY_ARB",
                "market": m.question[:60],
                "yes_price": m.yes_price,
                "no_price": m.no_price,
                "sum": total,
                "gap": gap,
                "profit_pct": profit_pct,
                "direction": direction,
                "condition_id": m.condition_id,
                "yes_token": m.yes_token_id,
                "no_token": m.no_token_id,
                "volume_24h": m.volume_24h,
                "liquidity": m.liquidity,
            })
    return sorted(results, key=lambda x: x["profit_pct"], reverse=True)


def find_temporal_arb(events: list[dict]) -> list[dict]:
    """
    Type 2: Temporal monotonicity arbitrage.
    For "X by date1" vs "X by date2" where date2 > date1:
    P(by date2) must be >= P(by date1).
    If violated, buy the later date and sell the earlier.
    """
    results = []
    for ev in events:
        markets = ev.get("markets", [])
        if len(markets) < 2:
            continue

        # Parse and sort by end date
        parsed = []
        for m in markets:
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            yes_price = float(prices[0]) if prices else 0
            end_date = m.get("endDate", "")
            vol = float(m.get("volumeNum", 0) or 0)
            if yes_price > 0 and yes_price < 1 and end_date:
                parsed.append({
                    "question": m.get("question", "")[:60],
                    "group_item": m.get("groupItemTitle", ""),
                    "yes_price": yes_price,
                    "end_date": end_date,
                    "volume": vol,
                    "condition_id": m.get("conditionId", ""),
                    "tokens": m.get("clobTokenIds", []),
                })

        parsed.sort(key=lambda x: x["end_date"])

        # Check monotonicity
        for i in range(len(parsed) - 1):
            early = parsed[i]
            late = parsed[i + 1]
            if early["yes_price"] > late["yes_price"] + 0.02:  # >2% violation
                gap = early["yes_price"] - late["yes_price"]
                results.append({
                    "type": "TEMPORAL_ARB",
                    "event": ev.get("title", "?")[:50],
                    "early_q": early["group_item"] or early["question"],
                    "early_price": early["yes_price"],
                    "early_date": early["end_date"][:10],
                    "late_q": late["group_item"] or late["question"],
                    "late_price": late["yes_price"],
                    "late_date": late["end_date"][:10],
                    "gap": gap,
                    "profit_pct": gap * 100,
                    "action": f"BUY '{late['group_item']}' YES + BUY '{early['group_item']}' NO",
                    "early_cid": early["condition_id"],
                    "late_cid": late["condition_id"],
                })

    return sorted(results, key=lambda x: x["profit_pct"], reverse=True)


def find_multi_outcome_arb(events: list[dict]) -> list[dict]:
    """
    Type 3: Multi-outcome completeness arbitrage.
    For mutually exclusive events (e.g. "Who will win?"),
    sum of all YES should = 1.0.
    """
    results = []
    for ev in events:
        markets = ev.get("markets", [])
        if len(markets) < 3:
            continue

        # Check if this is a "who will win" type event
        title = ev.get("title", "").lower()
        is_winner = any(kw in title for kw in ["winner", "nominee", "who will"])

        if not is_winner:
            continue

        total_yes = 0
        active_count = 0
        for m in markets:
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            yes = float(prices[0]) if prices else 0
            if yes > 0:
                total_yes += yes
                active_count += 1

        if active_count >= 3 and abs(total_yes - 1.0) > 0.05:
            gap = total_yes - 1.0
            results.append({
                "type": "MULTI_OUTCOME_ARB",
                "event": ev.get("title", "?")[:50],
                "num_outcomes": active_count,
                "sum_yes": total_yes,
                "overround": gap,
                "overround_pct": gap * 100,
                "note": "SUM > 1 = sell all YES; SUM < 1 = buy all YES" if gap != 0 else "",
            })

    return sorted(results, key=lambda x: abs(x["overround_pct"]), reverse=True)


def find_edge_opportunities(markets: list[MarketInfo]) -> list[dict]:
    """
    Type 4: High-spread / mispriced markets.
    Markets where spread is unusually wide = potential edge for market makers.
    """
    results = []
    for m in markets:
        if m.volume_24h < 500:
            continue
        if m.spread >= 0.04 and m.liquidity > 5000:
            mid = (m.yes_price + (1 - m.no_price)) / 2
            results.append({
                "type": "WIDE_SPREAD",
                "market": m.question[:60],
                "yes_price": m.yes_price,
                "no_price": m.no_price,
                "spread": m.spread,
                "midpoint": mid,
                "volume_24h": m.volume_24h,
                "liquidity": m.liquidity,
                "condition_id": m.condition_id,
                "yes_token": m.yes_token_id,
                "no_token": m.no_token_id,
            })
    return sorted(results, key=lambda x: x["spread"], reverse=True)


# ============================================================
# Display
# ============================================================

def print_top_markets(markets: list[MarketInfo], n: int = 20):
    """Print top N markets by 24h volume with full IDs."""
    sorted_m = sorted(markets, key=lambda x: x.volume_24h, reverse=True)
    print(f"\n{'='*90}")
    print(f" TOP {n} MARKETS BY 24H VOLUME")
    print(f"{'='*90}")
    print(f" {'#':>3} | {'YES':>5} | {'NO':>5} | {'Spread':>6} | {'24h Vol':>12} | {'Liq':>10} | Question")
    print(f" {'-'*3}-+-{'-'*5}-+-{'-'*5}-+-{'-'*6}-+-{'-'*12}-+-{'-'*10}-+-{'-'*35}")

    for i, m in enumerate(sorted_m[:n], 1):
        print(
            f" {i:3d} | {m.yes_price:5.3f} | {m.no_price:5.3f} | {m.spread:6.3f} | "
            f"${m.volume_24h:>11,.0f} | ${m.liquidity:>9,.0f} | {m.question[:40]}"
        )

    print(f"\n  To see full details: python3 tools/scan_markets.py --detail <number>")
    return sorted_m[:n]


def print_market_detail(m: MarketInfo):
    """Print full details of a market for config."""
    print(f"\n{'='*70}")
    print(f" MARKET DETAIL")
    print(f"{'='*70}")
    print(f" Question:      {m.question}")
    print(f" Event:         {m.event_title}")
    print(f" Slug:          {m.slug}")
    print(f" End Date:      {m.end_date}")
    print(f"")
    print(f" condition_id:  {m.condition_id}")
    print(f" YES token_id:  {m.yes_token_id}")
    print(f" NO  token_id:  {m.no_token_id}")
    print(f"")
    print(f" YES Price:     {m.yes_price:.4f}")
    print(f" NO  Price:     {m.no_price:.4f}")
    print(f" Sum:           {m.yes_price + m.no_price:.4f}")
    print(f" Spread:        {m.spread:.4f}")
    print(f" Tick Size:     {m.tick_size}")
    print(f" Min Order:     ${m.min_order}")
    print(f"")
    print(f" 24h Volume:    ${m.volume_24h:,.2f}")
    print(f" Total Volume:  ${m.total_volume:,.2f}")
    print(f" Liquidity:     ${m.liquidity:,.2f}")
    print(f"")
    print(f" --- markets.yaml snippet ---")
    print(f" - id: \"{m.slug}\"")
    print(f"   condition_id: \"{m.condition_id}\"")
    print(f"   token_id: \"{m.yes_token_id}\"")
    print(f"   no_token_id: \"{m.no_token_id}\"")
    print(f"{'='*70}")


def print_arbitrage(binary, temporal, multi, spread):
    """Print all arbitrage opportunities."""
    print(f"\n{'='*90}")
    print(f" ARBITRAGE SCAN RESULTS")
    print(f"{'='*90}")

    if binary:
        print(f"\n [1] BINARY MARKET MISPRICING (YES+NO != $1.00)")
        print(f"     Buy both sides when sum < $1 = guaranteed profit on settlement")
        print(f"     {'─'*70}")
        for a in binary[:10]:
            print(
                f"     {a['direction']:10s} | gap={a['profit_pct']:.1f}% | "
                f"YES={a['yes_price']:.3f} NO={a['no_price']:.3f} sum={a['sum']:.3f} | "
                f"vol=${a['volume_24h']:,.0f}"
            )
            print(f"       {a['market']}")
            print(f"       condition_id: {a['condition_id']}")
            print()
    else:
        print(f"\n [1] BINARY MISPRICING: None found (all YES+NO within 2% of $1.00)")

    if temporal:
        print(f"\n [2] TEMPORAL ARBITRAGE (later date cheaper than earlier date)")
        print(f"     P(event by June) must be >= P(event by March). Violations = free money.")
        print(f"     {'─'*70}")
        for a in temporal[:10]:
            print(
                f"     gap={a['profit_pct']:.1f}% | "
                f"{a['early_q'][:20]}={a['early_price']:.3f} ({a['early_date']}) > "
                f"{a['late_q'][:20]}={a['late_price']:.3f} ({a['late_date']})"
            )
            print(f"       Event: {a['event']}")
            print(f"       Action: {a['action']}")
            print()
    else:
        print(f"\n [2] TEMPORAL ARBITRAGE: None found (all date orderings consistent)")

    if multi:
        print(f"\n [3] MULTI-OUTCOME OVERROUND (winner markets)")
        print(f"     Sum of all YES > 1.0 = market overpriced. Sum < 1.0 = underpriced.")
        print(f"     {'─'*70}")
        for a in multi[:10]:
            print(
                f"     {a['event']:50s} | {a['num_outcomes']} outcomes | "
                f"sum={a['sum_yes']:.3f} | overround={a['overround_pct']:+.1f}%"
            )
    else:
        print(f"\n [3] MULTI-OUTCOME: None found")

    if spread:
        print(f"\n [4] WIDE SPREAD OPPORTUNITIES (market making edge)")
        print(f"     Wide spreads with decent liquidity = place limit orders on both sides")
        print(f"     {'─'*70}")
        for a in spread[:10]:
            print(
                f"     spread={a['spread']:.3f} | mid={a['midpoint']:.3f} | "
                f"liq=${a['liquidity']:,.0f} | vol=${a['volume_24h']:,.0f}"
            )
            print(f"       {a['market']}")
            print(f"       condition_id: {a['condition_id']}")
            print()
    else:
        print(f"\n [4] WIDE SPREAD: None found")

    total = len(binary) + len(temporal) + len(multi) + len(spread)
    print(f"\n Total opportunities found: {total}")
    print(f"{'='*90}")


def main():
    parser = argparse.ArgumentParser(description="Polymarket Market Scanner")
    parser.add_argument("--top", type=int, default=20, help="Show top N markets")
    parser.add_argument("--search", type=str, help="Search markets by keyword")
    parser.add_argument("--arb", action="store_true", help="Find arbitrage only")
    parser.add_argument("--detail", type=int, help="Show detail for market # from top list")
    parser.add_argument("--export", type=str, help="Export market to YAML (by condition_id)")
    parser.add_argument("--all", action="store_true", help="Scan more markets (slower)")
    args = parser.parse_args()

    max_markets = 1000 if args.all else 500
    max_events = 400 if args.all else 200

    print(f"\n  Scanning Polymarket ({max_markets} markets, {max_events} events)...")

    # Fetch data
    raw_markets = fetch_all_markets(max_markets)
    print(f"  Fetched {len(raw_markets)} markets")

    # Parse
    markets = [parse_market(m) for m in raw_markets]

    # Search mode
    if args.search:
        kw = args.search.lower()
        matches = [m for m in markets if kw in m.question.lower()]
        print(f"\n  Found {len(matches)} markets matching '{args.search}':\n")
        for i, m in enumerate(matches[:30], 1):
            print(f"  {i:3d}. YES={m.yes_price:.3f} | vol=${m.volume_24h:>10,.0f} | {m.question[:60]}")
            print(f"       condition_id: {m.condition_id}")
            print(f"       YES token:    {m.yes_token_id}")
            print(f"       NO  token:    {m.no_token_id}")
            print()
        return

    # Detail mode
    if args.detail:
        sorted_m = sorted(markets, key=lambda x: x.volume_24h, reverse=True)
        idx = args.detail - 1
        if 0 <= idx < len(sorted_m):
            print_market_detail(sorted_m[idx])
        else:
            print(f"  Invalid market number. Range: 1-{len(sorted_m)}")
        return

    # Export mode
    if args.export:
        match = [m for m in markets if m.condition_id == args.export]
        if match:
            print_market_detail(match[0])
        else:
            print(f"  Market with condition_id={args.export} not found")
        return

    # Top markets
    if not args.arb:
        top_list = print_top_markets(markets, args.top)

    # Arbitrage scan
    print(f"\n  Running arbitrage scan...")
    events = fetch_all_events(max_events)
    print(f"  Fetched {len(events)} events")

    binary = find_binary_arb(markets)
    temporal = find_temporal_arb(events)
    multi = find_multi_outcome_arb(events)
    spread = find_edge_opportunities(markets)

    print_arbitrage(binary, temporal, multi, spread)


if __name__ == "__main__":
    main()
