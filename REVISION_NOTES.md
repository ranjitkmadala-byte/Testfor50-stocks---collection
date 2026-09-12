# v3.6 revision — 09:20 same-strike option OI confirmation

- Locks/restores a per-contract option OI baseline immediately after the 09:20 Top-50 universe freeze.
- Keeps the existing frozen 3 OTM CE + 3 OTM PE basket unchanged for legacy scoring.
- For each frozen OTM strike, reads the opposite option type at the same strike and stores paired OI fields.
- Bullish: CE OI change vs baseline <= -20% and same-strike PE OI change > 0%.
- Bearish: PE OI change vs baseline <= -20% and same-strike CE OI change > 0%.
- Contribution: 0.5 confirmation +0.5 if opposite-side OI increase >=20% +0.5 after >=3 consecutive snapshots; cap 1.5.
- New columns are added idempotently to public.money_flow_option_snapshots.
- On same-day Railway restart, stored baseline_oi/baseline_ts are restored rather than overwritten.
