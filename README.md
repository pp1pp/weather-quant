# Weather Quant

Shanghai multi-outcome temperature trading system for Polymarket.

The codebase is now centered on the Shanghai daily highest-temperature bucket market:
- weather forecast ingestion from Open-Meteo / Wunderground
- fair-probability estimation for Shanghai temperature buckets
- edge detection, sizing, live/dry-run execution
- settlement review, replay validation, and bias calibration

## Quick Start

Install dependencies:

```bash
pip install -r requirements.txt
```

Create your environment file:

```bash
cp config/.env.example config/.env
```

Default mode is dry-run. Keep `LIVE_TRADING=false` until you have verified your setup.

## Main Entry Points

Recommended Shanghai workflow:

```bash
python3 main_shanghai.py --scan
python3 main_shanghai.py --analyze
python3 main_shanghai.py --once
python3 main_shanghai.py
```

Meaning:
- `--scan`: discover active Shanghai temperature markets
- `--analyze`: show forecast vs market without trading
- `--once`: run one trading cycle
- no flag: run the Shanghai loop continuously

General system entry point:

```bash
python3 main.py --once
python3 main.py --status
python3 main.py
```

## Backtest And Replay Validation

Reconcile the local Shanghai calibration samples:

```bash
python3 scripts/reconcile_shanghai_bias_samples.py
```

Validate local assumptions against verified Shanghai market outcomes:

```bash
python3 scripts/validate_shanghai_backtest.py
```

Important interpretation:
- `seed` rows are research-only warm-start samples
- `verified_market` rows are trusted settlement + forecast replay pairs
- live bias updates only use trusted reference samples

The default Shanghai bias in `config/settings.yaml` is intentionally neutral until at least 3 trusted replay samples exist.

## Live Trading

Live mode is controlled from `config/.env`:

```dotenv
LIVE_TRADING=true
PRIVATE_KEY=...
POLY_API_KEY=...
POLY_API_SECRET=...
POLY_API_PASSPHRASE=...
FUNDER=...
```

If credentials are missing or invalid, the system will fall back to dry-run behavior.

## Web Dashboard

API server / dashboard modules live under `src/web` and `frontend`.

The dashboard now distinguishes:
- applied bias from config
- trusted replay bias from verified samples
- research-only sample counts

## Project Layout

- `main.py`: full scheduler and system entry point
- `main_shanghai.py`: Shanghai-focused entry point
- `src/data`: forecast fetchers and normalization
- `src/engine`: probabilities, edge detection, calibration
- `src/trading`: execution, market scanning, sizing, risk control
- `src/review`: settlement review and PnL
- `src/web`: API + dashboard backend
- `scripts`: replay validation and calibration maintenance
- `tests`: offline unit tests

## Verification

Offline regression suite:

```bash
pytest -q tests/test_bias_calibrator.py tests/test_edge_detector.py tests/test_event_mapper.py tests/test_executor.py tests/test_fair_prob.py tests/test_normalizer.py tests/test_position_manager.py tests/test_risk_control.py tests/test_settlement_review.py tests/test_signal_generator.py
```

Network-dependent tests remain in:
- `tests/test_data_layer.py`
- `tests/test_integration.py`
