"""
Automatic Bias Calibration.

After each day's market settles, compares our Open-Meteo forecast vs the
actual settlement temperature. Maintains a rolling window of residuals
and updates the bias correction in settings.yaml.

The bias = mean(actual - raw_weighted_forecast) over the last N days.
We keep sample provenance in SQLite so seed assumptions never contaminate
live bias updates.
"""

from datetime import date, datetime, timezone

import yaml

from src.utils.db import get_connection
from src.utils.logger import logger

TRUSTED_SETTLEMENT_SOURCES = ("live_replay", "verified_market")


class BiasCalibrator:
    """Auto-calibrate Open-Meteo → WU bias from settlement history."""

    def __init__(self, config_path: str = "config/settings.yaml", window: int = 14):
        self.config_path = config_path
        self.window = window  # Rolling window days

    def record_settlement(
        self,
        city: str,
        settle_date: date,
        wu_temp: int,
        forecast_mean: float,
        *,
        source: str = "live_replay",
        settlement_ref: str | None = None,
        notes: str | None = None,
        is_reference: bool | None = None,
    ):
        """
        Record a settlement result for future bias calibration.

        Args:
            city: City key (e.g., "shanghai")
            settle_date: Settlement date
            wu_temp: WU reported temperature (whole °C, the settlement value)
            forecast_mean: Our raw weighted forecast mean (before bias correction)
            source: Sample provenance, e.g. "seed", "live_replay", "verified_market"
            settlement_ref: Optional URL or identifier for the settlement source
            notes: Optional notes about how the sample was derived
            is_reference: Whether this sample is trusted for live bias updates
        """
        if is_reference is None:
            is_reference = source in TRUSTED_SETTLEMENT_SOURCES

        conn = get_connection()
        try:
            conn.execute(
                """INSERT INTO bias_calibration
                   (city, settle_date, wu_temp, forecast_mean, residual, recorded_at,
                    source, settlement_ref, notes, is_reference)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(city, settle_date, source) DO UPDATE SET
                       wu_temp = excluded.wu_temp,
                       forecast_mean = excluded.forecast_mean,
                       residual = excluded.residual,
                       recorded_at = excluded.recorded_at,
                       settlement_ref = excluded.settlement_ref,
                       notes = excluded.notes,
                       is_reference = excluded.is_reference""",
                (
                    city,
                    settle_date.isoformat(),
                    wu_temp,
                    round(forecast_mean, 2),
                    round(wu_temp - forecast_mean, 2),
                    datetime.now(timezone.utc).isoformat(),
                    source,
                    settlement_ref,
                    notes,
                    int(is_reference),
                ),
            )
            conn.commit()
            logger.info(
                f"Recorded settlement: {city}/{settle_date} source={source} "
                f"actual={wu_temp}°C forecast={forecast_mean:.1f}°C "
                f"residual={wu_temp - forecast_mean:+.1f}°C "
                f"reference={bool(is_reference)}"
            )
        finally:
            conn.close()

    def compute_bias(
        self,
        city: str,
        *,
        reference_only: bool = False,
        sources: tuple[str, ...] | list[str] | None = None,
    ) -> dict:
        """
        Compute current bias correction from recent settlements.

        Returns dict with:
          - bias: mean residual (add to forecast)
          - std: residual standard deviation
          - n: number of data points
          - samples: list of recent residuals
        """
        if sources is not None and len(sources) == 0:
            return {"bias": 0.0, "std": 1.5, "n": 0, "samples": []}

        clauses = ["city = ?"]
        params: list[object] = [city]

        if reference_only:
            clauses.append("is_reference = 1")
        if sources is not None:
            placeholders = ", ".join("?" for _ in sources)
            clauses.append(f"source IN ({placeholders})")
            params.extend(sources)

        conn = get_connection()
        try:
            cursor = conn.execute(
                f"""SELECT residual, settle_date, source, is_reference
                   FROM bias_calibration
                   WHERE {" AND ".join(clauses)}
                   ORDER BY settle_date DESC
                   LIMIT ?""",
                (*params, self.window),
            )
            rows = cursor.fetchall()
        finally:
            conn.close()

        if not rows:
            return {"bias": 0.0, "std": 1.5, "n": 0, "samples": []}

        residuals = [r["residual"] for r in rows]
        n = len(residuals)
        mean_bias = sum(residuals) / n
        variance = sum((r - mean_bias) ** 2 for r in residuals) / n if n > 1 else 2.25
        std = variance ** 0.5

        filter_label = "reference_only" if reference_only else "all_sources"
        if sources is not None:
            filter_label = f"{filter_label}:{','.join(sources)}"
        logger.info(f"Bias for {city} [{filter_label}]: {mean_bias:+.2f}°C (std={std:.2f}, n={n})")

        return {
            "bias": round(mean_bias, 2),
            "std": round(std, 2),
            "n": n,
            "samples": residuals,
        }

    def update_config(
        self,
        city: str,
        *,
        min_samples: int = 3,
        reference_only: bool = True,
        sources: tuple[str, ...] | list[str] | None = None,
    ) -> bool:
        """
        Recompute bias and update settings.yaml with new value.

        Only updates if we have at least 3 data points.
        """
        result = self.compute_bias(city, reference_only=reference_only, sources=sources)

        if result["n"] < min_samples:
            source_label = "trusted reference" if reference_only else "selected"
            logger.info(
                f"Not enough {source_label} samples for {city} bias update "
                f"(have {result['n']}, need {min_samples})"
            )
            return False

        # Read current config
        with open(self.config_path, "r") as f:
            cfg = yaml.safe_load(f)

        mo_cfg = cfg.setdefault("multi_outcome", {})
        bc = mo_cfg.setdefault("bias_correction", {})

        old_bias = bc.get(city, 0.0)
        new_bias = result["bias"]

        # Guard: cap bias at ±2°C. If residuals are larger, they likely reflect
        # long-range forecast errors (fetched 5+ days before event), not systematic bias.
        # The actual day-of forecast bias is typically 0.5-1.5°C.
        MAX_BIAS = 2.0
        if abs(new_bias) > MAX_BIAS:
            logger.warning(
                f"Computed bias for {city} exceeds ±{MAX_BIAS}°C limit "
                f"({new_bias:+.2f}°C). Capping to {MAX_BIAS * (1 if new_bias > 0 else -1):.2f}°C. "
                "This may indicate use of long-range forecasts (>3 days ahead) in residuals."
            )
            new_bias = MAX_BIAS if new_bias > 0 else -MAX_BIAS

        if abs(old_bias - new_bias) < 0.05:
            logger.info(
                f"Bias for {city} unchanged: {old_bias:+.2f} → {new_bias:+.2f} (diff < 0.05)"
            )
            return False

        bc[city] = new_bias
        # NOTE: Do NOT update data_source_bias_std here — that is a separately
        # calibrated parameter representing the WU vs Open-Meteo grid uncertainty.
        # Auto-updating it with rolling residual std causes instability when
        # long-range forecasts (5+ days ahead) are included in the residual window.

        with open(self.config_path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        logger.info(
            f"Updated bias for {city}: {old_bias:+.2f} → {new_bias:+.2f}°C "
            f"(residual std={result['std']:.2f}, n={result['n']})"
        )
        return True


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'index') AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_names(conn, table_name: str) -> set[str]:
    return {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})")
    }


def _create_calibration_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bias_calibration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            settle_date TEXT NOT NULL,
            wu_temp REAL NOT NULL,
            forecast_mean REAL NOT NULL,
            residual REAL NOT NULL,
            recorded_at TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'legacy',
            settlement_ref TEXT,
            notes TEXT,
            is_reference INTEGER NOT NULL DEFAULT 0,
            UNIQUE(city, settle_date, source)
        )
    """)
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_bias_calibration_city_ref_date
           ON bias_calibration(city, is_reference, settle_date DESC)"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_bias_calibration_city_source_date
           ON bias_calibration(city, source, settle_date DESC)"""
    )


def ensure_calibration_table():
    """Create or migrate the bias_calibration table with provenance metadata."""
    conn = get_connection()
    try:
        if not _table_exists(conn, "bias_calibration"):
            _create_calibration_table(conn)
            conn.commit()
            return

        columns = _column_names(conn, "bias_calibration")
        needs_rebuild = "id" not in columns or "source" not in columns

        if needs_rebuild:
            legacy_base = (
                "bias_calibration_legacy_"
                + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            )
            legacy_table = legacy_base
            suffix = 1
            while _table_exists(conn, legacy_table):
                legacy_table = f"{legacy_base}_{suffix}"
                suffix += 1
            conn.execute(f"ALTER TABLE bias_calibration RENAME TO {legacy_table}")
            _create_calibration_table(conn)
            conn.execute(
                f"""INSERT INTO bias_calibration
                    (city, settle_date, wu_temp, forecast_mean, residual, recorded_at,
                     source, settlement_ref, notes, is_reference)
                    SELECT city, settle_date, wu_temp, forecast_mean, residual, recorded_at,
                           'legacy', NULL, 'Migrated from legacy bias_calibration schema', 0
                    FROM {legacy_table}"""
            )
            logger.info(
                f"Migrated bias_calibration to provenance-aware schema "
                f"(legacy copy: {legacy_table})"
            )
        else:
            optional_columns = {
                "settlement_ref": "TEXT",
                "notes": "TEXT",
                "is_reference": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, kind in optional_columns.items():
                if name not in columns:
                    conn.execute(f"ALTER TABLE bias_calibration ADD COLUMN {name} {kind}")
            _create_calibration_table(conn)

        conn.commit()
    finally:
        conn.close()
