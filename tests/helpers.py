import pytest


_OPEN_METEO_OFFLINE_HINTS = (
    "nodename nor servname provided",
    "name or service not known",
    "temporary failure in name resolution",
    "failed to resolve",
    "network is unreachable",
    "connection refused",
    "timed out",
)


def skip_if_openmeteo_unavailable(exc: Exception) -> None:
    message = str(exc).lower()
    cause = getattr(exc, "__cause__", None)
    if cause is not None:
        message = f"{message} {str(cause).lower()}"

    if any(hint in message for hint in _OPEN_METEO_OFFLINE_HINTS):
        pytest.skip(f"Skipping online test because Open-Meteo is unavailable: {exc}")

    raise exc
