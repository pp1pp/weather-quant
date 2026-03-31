from scipy.stats import norm

from src.data.schemas import EventSpec, FairProbResult, NormalizedForecast
from src.utils.logger import logger


class FairProbEngine:
    """L4: Estimate fair probability of weather event using multi-model CDF approach."""

    def __init__(self, config: dict):
        prob_cfg = config["probability"]
        self.uncertainty_std = prob_cfg["uncertainty_std"]
        self.obs_start_hours = prob_cfg["observation_influence_start_hours"]
        self.max_obs_weight = prob_cfg["max_observation_weight"]
        self.clamp_min = prob_cfg["prob_clamp_min"]
        self.clamp_max = prob_cfg["prob_clamp_max"]
        self.model_weights = config["model_weights"]

    def estimate(
        self, forecast: NormalizedForecast, event_spec: EventSpec
    ) -> FairProbResult:
        """Estimate fair probability from normalized forecast and event spec."""
        threshold = event_spec.threshold
        comparator = event_spec.comparator

        # Step 1 & 2: Per-model probabilities and weighted average
        model_probs = {}
        for model, temp in forecast.model_forecasts.items():
            p = self.temp_to_prob(temp, threshold, comparator, self.uncertainty_std)
            model_probs[model] = p

        base_prob = self._weighted_prob(model_probs)

        # Step 3: Observation correction
        obs_applied = False
        obs_weight = 0.0
        hours = forecast.hours_to_settlement

        if (
            forecast.latest_observation is not None
            and hours < self.obs_start_hours
        ):
            obs_weight = min(self.max_obs_weight, 1.0 - hours / 12.0)
            obs_weight = max(obs_weight, 0.0)
            obs_prob = self.temp_to_prob(
                forecast.latest_observation, threshold, comparator, self.uncertainty_std
            )
            base_prob = (1 - obs_weight) * base_prob + obs_weight * obs_prob
            obs_applied = True

        # Step 4: Clamp
        fair_prob = max(self.clamp_min, min(self.clamp_max, base_prob))

        # Confidence: 1 - std of model probs
        if len(model_probs) > 1:
            prob_values = list(model_probs.values())
            mean_p = sum(prob_values) / len(prob_values)
            variance = sum((p - mean_p) ** 2 for p in prob_values) / len(prob_values)
            confidence = 1.0 - variance ** 0.5
        else:
            confidence = 0.5

        breakdown = {
            "model_probs": model_probs,
            "base_prob": base_prob,
            "obs_correction_applied": obs_applied,
            "obs_weight": obs_weight,
            "final_prob": fair_prob,
        }

        logger.debug(
            f"FairProb: models={model_probs}, base={base_prob:.3f}, "
            f"obs_applied={obs_applied}, final={fair_prob:.3f}"
        )

        return FairProbResult(
            fair_prob=fair_prob,
            confidence=confidence,
            breakdown=breakdown,
        )

    def _weighted_prob(self, model_probs: dict[str, float]) -> float:
        """Weighted average of model probabilities, re-normalizing missing models."""
        total_weight = 0.0
        weighted_sum = 0.0

        for model, prob in model_probs.items():
            w = self.model_weights.get(model, 0)
            weighted_sum += w * prob
            total_weight += w

        if total_weight == 0:
            # Fallback: equal weight
            return sum(model_probs.values()) / len(model_probs)

        return weighted_sum / total_weight

    @staticmethod
    def temp_to_prob(
        temp: float, threshold: float, comparator: str, uncertainty_std: float
    ) -> float:
        """Convert a single model temperature to probability of exceeding threshold."""
        z = (threshold - temp) / uncertainty_std
        if comparator in [">", ">="]:
            return 1 - norm.cdf(z)
        else:  # "<", "<="
            return norm.cdf(z)
