import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
})

export interface ForecastData {
  city: string
  event_date: string
  event_slug: string
  settlement_time: string
  model_forecasts: Record<string, number>
  weighted_mean: number
  bias_correction: number
  bias_corrected_mean: number
  hours_to_settlement: number
  fetched_at: string | null
  data_source?: string | null
  error?: string
}

export interface BucketData {
  label: string
  yes_price: number
  no_price: number
  spread: number
  volume: number
  liquidity: number
}

export interface MarketData {
  event_slug: string
  settlement_time: string
  buckets: BucketData[]
  total_price_sum: number
  data_source?: string | null
  fetched_at?: string | null
  error?: string
}

export interface EdgeData {
  label: string
  fair_prob: number
  market_price: number
  edge: number
  direction: string
  strength: string
  limit_price: number
  fill_prob: number
  risk_reward: number
  kelly: number
}

export interface SignalData {
  label: string
  direction: string
  edge: number
  amount: number
  fair_prob: number
  market_price: number
}

export interface WeatherFactors {
  cloud_cover: number
  max_wind: number
  wind_dir: number
  sea_breeze: boolean
  precipitation: number
  humidity: number
  pressure: number
  diurnal_range: number
}

export interface EnsembleStats {
  n_members: number
  mean: number
  std: number
  min: number
  max: number
  p10: number
  p90: number
}

export interface ProbabilityData {
  bucket_probs: Record<string, number>
  weighted_mean_temp: number
  uncertainty_std: number
  confidence: number
  model_forecasts: Record<string, number>
  hours_to_settlement: number
  timing_multiplier: number
  observation: number | null
  edges: EdgeData[]
  sum_to_one_gap: number
  signals: SignalData[]
  scenario_pnl: Record<string, number>
  weather_factors: WeatherFactors | null
  ensemble: EnsembleStats | null
  error?: string
}

export interface PositionData {
  id: number
  market_id: string
  label: string
  side: string
  amount: number
  entry_price: number
  shares: number
  entry_fair_prob: number | null
  status: string
  executed_at: string
  event_slug: string
}

export interface PositionsResponse {
  open_positions: PositionData[]
  total_exposure: number
  closed_trades: any[]
  selected_event_slug?: string | null
  selected_date?: string | null
  is_filtered?: boolean
}

export interface TimingCurvePoint {
  hours: number
  multiplier: number
}

export interface TimingData {
  curve: TimingCurvePoint[]
  current_hours: number | null
  current_multiplier: number | null
  config: {
    min_hours: number
    max_hours: number
    sweet_spot_low: number
    sweet_spot_high: number
  }
}

export interface BiasHistoryPoint {
  date: string
  actual: number
  forecast: number
  residual: number
  source: string
  is_reference: boolean
  notes?: string
}

export interface BiasData {
  current_bias: number
  computed_bias: number
  residual_std: number
  n_samples: number
  trusted_bias: number
  trusted_residual_std: number
  trusted_n_samples: number
  research_bias: number
  research_residual_std: number
  research_n_samples: number
  total_n_samples: number
  source_counts: Record<string, number>
  trusted_history_samples: number
  research_history_samples: number
  history: BiasHistoryPoint[]
}

export interface StatsData {
  cumulative: Record<string, any>
  settlements: any[]
  pnl_series: { date: string; pnl: number; cumulative_pnl: number }[]
  trade_counts: Record<string, number>
}

// Backtest types
export interface BacktestDailyEntry {
  date: string
  actual: number | null
  raw_forecast: number | null
  bias_corrected: number | null
  walk_forward_pred: number | null
  walk_forward_bias: number
  models: Record<string, number>
  source: string | null
  is_reference: boolean
  raw_error?: number
  corrected_error?: number
  wf_error?: number
}

export interface BacktestMetrics {
  bias: number
  mae: number
  rmse: number
  n: number
}

export interface BacktestData {
  daily: BacktestDailyEntry[]
  metrics: {
    raw: BacktestMetrics
    bias_corrected: BacktestMetrics
    walk_forward: BacktestMetrics
  }
  per_model: Record<string, BacktestMetrics>
  current_weights: Record<string, number>
  adaptive_weights: Record<string, number>
  current_bias: number
  suggested_bias: number | null
  bucket_hit_rate: number
  suggestions: string[]
  n_dates: number
  n_with_actual: number
  selected_date?: string | null
  selected_entry?: BacktestDailyEntry | null
}

export interface CityInfo {
  key: string
  name: string
  unit: string
}

export interface SystemData {
  mode: string
  mode_label: string
  mode_badge: string
  mode_description: string
  target_mode: string
  target_mode_label: string
  mode_source: string
  updated_at: string
  selected_date: string | null
  selected_city: string
  available_dates: string[]
  available_cities: CityInfo[]
  view_mode: string
  view_label: string
  weather_data_source?: string | null
  weather_data_label?: string | null
  weather_fetched_at?: string | null
  market_data_source?: string | null
  market_data_label?: string | null
  market_fetched_at?: string | null
  refresh_bypassed_cache?: boolean
}

export interface DashboardData {
  system: SystemData
  forecast: ForecastData
  market: MarketData
  probabilities: ProbabilityData
  positions: PositionsResponse
  timing: TimingData
  bias: BiasData
  stats: StatsData
  backtest: BacktestData
}

export const fetchDashboard = (date?: string, city?: string, refreshToken?: number) => {
  const params: Record<string, string | number> = {}
  if (date) params.date = date
  if (city) params.city = city
  if (refreshToken) params.refresh = refreshToken
  return api.get<DashboardData>('/dashboard', { params }).then(r => r.data)
}
export const fetchForecast = () => api.get<ForecastData>('/forecast').then(r => r.data)
export const fetchMarket = () => api.get<MarketData>('/market').then(r => r.data)
export const fetchProbabilities = () => api.get<ProbabilityData>('/probabilities').then(r => r.data)
export const fetchPositions = () => api.get<PositionsResponse>('/positions').then(r => r.data)
export const fetchTiming = () => api.get<TimingData>('/timing').then(r => r.data)
export const fetchBias = () => api.get<BiasData>('/bias').then(r => r.data)
export const fetchStats = () => api.get<StatsData>('/stats').then(r => r.data)
export const switchMode = (mode: 'LIVE' | 'DRY_RUN') =>
  api.post('/mode', { mode }).then(r => r.data)

export const applyAdaptiveWeights = () =>
  api.post('/config/apply-weights').then(r => r.data)

export interface CalibrationStatus {
  n_total: number
  n_reference: number
  source_counts: Record<string, number>
  current_bias: number
  all_mean_residual: number | null
  ref_mean_residual: number | null
  recent_dates: string[]
  recommendation: string
}

export interface CollectResult {
  n_collected: number
  n_skipped: number
  n_failed: number
  failed_dates: string[]
  new_bias: number | null
  message: string
  collected: Array<{ date: string; wu_temp: number; raw_forecast: number | null; residual: number | null }>
}

export const fetchCalibrationStatus = () =>
  api.get<CalibrationStatus>('/calibrate/status').then(r => r.data)

export const collectSettlements = (daysBack: number = 14, city?: string) => {
  const params: Record<string, string | number> = { days_back: daysBack }
  if (city) params.city = city
  return api.post<CollectResult>('/calibrate/collect-settlements', null, { params }).then(r => r.data)
}

export const addSettlement = (settleDate: string, wuTemp: number, notes?: string, city?: string) => {
  const params: Record<string, string | number> = { settle_date: settleDate, wu_temp: wuTemp, notes: notes || '' }
  if (city) params.city = city
  return api.post('/calibrate/add-settlement', null, { params }).then(r => r.data)
}

export const fetchCalibrationStatusForCity = (city?: string) => {
  const params: Record<string, string> = {}
  if (city) params.city = city
  return api.get<CalibrationStatus>('/calibrate/status', { params }).then(r => r.data)
}

export default api
