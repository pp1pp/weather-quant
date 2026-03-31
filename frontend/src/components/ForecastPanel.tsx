import type { ForecastData, ProbabilityData } from '../api/client'
import type { BacktestData } from '../api/client'

interface Props {
  forecast: ForecastData | undefined
  prob: ProbabilityData | undefined
  backtest?: BacktestData
}

export default function ForecastPanel({ forecast, prob, backtest }: Props) {
  if (!forecast || forecast.error) {
    return (
      <div className="card">
        <div className="card-title">气象预报</div>
        <div className="empty-state">{forecast?.error || '暂无数据'}</div>
      </div>
    )
  }

  const models = forecast.model_forecasts || {}
  const selectedEntry = backtest?.selected_entry

  return (
    <div className="card">
      <div className="card-title">
        气象预报
        <span style={{ float: 'right', fontSize: 12, color: '#64748b' }}>
          {forecast.data_source === 'db' ? 'DB快照' : '实时抓取'}
        </span>
      </div>

      <div style={{ textAlign: 'center', marginBottom: 16 }}>
        <div className="big-value value-highlight">
          {prob?.observation != null ? prob.weighted_mean_temp : forecast.bias_corrected_mean}°C
        </div>
        <div className="label">
          {prob?.observation != null ? '融合实测均值' : '校准后均值'}
        </div>
        {prob?.observation != null && (
          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2 }}>
            模型校准: {forecast.bias_corrected_mean}°C → 融合WU {prob.observation}°C
          </div>
        )}
      </div>

      <div className="stat-grid">
        {Object.entries(models).map(([model, temp]) => (
          <div className="stat-item" key={model}>
            <span className="label">{model.toUpperCase()}</span>
            <span className="value">{temp}°C</span>
          </div>
        ))}
        <div className="stat-item">
          <span className="label">原始均值</span>
          <span className="value">{forecast.weighted_mean}°C</span>
        </div>
        <div className="stat-item">
          <span className="label">偏差修正</span>
          <span className="value value-positive">+{forecast.bias_correction}°C</span>
        </div>
        {prob && (
          <>
            <div className="stat-item">
              <span className="label">不确定度 (sigma)</span>
              <span className="value">{prob.uncertainty_std}°C</span>
            </div>
            <div className="stat-item">
              <span className="label">模型一致性</span>
              <span className="value">{(prob.confidence * 100).toFixed(0)}%</span>
            </div>
          </>
        )}
        {prob?.observation != null && (
          <div className="stat-item">
            <span className="label">WU 实测</span>
            <span className="value" style={{ color: 'var(--accent-orange)' }}>{prob.observation}°C</span>
          </div>
        )}
        <div className="stat-item">
          <span className="label">距结算</span>
          <span className="value">{forecast.hours_to_settlement}h</span>
        </div>
        {backtest && backtest.bucket_hit_rate > 0 && (
          <div className="stat-item">
            <span className="label">落桶命中率</span>
            <span className="value" style={{
              color: backtest.bucket_hit_rate >= 0.5 ? 'var(--accent-green)' : 'var(--accent-orange)'
            }}>
              {(backtest.bucket_hit_rate * 100).toFixed(0)}%
            </span>
          </div>
        )}
      </div>

      {selectedEntry && (
        <div style={{
          marginTop: 12,
          padding: '10px 12px',
          borderRadius: 8,
          background: '#f8fafc',
          border: '1px solid #e2e8f0',
          fontSize: 12,
          color: '#475569',
        }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>
            当前选中日期回放: {selectedEntry.date}
          </div>
          <div>
            实际 {selectedEntry.actual ?? '--'}°C
            {' | '}
            原始预测 {selectedEntry.raw_forecast != null ? `${selectedEntry.raw_forecast.toFixed(1)}°C` : '--'}
            {' | '}
            修正后 {selectedEntry.bias_corrected != null ? `${selectedEntry.bias_corrected.toFixed(1)}°C` : '--'}
            {selectedEntry.raw_error != null && (
              <>
                {' | '}
                误差 {selectedEntry.raw_error > 0 ? '+' : ''}{selectedEntry.raw_error.toFixed(1)}°C
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
