import type { WeatherFactors, EnsembleStats } from '../api/client'

interface Props {
  factors: WeatherFactors | null | undefined
  ensemble: EnsembleStats | null | undefined
  modelForecasts?: Record<string, number>
}

function windDirLabel(deg: number): string {
  const dirs = ['北', '东北', '东', '东南', '南', '西南', '西', '西北']
  return dirs[Math.round(deg / 45) % 8]
}

export default function WeatherFactorsPanel({ factors, ensemble, modelForecasts }: Props) {
  // Compute model divergence
  const modelTemps = modelForecasts ? Object.values(modelForecasts) : []
  const modelSpread = modelTemps.length >= 2
    ? Math.max(...modelTemps) - Math.min(...modelTemps)
    : 0
  const divergenceLevel = modelSpread >= 3 ? 'high' : modelSpread >= 1.5 ? 'medium' : 'low'
  const divergenceColor = divergenceLevel === 'high' ? '#dc2626' : divergenceLevel === 'medium' ? '#ea580c' : '#16a34a'
  return (
    <div className="card">
      <div className="card-title">天气因子分析</div>

      {factors ? (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <div className="kpi-item">
            <span className="kpi-label">云量</span>
            <span className="kpi-value" style={{ color: factors.cloud_cover > 70 ? '#ea580c' : '#334155' }}>
              {factors.cloud_cover.toFixed(0)}%
            </span>
            <span className="kpi-label">{factors.cloud_cover > 70 ? '多云 ↓' : factors.cloud_cover < 30 ? '晴朗 ↑' : '部分多云'}</span>
          </div>

          <div className="kpi-item">
            <span className="kpi-label">风速/风向</span>
            <span className="kpi-value" style={{ color: factors.sea_breeze ? '#2563eb' : '#334155' }}>
              {factors.max_wind.toFixed(0)}km/h
            </span>
            <span className="kpi-label">
              {windDirLabel(factors.wind_dir)}风 {factors.sea_breeze ? '🌊海风' : ''}
            </span>
          </div>

          <div className="kpi-item">
            <span className="kpi-label">降水</span>
            <span className="kpi-value" style={{ color: factors.precipitation > 1 ? '#dc2626' : '#334155' }}>
              {factors.precipitation.toFixed(1)}mm
            </span>
            <span className="kpi-label">{factors.precipitation > 1 ? '有降水 ↓↓' : factors.precipitation > 0.1 ? '微量' : '无降水'}</span>
          </div>

          <div className="kpi-item">
            <span className="kpi-label">湿度</span>
            <span className="kpi-value">{factors.humidity.toFixed(0)}%</span>
            <span className="kpi-label">{factors.humidity < 50 ? '干燥 ↑' : factors.humidity > 80 ? '潮湿 ↓' : '适中'}</span>
          </div>

          <div className="kpi-item">
            <span className="kpi-label">气压</span>
            <span className="kpi-value">{factors.pressure.toFixed(0)}hPa</span>
            <span className="kpi-label">{factors.pressure > 1020 ? '高压 ↑' : factors.pressure < 1010 ? '低压 ↓' : '正常'}</span>
          </div>

          <div className="kpi-item">
            <span className="kpi-label">日温差</span>
            <span className="kpi-value">{factors.diurnal_range.toFixed(1)}°C</span>
            <span className="kpi-label">{factors.diurnal_range > 10 ? '大温差' : '小温差'}</span>
          </div>
        </div>
      ) : (
        <div className="empty-state">天气因子数据加载中...</div>
      )}

      {/* Model Divergence */}
      {modelForecasts && Object.keys(modelForecasts).length >= 2 && (
        <div style={{
          marginTop: 12, padding: '8px 12px',
          background: divergenceLevel === 'high' ? '#fef2f2' : divergenceLevel === 'medium' ? '#fffbeb' : '#f0fdf4',
          borderRadius: 6, borderLeft: `3px solid ${divergenceColor}`
        }}>
          <div style={{ fontWeight: 600, fontSize: 12, color: divergenceColor, marginBottom: 4 }}>
            模型分歧度: {modelSpread.toFixed(1)}°C
            {divergenceLevel === 'high' ? ' (严重分歧)' : divergenceLevel === 'medium' ? ' (中等)' : ' (一致)'}
          </div>
          <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#334155' }}>
            {Object.entries(modelForecasts).map(([model, temp]) => (
              <span key={model}>{model.toUpperCase()}: <b>{temp.toFixed(1)}°C</b></span>
            ))}
          </div>
          {divergenceLevel === 'high' && (
            <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2 }}>
              分歧超过3°C，已自动降低弱势模型权重
            </div>
          )}
        </div>
      )}

      {/* Ensemble Stats */}
      {ensemble && (
        <div style={{ marginTop: 12, padding: '8px 12px', background: '#f0f9ff', borderRadius: 6 }}>
          <div style={{ fontWeight: 600, fontSize: 12, color: '#1e40af', marginBottom: 4 }}>
            集合预报 ({ensemble.n_members}成员)
          </div>
          <div style={{ display: 'flex', gap: 16, fontSize: 12, color: '#334155' }}>
            <span>均值: <b>{ensemble.mean.toFixed(1)}°C</b></span>
            <span>σ: <b>{ensemble.std.toFixed(2)}°C</b></span>
            <span>范围: {ensemble.min.toFixed(0)}-{ensemble.max.toFixed(0)}°C</span>
          </div>
          <div style={{ display: 'flex', gap: 16, fontSize: 11, color: '#64748b', marginTop: 2 }}>
            <span>P10: {ensemble.p10.toFixed(1)}°C</span>
            <span>P90: {ensemble.p90.toFixed(1)}°C</span>
            <span>置信区间: ±{(ensemble.std * 1.645).toFixed(1)}°C (90%)</span>
          </div>
        </div>
      )}
    </div>
  )
}
