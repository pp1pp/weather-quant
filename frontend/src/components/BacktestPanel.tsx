import ReactECharts from 'echarts-for-react'
import { useState } from 'react'
import type { BacktestData } from '../api/client'
import { useApplyAdaptiveWeights } from '../api/hooks'

interface Props {
  backtest: BacktestData | undefined
}

export default function BacktestPanel({ backtest }: Props) {
  const applyWeightsMutation = useApplyAdaptiveWeights()
  const [applyMsg, setApplyMsg] = useState<string | null>(null)

  const handleApplyWeights = async () => {
    try {
      const result = await applyWeightsMutation.mutateAsync()
      if (result.success) {
        setApplyMsg(`已应用: ${Object.entries(result.applied).map(([k,v]) => `${k.toUpperCase()}=${((v as number)*100).toFixed(0)}%`).join(' ')}`)
        setTimeout(() => setApplyMsg(null), 5000)
      } else {
        setApplyMsg(result.error || '应用失败')
      }
    } catch {
      setApplyMsg('请求失败')
    }
  }

  if (!backtest) {
    return (
      <div className="card">
        <div className="card-title">回测验证</div>
        <div className="loading">加载中...</div>
      </div>
    )
  }

  const daily = (backtest.daily || []).filter(d => d.actual != null && d.raw_forecast != null)
  const metrics = backtest.metrics
  const perModel = backtest.per_model || {}
  const selectedEntry = backtest.selected_entry

  // Forecast vs Actual line chart
  const forecastChartOption = daily.length > 0 ? {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis' as const,
      backgroundColor: '#ffffff',
      borderColor: '#e2e8f0',
      textStyle: { color: '#334155', fontSize: 12 },
      formatter: (params: any) => {
        const idx = params[0]?.dataIndex
        const d = daily[idx]
        if (!d) return ''
        let html = `<b>${d.date}</b><br/>`
        html += `实际温度: ${d.actual}°C<br/>`
        html += `原始预测: ${d.raw_forecast?.toFixed(1)}°C<br/>`
        html += `偏差修正: ${d.bias_corrected?.toFixed(1)}°C<br/>`
        if (d.walk_forward_pred != null) {
          html += `走向前预测: ${d.walk_forward_pred.toFixed(1)}°C<br/>`
        }
        if (d.raw_error != null) {
          html += `预测误差: ${d.raw_error > 0 ? '+' : ''}${d.raw_error.toFixed(1)}°C`
        }
        return html
      },
    },
    legend: {
      data: ['实际温度', '原始预测', '偏差修正后', '走向前预测'],
      top: 0,
      textStyle: { color: '#64748b', fontSize: 11 },
    },
    grid: { top: 35, bottom: 30, left: 50, right: 20 },
    xAxis: {
      type: 'category' as const,
      data: daily.map(d => d.date.slice(5)),  // MM-DD
      axisLabel: { color: '#64748b', fontSize: 10, rotate: 30 },
      axisLine: { lineStyle: { color: '#e2e8f0' } },
    },
    yAxis: {
      type: 'value' as const,
      name: '温度 (°C)',
      nameTextStyle: { color: '#64748b', fontSize: 11 },
      axisLabel: { color: '#64748b', fontSize: 11 },
      splitLine: { lineStyle: { color: '#f1f5f9' } },
    },
    series: [
      {
        name: '实际温度',
        type: 'line',
        data: daily.map(d => d.actual),
        lineStyle: { width: 3, color: '#ef4444' },
        itemStyle: { color: '#ef4444' },
        symbolSize: 8,
        symbol: 'circle',
      },
      {
        name: '原始预测',
        type: 'line',
        data: daily.map(d => d.raw_forecast),
        lineStyle: { width: 2, color: '#94a3b8', type: 'dashed' as const },
        itemStyle: { color: '#94a3b8' },
        symbolSize: 5,
      },
      {
        name: '偏差修正后',
        type: 'line',
        data: daily.map(d => d.bias_corrected),
        lineStyle: { width: 2, color: '#3b82f6' },
        itemStyle: { color: '#3b82f6' },
        symbolSize: 5,
      },
      {
        name: '走向前预测',
        type: 'line',
        data: daily.map(d => d.walk_forward_pred),
        lineStyle: { width: 2, color: '#22c55e', type: 'dotted' as const },
        itemStyle: { color: '#22c55e' },
        symbolSize: 5,
      },
    ],
  } : null

  // Per-model MAE bar chart
  const modelNames = Object.keys(perModel).filter(m => perModel[m].n > 0)
  const modelChartOption = modelNames.length > 0 ? {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis' as const,
      backgroundColor: '#ffffff',
      borderColor: '#e2e8f0',
      textStyle: { color: '#334155', fontSize: 12 },
    },
    grid: { top: 20, bottom: 30, left: 50, right: 20 },
    xAxis: {
      type: 'category' as const,
      data: modelNames.map(m => m.toUpperCase()),
      axisLabel: { color: '#64748b', fontSize: 11 },
      axisLine: { lineStyle: { color: '#e2e8f0' } },
    },
    yAxis: {
      type: 'value' as const,
      name: 'MAE (°C)',
      nameTextStyle: { color: '#64748b', fontSize: 11 },
      axisLabel: { color: '#64748b', fontSize: 11 },
      splitLine: { lineStyle: { color: '#f1f5f9' } },
    },
    series: [
      {
        type: 'bar',
        data: modelNames.map(m => ({
          value: perModel[m].mae,
          itemStyle: {
            color: m === 'ecmwf' ? '#3b82f6' : m === 'gfs' ? '#22c55e' : '#f59e0b',
          },
        })),
        barWidth: 40,
        label: {
          show: true,
          position: 'top' as const,
          formatter: (p: any) => `${p.value.toFixed(2)}°C`,
          color: '#64748b',
          fontSize: 11,
        },
      },
    ],
  } : null

  // Weight comparison
  const currentW = backtest.current_weights || {}
  const adaptiveW = backtest.adaptive_weights || {}

  return (
    <div className="card">
      <div className="card-title">
        回测验证
        <span style={{ float: 'right', fontSize: 12, color: '#64748b' }}>
          {backtest.selected_date ? `选中 ${backtest.selected_date} | ` : ''}
          共 {backtest.n_dates} 天 / {backtest.n_with_actual} 有实际数据
        </span>
      </div>

      {selectedEntry && (
        <div style={{
          marginBottom: 12,
          padding: '10px 12px',
          borderRadius: 8,
          background: selectedEntry.is_reference ? '#eff6ff' : '#f8fafc',
          border: `1px solid ${selectedEntry.is_reference ? '#bfdbfe' : '#e2e8f0'}`,
          fontSize: 12,
          color: '#334155',
        }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>
            选中日期样本: {selectedEntry.date}
          </div>
          <div>
            实际 {selectedEntry.actual ?? '--'}°C
            {' | '}
            原始 {selectedEntry.raw_forecast != null ? `${selectedEntry.raw_forecast.toFixed(1)}°C` : '--'}
            {' | '}
            修正后 {selectedEntry.bias_corrected != null ? `${selectedEntry.bias_corrected.toFixed(1)}°C` : '--'}
            {' | '}
            来源 {selectedEntry.source || '--'}
            {' | '}
            {selectedEntry.is_reference ? '可信样本' : '研究样本'}
          </div>
        </div>
      )}

      {/* Metrics Summary */}
      <div className="stat-grid" style={{ marginBottom: 12 }}>
        <div className="stat-item">
          <span className="label">原始 MAE</span>
          <span className="value">{metrics.raw.mae.toFixed(2)}°C</span>
        </div>
        <div className="stat-item">
          <span className="label">修正后 MAE</span>
          <span className={`value ${metrics.bias_corrected.mae < metrics.raw.mae ? 'value-positive' : ''}`}>
            {metrics.bias_corrected.mae.toFixed(2)}°C
          </span>
        </div>
        <div className="stat-item">
          <span className="label">走向前 MAE</span>
          <span className="value">{metrics.walk_forward.mae.toFixed(2)}°C</span>
        </div>
        <div className="stat-item">
          <span className="label">原始偏差</span>
          <span className={`value ${metrics.raw.bias >= 0 ? 'value-negative' : 'value-positive'}`}>
            {metrics.raw.bias > 0 ? '+' : ''}{metrics.raw.bias.toFixed(2)}°C
          </span>
        </div>
      </div>

      {/* Charts Row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        {/* Forecast vs Actual */}
        <div>
          <div style={{ fontSize: 12, fontWeight: 600, color: '#475569', marginBottom: 4 }}>
            预测 vs 实际温度
          </div>
          {forecastChartOption ? (
            <ReactECharts option={forecastChartOption} style={{ height: 220 }} />
          ) : (
            <div style={{ textAlign: 'center', color: '#94a3b8', padding: 30 }}>暂无足够数据</div>
          )}
        </div>

        {/* Per-model MAE */}
        <div>
          <div style={{ fontSize: 12, fontWeight: 600, color: '#475569', marginBottom: 4 }}>
            各模型精度 (MAE)
          </div>
          {modelChartOption ? (
            <ReactECharts option={modelChartOption} style={{ height: 220 }} />
          ) : (
            <div style={{ textAlign: 'center', color: '#94a3b8', padding: 30 }}>暂无模型数据</div>
          )}
        </div>
      </div>

      {/* Weight Comparison */}
      {Object.keys(adaptiveW).length > 0 && (
        <div style={{ marginTop: 12, fontSize: 12, color: '#64748b' }}>
          <span style={{ fontWeight: 600, color: '#475569' }}>模型权重对比: </span>
          {['ecmwf', 'gfs', 'icon'].map(m => (
            <span key={m} style={{ marginRight: 16 }}>
              {m.toUpperCase()}: {((currentW[m] || 0) * 100).toFixed(0)}%
              {adaptiveW[m] !== undefined && adaptiveW[m] !== currentW[m] && (
                <span style={{ color: '#3b82f6' }}> → {((adaptiveW[m] || 0) * 100).toFixed(0)}%</span>
              )}
            </span>
          ))}
        </div>
      )}

      {/* RMSE + bucket hit */}
      <div style={{ marginTop: 8, fontSize: 12, color: '#64748b' }}>
        RMSE: 原始 {metrics.raw.rmse.toFixed(2)}°C |
        修正后 {metrics.bias_corrected.rmse.toFixed(2)}°C |
        走向前 {metrics.walk_forward.rmse.toFixed(2)}°C
        {backtest.bucket_hit_rate > 0 && (
          <span style={{ marginLeft: 12, color: backtest.bucket_hit_rate >= 0.6 ? '#16a34a' : '#ca8a04' }}>
            落桶命中率: {(backtest.bucket_hit_rate * 100).toFixed(0)}%
          </span>
        )}
      </div>

      {/* AI Suggestions */}
      {(backtest.suggestions?.length > 0 || backtest.suggested_bias !== null) && (
        <div style={{ marginTop: 12, padding: '10px 12px', background: '#f0fdf4', borderRadius: 8, border: '1px solid #bbf7d0' }}>
          <div style={{ fontWeight: 600, fontSize: 12, color: '#15803d', marginBottom: 6 }}>
            📊 校准建议
          </div>
          {backtest.suggestions?.map((s, i) => (
            <div key={i} style={{ fontSize: 12, color: '#334155', marginBottom: 3 }}>• {s}</div>
          ))}
          {backtest.suggested_bias !== null && Math.abs(backtest.suggested_bias - backtest.current_bias) > 0.05 && (
            <div style={{ fontSize: 12, color: '#475569', marginTop: 4 }}>
              走向前偏差建议: <b style={{ color: '#2563eb' }}>{backtest.suggested_bias > 0 ? '+' : ''}{backtest.suggested_bias.toFixed(3)}°C</b>
              {' '}(当前: {backtest.current_bias > 0 ? '+' : ''}{backtest.current_bias.toFixed(3)}°C)
            </div>
          )}
        </div>
      )}

      {/* Apply adaptive weights button */}
      {Object.keys(backtest.adaptive_weights || {}).length > 0 && (
        <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 10 }}>
          <button
            onClick={handleApplyWeights}
            disabled={applyWeightsMutation.isPending}
            style={{
              padding: '5px 14px', fontSize: 12, fontWeight: 600,
              borderRadius: 6, border: '1px solid #bfdbfe',
              background: '#eff6ff', color: '#2563eb', cursor: 'pointer',
              opacity: applyWeightsMutation.isPending ? 0.6 : 1,
            }}
          >
            {applyWeightsMutation.isPending ? '应用中...' : '应用自适应权重'}
          </button>
          {applyMsg && (
            <span style={{ fontSize: 12, color: '#16a34a' }}>{applyMsg}</span>
          )}
        </div>
      )}
    </div>
  )
}
