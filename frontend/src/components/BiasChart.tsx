import ReactECharts from 'echarts-for-react'
import type { BiasData } from '../api/client'

interface Props {
  bias: BiasData | undefined
}

export default function BiasChart({ bias }: Props) {
  if (!bias) {
    return (
      <div className="card">
        <div className="card-title">偏差校准</div>
        <div className="loading">加载中...</div>
      </div>
    )
  }

  const history = bias.history || []
  const formatSigned = (value: number, digits = 2) =>
    `${value >= 0 ? '+' : ''}${value.toFixed(digits)}`
  const trustedBias = formatSigned(bias.trusted_bias)
  const appliedBias = formatSigned(bias.current_bias)
  const sourceSummary = Object.entries(bias.source_counts || {})
    .map(([source, count]) => `${source}: ${count}`)
    .join('  |  ')

  const option = history.length > 0 ? {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item' as const,
      backgroundColor: '#ffffff',
      borderColor: '#e2e8f0',
      textStyle: { color: '#334155', fontSize: 12 },
      formatter: (p: any) => {
        const d = history[p.dataIndex]
        return `<b>${d.date}</b><br/>
          实际: ${d.actual}°C<br/>
          预测: ${d.forecast}°C<br/>
          残差: ${d.residual > 0 ? '+' : ''}${d.residual}°C<br/>
          来源: ${d.source}<br/>
          可信: ${d.is_reference ? '是' : '否'}${d.notes ? `<br/>${d.notes}` : ''}`
      },
    },
    grid: { top: 30, bottom: 30, left: 50, right: 20 },
    xAxis: {
      type: 'category' as const,
      data: history.map(h => h.date),
      axisLabel: { color: '#64748b', fontSize: 10, rotate: 30 },
      axisLine: { lineStyle: { color: '#e2e8f0' } },
    },
    yAxis: {
      type: 'value' as const,
      name: '残差 (°C)',
      nameTextStyle: { color: '#64748b', fontSize: 11 },
      axisLabel: { color: '#64748b', fontSize: 11 },
      splitLine: { lineStyle: { color: '#f1f5f9' } },
    },
    series: [
      {
        type: 'scatter',
        data: history.map(h => h.residual),
        symbolSize: 10,
        itemStyle: {
          color: (params: any) => {
            const h = history[params.dataIndex]
            return h.is_reference ? '#3b82f6' : '#94a3b8'
          },
        },
        markLine: {
          silent: true,
          data: [
            {
              yAxis: bias.current_bias,
              lineStyle: { color: '#22c55e', type: 'dashed' as const, width: 2 },
              label: {
                formatter: `应用 ${appliedBias}°C`,
                color: '#22c55e',
                fontSize: 11,
              },
            },
            {
              yAxis: bias.trusted_bias,
              lineStyle: { color: '#3b82f6', type: 'dotted' as const, width: 2 },
              label: {
                formatter: `可信 ${trustedBias}°C`,
                color: '#3b82f6',
                fontSize: 11,
              },
            },
            {
              yAxis: 0,
              lineStyle: { color: '#e2e8f0', type: 'solid' as const, width: 1 },
              label: { show: false },
            },
          ],
        },
      },
    ],
  } : null

  return (
    <div className="card">
      <div className="card-title">
        偏差校准
        <span style={{ float: 'right', fontSize: 12, color: '#22c55e' }}>
          应用 {appliedBias}°C
        </span>
      </div>

      <div className="stat-grid" style={{ marginBottom: 12 }}>
        <div className="stat-item">
          <span className="label">当前偏差</span>
          <span className={`value ${bias.current_bias >= 0 ? 'value-positive' : 'value-negative'}`}>
            {appliedBias}°C
          </span>
        </div>
        <div className="stat-item">
          <span className="label">可信回放</span>
          <span className={`value ${bias.trusted_bias >= 0 ? 'value-positive' : 'value-negative'}`}>
            {trustedBias}°C
          </span>
        </div>
        <div className="stat-item">
          <span className="label">可信样本</span>
          <span className="value">{bias.trusted_n_samples}</span>
        </div>
        <div className="stat-item">
          <span className="label">研究样本</span>
          <span className="value">{bias.research_history_samples}</span>
        </div>
      </div>

      {option ? (
        <ReactECharts option={option} style={{ height: 200 }} />
      ) : (
        <div style={{ textAlign: 'center', color: '#94a3b8', padding: 20 }}>
          校准数据可用后将显示散点图
        </div>
      )}

      <div style={{ marginTop: 10, fontSize: 12, color: '#64748b' }}>
        可信 σ: {bias.trusted_residual_std.toFixed(2)}°C
        {sourceSummary ? `  |  ${sourceSummary}` : ''}
      </div>
    </div>
  )
}
