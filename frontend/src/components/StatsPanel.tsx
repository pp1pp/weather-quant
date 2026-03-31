import ReactECharts from 'echarts-for-react'
import type { StatsData } from '../api/client'

interface Props {
  stats: StatsData | undefined
}

export default function StatsPanel({ stats }: Props) {
  const series = stats?.pnl_series || []
  const counts = stats?.trade_counts || {}
  const totalTrades = Object.values(counts).reduce((a: number, b: number) => a + b, 0)
  const cumulative = stats?.cumulative || {}

  const option = series.length > 0 ? {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis' as const,
      backgroundColor: '#fff',
      borderColor: '#e2e8f0',
      textStyle: { color: '#1e293b', fontSize: 12 },
      extraCssText: 'box-shadow: 0 2px 12px rgba(0,0,0,0.08);',
    },
    grid: { top: 16, bottom: 28, left: 48, right: 16 },
    xAxis: {
      type: 'category' as const,
      data: series.map((s: any) => s.date),
      axisLabel: { color: '#94a3b8', fontSize: 10, rotate: 30 },
      axisLine: { lineStyle: { color: '#e2e8f0' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value' as const,
      axisLabel: { color: '#94a3b8', fontSize: 11, formatter: (v: number) => `$${v}` },
      splitLine: { lineStyle: { color: '#f1f5f9' } },
    },
    series: [
      {
        type: 'line',
        data: series.map((s: any) => s.cumulative_pnl),
        smooth: true,
        symbol: 'circle',
        symbolSize: 5,
        lineStyle: { color: '#16a34a', width: 2 },
        itemStyle: { color: '#16a34a' },
        areaStyle: {
          color: {
            type: 'linear' as const,
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(22,163,74,0.1)' },
              { offset: 1, color: 'rgba(22,163,74,0.01)' },
            ],
          },
        },
      },
    ],
  } : null

  return (
    <div className="card">
      <div className="card-title">交易绩效</div>

      <div className="stat-grid" style={{ marginBottom: 12 }}>
        <div className="stat-item">
          <span className="label">总交易数</span>
          <span className="value">{totalTrades}</span>
        </div>
        <div className="stat-item">
          <span className="label">胜率</span>
          <span className="value">{cumulative.win_rate ? `${(cumulative.win_rate * 100).toFixed(0)}%` : '--'}</span>
        </div>
        <div className="stat-item">
          <span className="label">累计盈亏</span>
          <span className={`value ${(cumulative.total_pnl ?? 0) >= 0 ? 'value-positive' : 'value-negative'}`}>
            {(cumulative.total_pnl ?? 0) >= 0 ? '+' : ''}${(cumulative.total_pnl ?? 0).toFixed(2)}
          </span>
        </div>
        <div className="stat-item">
          <span className="label">交易状态</span>
          <span className="value" style={{ fontSize: 12 }}>
            {Object.entries(counts).map(([k, v]) => {
              const label = k === 'DRY_RUN' ? '模拟' : k === 'SUBMITTED' ? '已提交' : k === 'CLOSED' ? '已平' : k
              return `${label}: ${v}`
            }).join(', ') || '--'}
          </span>
        </div>
      </div>

      {option ? (
        <ReactECharts option={option} style={{ height: 200 }} />
      ) : (
        <div className="empty-state">
          结算后将显示累计收益曲线
        </div>
      )}
    </div>
  )
}
