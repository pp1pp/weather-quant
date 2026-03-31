import ReactECharts from 'echarts-for-react'
import type { TimingData } from '../api/client'

interface Props {
  timing: TimingData | undefined
}

export default function TimingChart({ timing }: Props) {
  if (!timing) {
    return (
      <div className="card">
        <div className="card-title">入场时机</div>
        <div className="loading">加载中...</div>
      </div>
    )
  }

  const hours = timing.curve.map(p => p.hours)
  const mults = timing.curve.map(p => p.multiplier)
  const cfg = timing.config

  const option = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis' as const,
      backgroundColor: '#fff',
      borderColor: '#e2e8f0',
      textStyle: { color: '#1e293b', fontSize: 12 },
      extraCssText: 'box-shadow: 0 2px 12px rgba(0,0,0,0.08);',
      formatter: (params: any[]) => {
        const h = params[0].axisValue
        const m = params[0].value
        return `距结算 <b>${h}h</b><br/>仓位乘数: <b>${m}</b>`
      },
    },
    grid: { top: 24, bottom: 34, left: 46, right: 16 },
    xAxis: {
      type: 'category' as const,
      data: hours,
      name: '距结算 (小时)',
      nameLocation: 'center' as const,
      nameGap: 22,
      nameTextStyle: { color: '#94a3b8', fontSize: 11 },
      axisLabel: { color: '#94a3b8', fontSize: 10, interval: 19 },
      axisLine: { lineStyle: { color: '#e2e8f0' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value' as const,
      min: 0,
      max: 1.1,
      axisLabel: { color: '#94a3b8', fontSize: 11 },
      splitLine: { lineStyle: { color: '#f1f5f9' } },
    },
    series: [
      {
        type: 'line',
        data: mults,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#2563eb', width: 2 },
        areaStyle: {
          color: {
            type: 'linear' as const,
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(37,99,235,0.12)' },
              { offset: 1, color: 'rgba(37,99,235,0.01)' },
            ],
          },
        },
        markArea: {
          silent: true,
          data: [
            [
              { xAxis: cfg.sweet_spot_low, itemStyle: { color: 'rgba(22,163,74,0.06)' } },
              { xAxis: cfg.sweet_spot_high },
            ],
          ],
        },
        markLine: timing.current_hours != null ? {
          silent: true,
          data: [
            {
              xAxis: timing.current_hours,
              lineStyle: { color: '#ea580c', type: 'solid' as const, width: 2 },
              label: {
                formatter: `当前 ${timing.current_hours?.toFixed(1)}h`,
                color: '#ea580c',
                fontSize: 11,
                fontWeight: 600,
              },
            },
          ],
        } : undefined,
      },
    ],
  }

  return (
    <div className="card">
      <div className="card-title">
        <span>入场时机策略</span>
        <span className="card-title-extra">
          {timing.current_multiplier != null && (
            <span style={{
              color: timing.current_multiplier > 0.5 ? 'var(--accent-green)' : 'var(--accent-orange)',
              fontWeight: 600,
            }}>
              当前乘数: {timing.current_multiplier}x
            </span>
          )}
        </span>
      </div>
      <ReactECharts option={option} style={{ height: 260 }} />
    </div>
  )
}
