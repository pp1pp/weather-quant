import ReactECharts from 'echarts-for-react'
import type { ProbabilityData, MarketData } from '../api/client'
import dayjs from 'dayjs'

interface Props {
  prob: ProbabilityData | undefined
  market: MarketData | undefined
}

export default function ProbabilityChart({ prob, market }: Props) {
  if (!prob || prob.error || !market || market.error) {
    return (
      <div className="card">
        <div className="card-title">概率分布</div>
        <div className="loading">等待数据...</div>
      </div>
    )
  }

  const labels = market.buckets.map(b => b.label)
  const fairProbs = labels.map(l => +(prob.bucket_probs[l] ?? 0).toFixed(4))
  const marketPrices = market.buckets.map(b => +b.yes_price.toFixed(4))

  // Gaussian curve
  const mu = prob.weighted_mean_temp
  const sigma = prob.uncertainty_std
  const gaussianY = labels.map((_, i) => {
    const temp = i + 12
    const z = (temp - mu) / sigma
    return +(Math.exp(-0.5 * z * z) / (sigma * Math.sqrt(2 * Math.PI))).toFixed(4)
  })

  const option = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis' as const,
      backgroundColor: '#fff',
      borderColor: '#e2e8f0',
      textStyle: { color: '#1e293b', fontSize: 12 },
      extraCssText: 'box-shadow: 0 2px 12px rgba(0,0,0,0.08);',
      formatter: (params: any[]) => {
        const label = params[0]?.axisValue || ''
        let html = `<div style="font-weight:600;margin-bottom:4px">${label}</div>`
        for (const p of params) {
          if (p.seriesName === '高斯拟合') continue
          const val = (p.value * 100).toFixed(1)
          html += `<div>${p.marker} ${p.seriesName}: <b>${val}%</b></div>`
        }
        const fair = prob.bucket_probs[label] ?? 0
        const mkt = market.buckets.find(b => b.label === label)?.yes_price ?? 0
        const edge = fair - mkt
        if (Math.abs(edge) > 0.005) {
          const color = edge > 0 ? '#16a34a' : '#dc2626'
          const dir = edge > 0 ? '买入YES' : '买入NO'
          html += `<div style="color:${color};margin-top:4px;font-weight:500">Edge: ${(edge * 100).toFixed(1)}% ${dir}</div>`
        }
        return html
      },
    },
    legend: {
      data: ['公允概率', '市场价格'],
      textStyle: { color: '#64748b', fontSize: 12 },
      top: 0,
      itemWidth: 12,
      itemHeight: 8,
    },
    grid: { top: 36, bottom: 36, left: 48, right: 16 },
    xAxis: {
      type: 'category' as const,
      data: labels,
      axisLabel: { color: '#64748b', fontSize: 11, rotate: 35 },
      axisLine: { lineStyle: { color: '#e2e8f0' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value' as const,
      axisLabel: {
        color: '#94a3b8',
        fontSize: 11,
        formatter: (v: number) => `${(v * 100).toFixed(0)}%`,
      },
      splitLine: { lineStyle: { color: '#f1f5f9' } },
    },
    series: [
      {
        name: '公允概率',
        type: 'bar',
        data: fairProbs,
        itemStyle: { color: '#3b82f6', borderRadius: [4, 4, 0, 0] },
        barGap: '15%',
        barMaxWidth: 28,
      },
      {
        name: '市场价格',
        type: 'bar',
        data: marketPrices,
        itemStyle: { color: '#f97316', borderRadius: [4, 4, 0, 0] },
        barMaxWidth: 28,
      },
      {
        name: '高斯拟合',
        type: 'line',
        data: gaussianY,
        smooth: true,
        lineStyle: { color: '#a855f7', type: 'dashed' as const, width: 2 },
        symbol: 'none',
      },
    ],
  }

  return (
    <div className="card">
      <div className="card-title">
        <span>概率分布 vs 市场定价</span>
        <span className="card-title-extra">
          价格之和: {market.total_price_sum.toFixed(3)}
          {' | '}
          均值: {prob.weighted_mean_temp}°C
          {' | '}
          sigma: {prob.uncertainty_std}°C
          {market.fetched_at && (
            <>
              {' | '}
              盘口 {dayjs(market.fetched_at).format('HH:mm:ss')}
            </>
          )}
        </span>
      </div>
      <ReactECharts option={option} style={{ height: 320 }} />
    </div>
  )
}
