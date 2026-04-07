import type { EdgeData, SignalData } from '../api/client'

interface Props {
  edges: EdgeData[] | undefined
  signals?: SignalData[]
  scenarioPnl?: Record<string, number>
  sumToOneGap?: number
}

export default function EdgeTable({ edges, signals, scenarioPnl, sumToOneGap }: Props) {
  if (!edges || edges.length === 0) {
    return (
      <div className="card">
        <div className="card-title">交易信号</div>
        <div className="empty-state">暂无信号</div>
      </div>
    )
  }

  const sorted = [...edges].sort((a, b) => Math.abs(b.edge) - Math.abs(a.edge))
  const maxAbsEdge = Math.max(...sorted.map(e => Math.abs(e.edge)), 0.01)

  // Check if a signal exists for a bucket
  const signalMap = new Map<string, SignalData>()
  if (signals) {
    for (const s of signals) signalMap.set(s.label, s)
  }

  return (
    <div className="card">
      <div className="card-title" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>交易信号</span>
        {sumToOneGap !== undefined && Math.abs(sumToOneGap) > 0.01 && (
          <span style={{
            fontSize: 11,
            padding: '2px 6px',
            borderRadius: 4,
            background: sumToOneGap > 0.03 ? '#fef2f2' : sumToOneGap < -0.03 ? '#f0fdf4' : '#f8fafc',
            color: sumToOneGap > 0.03 ? '#dc2626' : sumToOneGap < -0.03 ? '#16a34a' : '#64748b',
          }}>
            价格和: {(1 + sumToOneGap).toFixed(3)} ({sumToOneGap > 0 ? '高估' : '低估'})
          </span>
        )}
      </div>

      <div className="data-table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              <th>温度</th>
              <th>方向</th>
              <th style={{ textAlign: 'right' }}>Edge</th>
              <th>挂单</th>
              <th>盈亏比</th>
              <th>下注</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(e => {
              const absEdge = Math.abs(e.edge)
              const barWidth = Math.min(100, (absEdge / maxAbsEdge) * 100)
              const isActionable = e.strength !== 'NONE'
              const signal = signalMap.get(e.label)

              return (
                <tr key={e.label} style={{ opacity: isActionable ? 1 : 0.45 }}>
                  <td style={{ fontWeight: 600, fontSize: 12 }}>{e.label}</td>
                  <td>
                    {isActionable ? (
                      <span className={`badge ${e.direction === 'BUY_YES' ? 'badge-buy-yes' : 'badge-buy-no'}`}>
                        {e.direction === 'BUY_YES' ? 'YES' : 'NO'}
                      </span>
                    ) : (
                      <span style={{ color: '#94a3b8', fontSize: 11 }}>--</span>
                    )}
                  </td>
                  <td style={{ textAlign: 'right' }}>
                    <div className="edge-bar-wrap" style={{ justifyContent: 'flex-end' }}>
                      <span className={absEdge >= 0.05 ? (e.edge > 0 ? 'value-positive' : 'value-negative') : ''}
                            style={{ fontSize: 12, fontWeight: absEdge >= 0.10 ? 700 : 400 }}>
                        {e.edge > 0 ? '+' : ''}{(e.edge * 100).toFixed(1)}%
                      </span>
                      <div className={`edge-bar ${e.edge > 0 ? 'edge-bar-pos' : 'edge-bar-neg'}`}
                           style={{ width: `${barWidth}%`, maxWidth: 40 }} />
                    </div>
                  </td>
                  <td style={{ fontSize: 11, color: '#475569', fontFamily: 'monospace' }}>
                    {isActionable && e.limit_price > 0 ? (
                      <span title={`成交概率: ${(e.fill_prob * 100).toFixed(0)}%`}>
                        ¢{(e.limit_price * 100).toFixed(1)}
                      </span>
                    ) : '--'}
                  </td>
                  <td style={{ fontSize: 11, fontFamily: 'monospace' }}>
                    {isActionable && e.risk_reward > 0 ? (
                      <span style={{ color: e.risk_reward >= 3 ? '#16a34a' : e.risk_reward >= 1.5 ? '#ca8a04' : '#64748b' }}>
                        {e.risk_reward.toFixed(1)}:1
                      </span>
                    ) : '--'}
                  </td>
                  <td style={{ fontSize: 11, fontWeight: 600 }}>
                    {signal ? (
                      <span style={{ color: '#2563eb' }}>${signal.amount.toFixed(0)}</span>
                    ) : '--'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Scenario P&L */}
      {scenarioPnl && Object.keys(scenarioPnl).length > 0 && signals && signals.length > 0 && (
        <div style={{ marginTop: 12, padding: '8px 12px', background: '#f8fafc', borderRadius: 6, fontSize: 11 }}>
          <div style={{ fontWeight: 600, marginBottom: 4, color: '#334155' }}>场景盈亏分析</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 12px' }}>
            {Object.entries(scenarioPnl).map(([label, pnl]) => (
              <span key={label} style={{ color: pnl >= 0 ? '#16a34a' : '#dc2626' }}>
                {label}: {pnl >= 0 ? '+' : ''}{pnl.toFixed(1)}
              </span>
            ))}
          </div>
          <div style={{ marginTop: 4, color: '#64748b' }}>
            最大亏损: ${Math.min(...Object.values(scenarioPnl)).toFixed(1)} |
            最大盈利: +${Math.max(...Object.values(scenarioPnl)).toFixed(1)} |
            总投入: ${signals.reduce((s, sig) => s + sig.amount, 0).toFixed(1)}
          </div>
        </div>
      )}
    </div>
  )
}
