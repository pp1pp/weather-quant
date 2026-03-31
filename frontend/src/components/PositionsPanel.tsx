import type { PositionsResponse } from '../api/client'

interface Props {
  positions: PositionsResponse | undefined
}

export default function PositionsPanel({ positions }: Props) {
  const open = positions?.open_positions ?? []
  const closed = positions?.closed_trades ?? []
  const isFiltered = positions?.is_filtered
  const selectedDate = positions?.selected_date

  return (
    <div className="card">
      <div className="card-title">
        <span>持仓管理</span>
        <span className="card-title-extra">
          {isFiltered && selectedDate ? `日期 ${selectedDate} | ` : ''}
          总敞口: ${positions?.total_exposure?.toFixed(2) ?? '0.00'}
        </span>
      </div>

      {open.length === 0 && closed.length === 0 ? (
        <div className="empty-state">{isFiltered ? '该市场日期暂无相关持仓' : '暂无持仓'}</div>
      ) : (
        <>
          {open.length > 0 && (
            <table className="data-table">
              <thead>
                <tr>
                  <th>温度区间</th>
                  <th>方向</th>
                  <th style={{ textAlign: 'right' }}>金额</th>
                  <th style={{ textAlign: 'right' }}>入场价</th>
                  <th style={{ textAlign: 'right' }}>份额</th>
                  <th>状态</th>
                </tr>
              </thead>
              <tbody>
                {open.map(p => (
                  <tr key={p.id}>
                    <td style={{ fontWeight: 600 }}>{p.label || p.market_id.slice(-12)}</td>
                    <td>
                      <span className={`badge ${p.side === 'YES' ? 'badge-buy-yes' : 'badge-buy-no'}`}>
                        {p.side}
                      </span>
                    </td>
                    <td style={{ textAlign: 'right', fontWeight: 500 }}>${p.amount.toFixed(2)}</td>
                    <td style={{ textAlign: 'right' }}>{(p.entry_price * 100).toFixed(1)}%</td>
                    <td style={{ textAlign: 'right' }}>{p.shares.toFixed(1)}</td>
                    <td>
                      <span className={`badge ${p.status === 'DRY_RUN' ? 'badge-dry' : p.status === 'SUBMITTED' ? 'badge-strong' : 'badge-live'}`}>
                        {p.status === 'DRY_RUN' ? '模拟' : p.status === 'SUBMITTED' ? '已提交' : p.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          {closed.length > 0 && (
            <>
              <div style={{ marginTop: 16, marginBottom: 8, fontSize: 12, color: 'var(--text-muted)' }}>
                近期已平仓
              </div>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>温度区间</th>
                    <th>方向</th>
                    <th style={{ textAlign: 'right' }}>金额</th>
                    <th style={{ textAlign: 'right' }}>盈亏</th>
                  </tr>
                </thead>
                <tbody>
                  {closed.slice(0, 5).map((t: any) => (
                    <tr key={t.id}>
                      <td>{t.label}</td>
                      <td>{t.side}</td>
                      <td style={{ textAlign: 'right' }}>${t.amount?.toFixed(2)}</td>
                      <td style={{ textAlign: 'right' }} className={t.pnl >= 0 ? 'value-positive' : 'value-negative'}>
                        {t.pnl >= 0 ? '+' : ''}${t.pnl?.toFixed(2)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </>
      )}
    </div>
  )
}
