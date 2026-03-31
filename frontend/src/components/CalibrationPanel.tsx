import { useState } from 'react'
import { useCalibrationStatus, useCollectSettlements, useAddSettlement } from '../api/hooks'

export default function CalibrationPanel() {
  const { data: status } = useCalibrationStatus()
  const collectMutation = useCollectSettlements()
  const addMutation = useAddSettlement()

  const [collectDays, setCollectDays] = useState(14)
  const [collectMsg, setCollectMsg] = useState<string | null>(null)
  const [manualDate, setManualDate] = useState('')
  const [manualTemp, setManualTemp] = useState('')
  const [manualNotes, setManualNotes] = useState('')
  const [addMsg, setAddMsg] = useState<string | null>(null)

  const handleCollect = async () => {
    try {
      const result = await collectMutation.mutateAsync(collectDays)
      setCollectMsg(`✓ ${result.message}${result.new_bias ? ` 新偏差: ${result.new_bias.toFixed(3)}°C` : ''}`)
      setTimeout(() => setCollectMsg(null), 8000)
    } catch {
      setCollectMsg('请求失败')
    }
  }

  const handleAddManual = async () => {
    if (!manualDate || !manualTemp) return
    const temp = parseInt(manualTemp)
    if (isNaN(temp)) return
    try {
      await addMutation.mutateAsync({ settleDate: manualDate, wuTemp: temp, notes: manualNotes })
      setAddMsg(`✓ 已添加 ${manualDate}: ${temp}°C`)
      setManualDate('')
      setManualTemp('')
      setManualNotes('')
      setTimeout(() => setAddMsg(null), 5000)
    } catch {
      setAddMsg('添加失败')
    }
  }

  const qualityColor = !status ? '#64748b'
    : status.n_reference >= 20 ? '#16a34a'
    : status.n_reference >= 10 ? '#ca8a04'
    : '#dc2626'

  return (
    <div className="card">
      <div className="card-title">数据校准管理</div>

      {/* Status summary */}
      <div className="stat-grid" style={{ marginBottom: 12 }}>
        <div className="stat-item">
          <span className="label">总样本数</span>
          <span className="value">{status?.n_total ?? '—'}</span>
        </div>
        <div className="stat-item">
          <span className="label">参考样本</span>
          <span className="value" style={{ color: qualityColor }}>{status?.n_reference ?? '—'}</span>
        </div>
        <div className="stat-item">
          <span className="label">当前偏差</span>
          <span className="value">{status?.current_bias != null ? `${status.current_bias > 0 ? '+' : ''}${status.current_bias.toFixed(3)}°C` : '—'}</span>
        </div>
        <div className="stat-item">
          <span className="label">平均残差</span>
          <span className="value" style={{ color: status?.ref_mean_residual != null && Math.abs(status.ref_mean_residual) > 0.3 ? '#dc2626' : '#16a34a' }}>
            {status?.ref_mean_residual != null ? `${status.ref_mean_residual > 0 ? '+' : ''}${status.ref_mean_residual.toFixed(2)}°C` : '—'}
          </span>
        </div>
      </div>

      {/* Recommendation */}
      {status?.recommendation && (
        <div style={{ fontSize: 12, color: qualityColor, marginBottom: 10, padding: '4px 8px', background: '#f8fafc', borderRadius: 4 }}>
          {status.recommendation}
        </div>
      )}

      {/* Source breakdown */}
      {status?.source_counts && Object.keys(status.source_counts).length > 0 && (
        <div style={{ fontSize: 11, color: '#64748b', marginBottom: 10 }}>
          {Object.entries(status.source_counts).map(([src, n]) => (
            <span key={src} style={{ marginRight: 12 }}>{src}: {n}</span>
          ))}
        </div>
      )}

      {/* Auto-collect from WU */}
      <div style={{ marginBottom: 12, padding: '10px 12px', background: '#f0f9ff', borderRadius: 6, border: '1px solid #bae6fd' }}>
        <div style={{ fontWeight: 600, fontSize: 12, color: '#0369a1', marginBottom: 8 }}>自动收集历史结算数据</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <label style={{ fontSize: 12, color: '#334155' }}>回溯天数:</label>
          <select
            value={collectDays}
            onChange={e => setCollectDays(parseInt(e.target.value))}
            style={{ fontSize: 12, padding: '2px 4px', borderRadius: 4, border: '1px solid #cbd5e1' }}
          >
            {[7, 14, 21, 30].map(d => (
              <option key={d} value={d}>{d}天</option>
            ))}
          </select>
          <button
            onClick={handleCollect}
            disabled={collectMutation.isPending}
            style={{
              padding: '4px 12px', fontSize: 12, fontWeight: 600,
              borderRadius: 6, border: '1px solid #7dd3fc',
              background: '#e0f2fe', color: '#0369a1', cursor: 'pointer',
              opacity: collectMutation.isPending ? 0.6 : 1,
            }}
          >
            {collectMutation.isPending ? '收集中...' : '从WU收集'}
          </button>
        </div>
        {collectMsg && (
          <div style={{ marginTop: 6, fontSize: 11, color: '#0369a1' }}>{collectMsg}</div>
        )}
      </div>

      {/* Manual add */}
      <div style={{ padding: '10px 12px', background: '#fefce8', borderRadius: 6, border: '1px solid #fde68a' }}>
        <div style={{ fontWeight: 600, fontSize: 12, color: '#92400e', marginBottom: 8 }}>手动添加验证结算</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
          <input
            type="date"
            value={manualDate}
            onChange={e => setManualDate(e.target.value)}
            style={{ fontSize: 12, padding: '3px 6px', borderRadius: 4, border: '1px solid #fcd34d', width: 130 }}
          />
          <input
            type="number"
            placeholder="WU温度(°C)"
            value={manualTemp}
            onChange={e => setManualTemp(e.target.value)}
            style={{ fontSize: 12, padding: '3px 6px', borderRadius: 4, border: '1px solid #fcd34d', width: 100 }}
          />
          <input
            type="text"
            placeholder="备注(可选)"
            value={manualNotes}
            onChange={e => setManualNotes(e.target.value)}
            style={{ fontSize: 12, padding: '3px 6px', borderRadius: 4, border: '1px solid #fcd34d', flex: 1, minWidth: 120 }}
          />
          <button
            onClick={handleAddManual}
            disabled={!manualDate || !manualTemp || addMutation.isPending}
            style={{
              padding: '4px 12px', fontSize: 12, fontWeight: 600,
              borderRadius: 6, border: '1px solid #fcd34d',
              background: '#fef9c3', color: '#92400e', cursor: 'pointer',
              opacity: (!manualDate || !manualTemp || addMutation.isPending) ? 0.5 : 1,
            }}
          >
            添加
          </button>
        </div>
        {addMsg && (
          <div style={{ marginTop: 6, fontSize: 11, color: '#92400e' }}>{addMsg}</div>
        )}
      </div>
    </div>
  )
}
