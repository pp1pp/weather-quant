import { useDashboard, useSwitchMode } from '../api/hooks'
import ForecastPanel from './ForecastPanel'
import ProbabilityChart from './ProbabilityChart'
import EdgeTable from './EdgeTable'
import PositionsPanel from './PositionsPanel'
import TimingChart from './TimingChart'
import StatsPanel from './StatsPanel'
import BiasChart from './BiasChart'
import BacktestPanel from './BacktestPanel'
import WeatherFactorsPanel from './WeatherFactorsPanel'
import CalibrationPanel from './CalibrationPanel'
import dayjs from 'dayjs'
import { useEffect, useState } from 'react'

function Countdown({ target }: { target: string }) {
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [])
  const diff = Math.max(0, new Date(target).getTime() - now)
  const h = Math.floor(diff / 3600000)
  const m = Math.floor((diff % 3600000) / 60000)
  const s = Math.floor((diff % 60000) / 1000)
  if (diff <= 0) return <span className="countdown">已结算</span>
  return <span className="countdown">{h}:{String(m).padStart(2,'0')}:{String(s).padStart(2,'0')}</span>
}

const CITY_EMOJI: Record<string, string> = {
  shanghai: '🇨🇳',
  chicago: '🇺🇸',
  miami: '🇺🇸',
  los_angeles: '🇺🇸',
  london: '🇬🇧',
  tokyo: '🇯🇵',
}

export default function Dashboard() {
  const [selectedDate, setSelectedDate] = useState<string | undefined>(undefined)
  const [selectedCity, setSelectedCity] = useState<string>('shanghai')
  const {
    data,
    isLoading,
    error,
    isFetching,
    manualRefresh,
    isManualRefreshing,
  } = useDashboard(selectedDate, selectedCity)
  const modeMutation = useSwitchMode()

  if (isLoading) return <div className="loading">加载中...</div>
  if (error) return <div className="error">API 连接失败: {String(error)}</div>
  if (!data) return <div className="error">暂无数据</div>

  const forecast = data.forecast
  const market = data.market
  const prob = data.probabilities
  const positions = data.positions
  const timing = data.timing
  const availableDates = data.system.available_dates || []
  const availableCities = data.system.available_cities || []

  // Find current city info
  const currentCityInfo = availableCities.find(c => c.key === selectedCity)
  const cityName = currentCityInfo?.name || selectedCity

  const displayDate = data.system.selected_date || forecast?.event_date || ''
  const eventLabel = displayDate
    ? `${cityName}最高气温 ${displayDate}`
    : (forecast?.event_slug || '').replace(/-/g, ' ')

  const isLive = data.system.mode === 'LIVE'
  const isRealtimeWeather = data.system.weather_data_source === 'live'
  const hasMarketStatus = Boolean(data.system.market_data_label)
  const isHistoricalView = data.system.view_mode === 'HISTORICAL'
  const isModeBusy = modeMutation.isPending
  const isRefreshBusy = isManualRefreshing
  const nextMode = isLive ? 'DRY_RUN' : 'LIVE'

  const handleModeToggle = () => {
    if (nextMode === 'LIVE') {
      if (!confirm('确认切换到实盘模式？将使用真实资金交易。')) return
    }
    modeMutation.mutate(nextMode as 'LIVE' | 'DRY_RUN')
  }

  const handleManualRefresh = () => {
    manualRefresh().catch(() => undefined)
  }

  const handleCityChange = (newCity: string) => {
    setSelectedCity(newCity)
    setSelectedDate(undefined) // Reset date when switching city
  }

  return (
    <div className="dashboard">
      <div className="status-bar">
        <div className="status-main">
          <div className="status-main-top">
            {availableCities.length > 1 && (
              <select
                className="city-selector"
                value={selectedCity}
                onChange={(e) => handleCityChange(e.target.value)}
              >
                {availableCities.map(c => (
                  <option key={c.key} value={c.key}>
                    {CITY_EMOJI[c.key] || '🌍'} {c.name} ({c.unit})
                  </option>
                ))}
              </select>
            )}
            <div className="event-name">{eventLabel}</div>
          </div>
          <div className="status-pills">
            <span className={`status-pill ${isLive ? 'status-pill-live' : 'status-pill-dry'}`}>
              {isLive ? '当前为实盘交易' : '当前为模拟运行'}
            </span>
            <span className={`status-pill ${isRealtimeWeather ? 'status-pill-realtime' : 'status-pill-fallback'}`}>
              天气数据 {data.system.weather_data_label || (forecast?.data_source === 'db' ? 'DB快照回退' : '实时抓取')}
            </span>
            {hasMarketStatus && (
              <span className="status-pill status-pill-market">
                盘口数据 {data.system.market_data_label}
              </span>
            )}
            <span className="status-pill status-pill-view">
              {data.system.view_label || (isHistoricalView ? `历史市场 ${data.system.selected_date}` : '当前市场')}
            </span>
          </div>
        </div>

        <div className="status-controls">
          <div className="mode-summary">
            <div className="mode-summary-label">{data.system.mode_label}</div>
            <div className="mode-summary-desc">{data.system.mode_description}</div>
          </div>

          <div className="control-row">
            <button
              className={`mode-toggle ${isLive ? 'mode-live' : 'mode-dry'}`}
              onClick={handleModeToggle}
              disabled={isModeBusy}
            >
              {isModeBusy ? '切换中...' : data.system.target_mode_label}
            </button>

            <button
              className={`refresh-button ${isRefreshBusy ? 'refresh-button-busy' : ''}`}
              onClick={handleManualRefresh}
              disabled={isModeBusy || isRefreshBusy}
            >
              {isRefreshBusy ? '刷新中...' : '立即刷新'}
            </button>

            {availableDates.length > 0 && (
              <select
                className="date-selector"
                value={selectedDate || ''}
                onChange={(e) => setSelectedDate(e.target.value || undefined)}
              >
                <option value="">最新市场</option>
                {availableDates.map(d => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            )}
          </div>

          <div className="status-meta-list">
            {forecast?.settlement_time && (
              <span>
                距结算 <Countdown target={forecast.settlement_time} />
              </span>
            )}
            {data.system.weather_fetched_at && (
              <span>天气抓取 {dayjs(data.system.weather_fetched_at).format('HH:mm:ss')}</span>
            )}
            {data.system.market_fetched_at && (
              <span>盘口抓取 {dayjs(data.system.market_fetched_at).format('HH:mm:ss')}</span>
            )}
            <span>界面更新 {dayjs(data.system.updated_at).format('HH:mm:ss')}</span>
            {(isFetching || isRefreshBusy) && (
              <span>{isRefreshBusy ? '手动刷新中' : '同步最新数据中'}</span>
            )}
          </div>
        </div>
      </div>

      <div className="grid-row grid-3">
        <ForecastPanel forecast={forecast} prob={prob} backtest={data.backtest} />
        <ProbabilityChart prob={prob} market={market} />
        <EdgeTable
          edges={prob?.edges}
          signals={prob?.signals}
          scenarioPnl={prob?.scenario_pnl}
          sumToOneGap={prob?.sum_to_one_gap}
        />
      </div>

      <div className="grid-row grid-3">
        <WeatherFactorsPanel
          factors={prob?.weather_factors}
          ensemble={prob?.ensemble}
          modelForecasts={prob?.model_forecasts}
        />
        <PositionsPanel positions={positions} />
        <TimingChart timing={timing} />
      </div>

      <div className="grid-row grid-2">
        <StatsPanel stats={data.stats} />
        <BiasChart bias={data.bias} />
      </div>

      <div className="grid-row grid-2">
        <BacktestPanel backtest={data.backtest} />
        <CalibrationPanel />
      </div>
    </div>
  )
}
