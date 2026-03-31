import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import {
  fetchDashboard,
  fetchBias,
  fetchStats,
  switchMode,
  applyAdaptiveWeights,
  fetchCalibrationStatus,
  collectSettlements,
  addSettlement,
  type DashboardData,
  type BiasData,
  type StatsData,
  type CalibrationStatus,
} from './client'

/** Main dashboard data — refreshes every 30s, supports date and city selection */
export function useDashboard(date?: string, city?: string) {
  const queryClient = useQueryClient()
  const [isManualRefreshing, setIsManualRefreshing] = useState(false)

  const query = useQuery<DashboardData>({
    queryKey: ['dashboard', city, date],
    queryFn: () => fetchDashboard(date, city),
    refetchInterval: 30_000,
    retry: 2,
  })

  const manualRefresh = async () => {
    setIsManualRefreshing(true)
    try {
      const fresh = await fetchDashboard(date, city, Date.now())
      queryClient.setQueryData(['dashboard', city, date], fresh)
      return fresh
    } finally {
      setIsManualRefreshing(false)
    }
  }

  return {
    ...query,
    manualRefresh,
    isManualRefreshing,
  }
}

/** Bias calibration — refreshes every 5 min */
export function useBias() {
  return useQuery<BiasData>({
    queryKey: ['bias'],
    queryFn: fetchBias,
    refetchInterval: 300_000,
  })
}

/** Stats/settlements — refreshes every 2 min */
export function useStats() {
  return useQuery<StatsData>({
    queryKey: ['stats'],
    queryFn: fetchStats,
    refetchInterval: 120_000,
  })
}

/** Mode switch mutation */
export function useSwitchMode() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (mode: 'LIVE' | 'DRY_RUN') => switchMode(mode),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard'] })
    },
  })
}

/** Apply adaptive weights from backtest to live config */
export function useApplyAdaptiveWeights() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: applyAdaptiveWeights,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard'] })
    },
  })
}

/** Calibration status */
export function useCalibrationStatus() {
  return useQuery<CalibrationStatus>({
    queryKey: ['calibration-status'],
    queryFn: fetchCalibrationStatus,
    refetchInterval: 300_000,
  })
}

/** Auto-collect past settlements from WU */
export function useCollectSettlements() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (daysBack: number) => collectSettlements(daysBack),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard'] })
      queryClient.invalidateQueries({ queryKey: ['calibration-status'] })
    },
  })
}

/** Manually add a settlement record */
export function useAddSettlement() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ settleDate, wuTemp, notes }: { settleDate: string; wuTemp: number; notes?: string }) =>
      addSettlement(settleDate, wuTemp, notes),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard'] })
      queryClient.invalidateQueries({ queryKey: ['calibration-status'] })
    },
  })
}
