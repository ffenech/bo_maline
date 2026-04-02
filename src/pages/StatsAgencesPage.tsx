import { useEffect, useMemo, useState } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, BarChart, Bar, Cell } from 'recharts'
import { cachedFetch, invalidateClientCache } from '../lib/fetchCache'

// Types pour les stats de réactivité
interface ReactivityStats {
  global: {
    total_leads_with_action: number
    avg_hours_to_first_action: number
    contacted_within_1h: number
    contacted_within_2h: number
    contacted_within_24h: number
    contacted_within_48h: number
    contacted_within_72h: number
    pct_within_1h: number
    pct_within_24h: number
    pct_within_48h: number
    pct_within_72h: number
  }
  per_agency: {
    id_client: string
    client_name: string
    subscription_start_date: string | null
    leads_with_action: number
    avg_hours_to_first_action: number
    contacted_within_1h: number
    contacted_within_2h: number
    contacted_within_24h: number
    contacted_within_48h: number
    contacted_within_72h: number
    pct_within_24h: number
    pct_within_48h: number
  }[]
  distribution: { bucket: string; count: number }[]
  by_action_type: { type: string; leads_count: number; avg_hours: number }[]
  monthly_trend: { month: string; total_leads: number; avg_hours: number; within_24h: number; pct_within_24h: number }[]
  active_clients_count: number
  updated_at: string
}

interface MonthlyStat {
  label: string // YYYY-MM
  active: number
  churned: number
  started: number
  churnRate: number // %
}

function formatMonthLabel(label: string) {
  // label attendu: YYYY-MM
  const [y, m] = label.split('-').map(Number)
  if (!y || !m) return label
  return new Date(y, m - 1, 1).toLocaleDateString('fr-FR', { month: 'short', year: '2-digit' })
}

// Helpers de dates et d'accès par chemin (alignés sur la logique serveur)
function getValueByPath(obj: any, path: string): any {
  if (!obj || !path) return undefined
  const parts = path.split('.')
  let current: any = obj
  for (const p of parts) {
    if (current && Object.prototype.hasOwnProperty.call(current, p)) {
      current = current[p]
    } else {
      return undefined
    }
  }
  return current
}

function coerceDate(input: any): Date | null {
  if (!input) return null
  const d = new Date(input)
  return isNaN(d.getTime()) ? null : d
}

function pickDate(obj: any, candidates: string[]): Date | null {
  for (const key of candidates) {
    const raw = getValueByPath(obj, key)
    const d = coerceDate(raw)
    if (d) return d
  }
  return null
}

function pickString(obj: any, candidates: string[]): string | null {
  for (const key of candidates) {
    const val = getValueByPath(obj, key)
    if (typeof val === 'string' && val.trim().length > 0) return val.trim()
  }
  return null
}

function monthStart(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth(), 1, 0, 0, 0, 0)
}

function monthEnd(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth() + 1, 0, 23, 59, 59, 999)
}

function getMonthBoundsFromLabel(label: string): { start: Date; end: Date } | null {
  const [y, m] = label.split('-').map(Number)
  if (!y || !m) return null
  const start = monthStart(new Date(y, m - 1, 1))
  const end = monthEnd(start)
  return { start, end }
}

function addMonths(d: Date, n: number): Date {
  return new Date(d.getFullYear(), d.getMonth() + n, 1)
}

function formatDateForInput(d: Date): string {
  const yyyy = d.getFullYear()
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}`
}

function computeMonthlyStatsFromAgencies(agencies: { start: Date; end?: Date | null }[]): MonthlyStat[] {
  const valid = agencies.filter(a => !!a.start)
  if (valid.length === 0) return []

  const minStart = valid.reduce((min: Date, a) => (a.start! < min ? a.start! : min), valid[0].start!)
  const firstMonth = monthStart(minStart)
  const lastMonth = monthStart(new Date())

  const months: MonthlyStat[] = []
  for (let cursor = new Date(firstMonth); cursor <= lastMonth; cursor = addMonths(cursor, 1)) {
    const mStart = monthStart(cursor)
    const mEnd = monthEnd(cursor)

    const activeAtStart = valid.filter(a => a.start! <= mStart && (!a.end || a.end > mStart)).length
    const activeAtEnd = valid.filter(a => a.start! <= mEnd && (!a.end || a.end > mEnd)).length
    const churnedInMonth = valid.filter(a => a.end && a.end >= mStart && a.end <= mEnd).length
    const startedInMonth = valid.filter(a => a.start && a.start >= mStart && a.start <= mEnd).length

    const churnRate = activeAtStart > 0 ? (churnedInMonth / activeAtStart) * 100 : 0

    const yyyy = mStart.getFullYear()
    const mm = String(mStart.getMonth() + 1).padStart(2, '0')
    months.push({
      label: `${yyyy}-${mm}`,
      active: activeAtEnd,
      churned: churnedInMonth,
      started: startedInMonth,
      churnRate: Math.round(churnRate * 100) / 100,
    })
  }
  return months
}

function StatsAgencesPage() {
  // Onglet actif
  const [activeTab, setActiveTab] = useState<'general' | 'reactivity'>('general')

  // États pour l'onglet général
  const [stats, setStats] = useState<MonthlyStat[]>([])
  const [loading, setLoading] = useState(true)
  const [agencies, setAgencies] = useState<any[]>([])
  const [loadingAgencies, setLoadingAgencies] = useState(true)
  const [ignoredManualIds, setIgnoredManualIds] = useState<Set<string>>(new Set())
  const [suspendedIds, setSuspendedIds] = useState<Set<string>>(new Set())
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [sortKey, setSortKey] = useState<'name' | 'start' | 'end' | 'status' | 'postalCodes' | 'contactRate' | 'reminderRate' | 'avgReminders' | 'mandatsSigned' | 'leadsCount'>('name')
  const [contactStats, setContactStats] = useState<Map<string, { leadsWithPhone: number; leadsContacted: number; contactRate: number; leadsWithReminder: number; reminderRate: number; avgRemindersDone: number; mandatsSigned: number }>>(new Map())
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [filterIgnored, setFilterIgnored] = useState<'all' | 'ignored' | 'active'>('all')
  const [filterLocale, setFilterLocale] = useState<string>('all')
  const [clientLocales, setClientLocales] = useState<Map<string, string>>(new Map())
  const [showTerminated, setShowTerminated] = useState(true)
  const [manualEndDates, setManualEndDates] = useState<Map<string, Date>>(new Map())
  const [editingEndId, setEditingEndId] = useState<string | null>(null)
  const [editingEndValue, setEditingEndValue] = useState<string>('')

  // États pour l'onglet réactivité
  const [reactivityStats, setReactivityStats] = useState<ReactivityStats | null>(null)
  const [loadingReactivity, setLoadingReactivity] = useState(false)
  const [reactivitySortKey, setReactivitySortKey] = useState<'name' | 'avg_hours' | 'pct_24h' | 'leads' | 'start_date' | 'months'>('avg_hours')
  const [reactivitySortDir, setReactivitySortDir] = useState<'asc' | 'desc'>('asc')

  // Charger toutes les données en parallèle avec cache client
  useEffect(() => {
    const fetchAllData = async () => {
      const apiUrl = import.meta.env.VITE_API_URL || '/api'

      try {
        const [statsData, agenciesData, endDatesData, ignoredData, suspendedData, contactStatsData, clientLocalesData] = await Promise.all([
          cachedFetch<any[]>(`${apiUrl}/agency-stats`),
          cachedFetch<any>(`${apiUrl}/v2/agencies`),
          cachedFetch<any[]>(`${apiUrl}/agency-end-dates`),
          cachedFetch<any[]>(`${apiUrl}/ignored-agencies`),
          cachedFetch<any[]>(`${apiUrl}/suspended-agencies`),
          cachedFetch<any[]>(`${apiUrl}/agency-contact-stats`),
          cachedFetch<any[]>(`${apiUrl}/client-locales`)
        ])

        // Traiter les stats
        setStats(statsData || [])

        // Traiter les agences
        const list = Array.isArray(agenciesData) ? agenciesData : Array.isArray(agenciesData?.data) ? agenciesData.data : []
        setAgencies(list)

        // Traiter les dates de fin manuelles
        const endDatesMap = new Map<string, Date>()
        ;(endDatesData || []).forEach((r: any) => {
          if (r?.agency_id && r?.end_date) {
            const d = coerceDate(r.end_date)
            if (d) endDatesMap.set(String(r.agency_id), d)
          }
        })
        setManualEndDates(endDatesMap)

        // Traiter les agences ignorées
        const ignoredIds = new Set<string>((ignoredData || []).map((r: any) => String(r.agency_id)))
        setIgnoredManualIds(ignoredIds)

        // Traiter les agences suspendues
        const suspendedIdsSet = new Set<string>((suspendedData || []).map((r: any) => String(r.agency_id)))
        setSuspendedIds(suspendedIdsSet)

        // Traiter les stats de contact (mapping par nom normalisé)
        const contactMap = new Map<string, { leadsWithPhone: number; leadsContacted: number; contactRate: number; leadsWithReminder: number; reminderRate: number; avgRemindersDone: number; mandatsSigned: number }>()
        ;(contactStatsData || []).forEach((r: any) => {
          if (r?.agency_name_normalized) {
            contactMap.set(r.agency_name_normalized, {
              leadsWithPhone: r.leads_with_phone || 0,
              leadsContacted: r.leads_contacted || 0,
              contactRate: r.contact_rate || 0,
              leadsWithReminder: r.leads_with_reminder || 0,
              reminderRate: r.reminder_rate || 0,
              avgRemindersDone: r.avg_reminders_done || 0,
              mandatsSigned: r.mandats_signed || 0
            })
          }
        })
        setContactStats(contactMap)

        // Traiter les locales clients (mapping par id_gocardless et nom normalisé)
        const localeMap = new Map<string, string>()
        ;(clientLocalesData || []).forEach((r: any) => {
          if (r?.id_gocardless) localeMap.set(r.id_gocardless, r.locale || 'fr_FR')
          if (r?.name) localeMap.set(r.name.toLowerCase().trim(), r.locale || 'fr_FR')
        })
        setClientLocales(localeMap)
      } catch (e) {
        console.error(e)
      } finally {
        setLoading(false)
        setLoadingAgencies(false)
      }
    }

    fetchAllData()
  }, [])

  // Charger les stats de réactivité quand l'onglet est sélectionné
  useEffect(() => {
    if (activeTab === 'reactivity' && !reactivityStats && !loadingReactivity) {
      const fetchReactivity = async () => {
        setLoadingReactivity(true)
        const apiUrl = import.meta.env.VITE_API_URL || '/api'
        try {
          const data = await cachedFetch<ReactivityStats>(`${apiUrl}/reactivity-stats`)
          setReactivityStats(data)
        } catch (e) {
          console.error('Erreur chargement stats réactivité:', e)
        } finally {
          setLoadingReactivity(false)
        }
      }
      fetchReactivity()
    }
  }, [activeTab, reactivityStats, loadingReactivity])

  // Fonction pour calculer le nombre de mois entre une date et aujourd'hui
  const calculateMonthsDiff = (startDate: string | null): number => {
    if (!startDate) return 0
    const start = new Date(startDate)
    const today = new Date()
    const months = (today.getFullYear() - start.getFullYear()) * 12 + (today.getMonth() - start.getMonth())
    return Math.max(0, months)
  }

  // Tri des agences par réactivité
  const sortedReactivityAgencies = useMemo(() => {
    if (!reactivityStats?.per_agency) return []
    const sorted = [...reactivityStats.per_agency]
    sorted.sort((a, b) => {
      let cmp = 0
      switch (reactivitySortKey) {
        case 'name':
          cmp = a.client_name.localeCompare(b.client_name)
          break
        case 'avg_hours':
          cmp = a.avg_hours_to_first_action - b.avg_hours_to_first_action
          break
        case 'pct_24h':
          cmp = a.pct_within_24h - b.pct_within_24h
          break
        case 'leads':
          cmp = a.leads_with_action - b.leads_with_action
          break
        case 'start_date': {
          const aDate = a.subscription_start_date ? new Date(a.subscription_start_date).getTime() : 0
          const bDate = b.subscription_start_date ? new Date(b.subscription_start_date).getTime() : 0
          cmp = aDate - bDate
          break
        }
        case 'months': {
          const aMonths = calculateMonthsDiff(a.subscription_start_date)
          const bMonths = calculateMonthsDiff(b.subscription_start_date)
          cmp = aMonths - bMonths
          break
        }
      }
      return reactivitySortDir === 'asc' ? cmp : -cmp
    })
    return sorted
  }, [reactivityStats, reactivitySortKey, reactivitySortDir])

  // Clés candidates pour dates (alignées sur le serveur)
  const startKeys = useMemo(() => [
    'startDate', 'start_at', 'startAt', 'startedAt',
    'subscriptionStart', 'subscription.startDate', 'subscription.start_at', 'subscription.startedAt',
    'activationDate', 'activatedAt', 'activated_at',
    'createdAt', 'created_at', 'created_date',
    'dateStart', 'date_start',
    'dateDebut', 'date_debut', 'debut'
  ], [])

  const endKeys = useMemo(() => [
    'endDate', 'end_at', 'endAt', 'endedAt',
    'subscriptionEnd', 'subscription.endDate', 'subscription.end_at', 'subscription.endedAt',
    'deactivationDate', 'deactivatedAt', 'deactivated_at',
    'canceledAt', 'cancelledAt', 'cancellationDate',
    'closedAt', 'closed_at',
    'dateEnd', 'date_end',
    'dateFin', 'date_fin', 'resiliationDate', 'resiliation_date', 'fin'
  ], [])

  const agencyNameKeys = useMemo(() => [
    'name', 'agencyName', 'agency_name', 'title', 'companyName', 'denomination',
    'nom', 'libelle', 'label', 'raisonSociale', 'raison_sociale', 'agence',
    'agency.name', 'company.name'
  ], [])

  const clientNameKeys = useMemo(() => [
    'clientName', 'client_name', 'client.nom', 'client.name', 'customerName',
    'brand', 'group', 'groupName', 'accountName', 'organization', 'organisation', 'company', 'client'
  ], [])

  // Normalisation de base des agences
  const baseAgencies = useMemo(() => {
    return agencies.map((a, idx) => {
      let start = pickDate(a, startKeys)
      if (!start) start = pickDate(a, ['created', 'creationDate'])
      let end = pickDate(a, endKeys)

      const agencyName = pickString(a, agencyNameKeys)
      const clientName = pickString(a, clientNameKeys)
      const displayName = clientName
        ? `${clientName} — ${agencyName ?? 'Sans nom'}`
        : (agencyName ?? `Agence ${a?.id ?? a?.idClient ?? idx + 1}`)

      // Nom brut pour le mapping avec la BDD V3 (normalisé en minuscules)
      const rawName = agencyName?.toLowerCase().trim() || ''

      const rawId = a?.id ?? a?.idClient ?? a?.identifier ?? a?.uuid ?? idx
      const id = String(rawId)
      const manualEnd = manualEndDates.get(id)
      if (manualEnd) {
        end = manualEnd
      }
      const isIgnoredAuto = !start

      // Extraction des codes postaux depuis tarifs
      const postalCodes: string[] = []
      if (a?.tarifs && Array.isArray(a.tarifs)) {
        a.tarifs.forEach((tarif: any) => {
          if (tarif?.code_postal && !postalCodes.includes(tarif.code_postal)) {
            postalCodes.push(tarif.code_postal)
          }
        })
      }

      // Déterminer la locale via id_gocardless ou nom
      const gcl = a?.id_gocardless?.replace?.(/"/g, '') || ''
      const locale = (gcl && clientLocales.get(gcl)) || clientLocales.get(rawName) || 'fr_FR'

      return { raw: a, id, name: displayName, rawName, start, end, isIgnoredAuto, postalCodes, locale }
    })
  }, [agencies, startKeys, endKeys, agencyNameKeys, clientNameKeys, manualEndDates, clientLocales])

  // Recalcul local des stats en excluant ignorées (auto + manuel)
  const filteredForStats = useMemo(() => {
    return baseAgencies
      .filter(a => !a.isIgnoredAuto && !ignoredManualIds.has(a.id))
      .map(a => ({ start: a.start as Date, end: a.end || null }))
  }, [baseAgencies, ignoredManualIds])

  const computedStats = useMemo(() => computeMonthlyStatsFromAgencies(filteredForStats), [filteredForStats])

  const finalStats = useMemo(() => (computedStats.length > 0 ? computedStats : stats), [computedStats, stats])

  const totals = useMemo(() => {
    return finalStats.reduce(
      (acc, m) => {
        acc.active = m.active
        acc.churned += m.churned
        return acc
      },
      { active: 0, churned: 0 }
    )
  }, [finalStats])

  const displayStats = useMemo(() => {
    return [...finalStats].reverse()
  }, [finalStats])

  const latestMonthBounds = useMemo(() => {
    if (!finalStats || finalStats.length === 0) return null
    const last = finalStats[finalStats.length - 1]
    return getMonthBoundsFromLabel(last.label)
  }, [finalStats])

  const normalizedAgencies = useMemo(() => {
    const bounds = latestMonthBounds
    return baseAgencies.map(a => {
      const isChurnedThisLatestMonth = !!(bounds && a.end && a.end >= bounds.start && a.end <= bounds.end)
      const isIgnoredManual = ignoredManualIds.has(a.id)
      const isIgnored = a.isIgnoredAuto || isIgnoredManual
      const isSuspended = suspendedIds.has(a.id)
      return { ...a, isChurnedThisLatestMonth, isIgnored, isIgnoredManual, isSuspended }
    })
  }, [baseAgencies, latestMonthBounds, ignoredManualIds, suspendedIds])

  // Tri
  const statusWeight = (ag: any) => {
    if (ag.isIgnored) return 2
    if (ag.end) return 1 // résiliée
    return 0 // active
  }

  const compare = (a: any, b: any): number => {
    switch (sortKey) {
      case 'name': {
        const an = (a.name || '').toString().toLowerCase()
        const bn = (b.name || '').toString().toLowerCase()
        return an.localeCompare(bn)
      }
      case 'start': {
        const av = a.start ? a.start.getTime() : Number.POSITIVE_INFINITY
        const bv = b.start ? b.start.getTime() : Number.POSITIVE_INFINITY
        return av - bv
      }
      case 'end': {
        const av = a.end ? a.end.getTime() : Number.POSITIVE_INFINITY
        const bv = b.end ? b.end.getTime() : Number.POSITIVE_INFINITY
        return av - bv
      }
      case 'status': {
        return statusWeight(a) - statusWeight(b)
      }
      case 'postalCodes': {
        const aFirst = a.postalCodes && a.postalCodes.length > 0 ? a.postalCodes[0] : ''
        const bFirst = b.postalCodes && b.postalCodes.length > 0 ? b.postalCodes[0] : ''
        return aFirst.localeCompare(bFirst)
      }
      case 'contactRate': {
        const aStats = contactStats.get(a.rawName || '')
        const bStats = contactStats.get(b.rawName || '')
        const aRate = aStats?.contactRate || 0
        const bRate = bStats?.contactRate || 0
        return aRate - bRate
      }
      case 'reminderRate': {
        const aStats = contactStats.get(a.rawName || '')
        const bStats = contactStats.get(b.rawName || '')
        const aRate = aStats?.reminderRate || 0
        const bRate = bStats?.reminderRate || 0
        return aRate - bRate
      }
      case 'avgReminders': {
        const aStats = contactStats.get(a.rawName || '')
        const bStats = contactStats.get(b.rawName || '')
        const aAvg = aStats?.avgRemindersDone || 0
        const bAvg = bStats?.avgRemindersDone || 0
        return aAvg - bAvg
      }
      case 'mandatsSigned': {
        const aStats = contactStats.get(a.rawName || '')
        const bStats = contactStats.get(b.rawName || '')
        const aCount = aStats?.mandatsSigned || 0
        const bCount = bStats?.mandatsSigned || 0
        return aCount - bCount
      }
      case 'leadsCount': {
        const aStats = contactStats.get(a.rawName || '')
        const bStats = contactStats.get(b.rawName || '')
        const aCount = aStats?.leadsWithPhone || 0
        const bCount = bStats?.leadsWithPhone || 0
        return aCount - bCount
      }
      default:
        return 0
    }
  }

  const filteredAgencies = useMemo(() => {
    let filtered = normalizedAgencies

    // Filtre par statut ignoré/actif
    if (filterIgnored === 'ignored') {
      filtered = filtered.filter(a => a.isIgnored)
    } else if (filterIgnored === 'active') {
      filtered = filtered.filter(a => !a.isIgnored)
    }

    // Filtre pour masquer les résiliées
    if (!showTerminated) {
      filtered = filtered.filter(a => !a.end)
    }

    // Filtre par pays
    if (filterLocale !== 'all') {
      filtered = filtered.filter(a => a.locale === filterLocale)
    }

    return filtered
  }, [normalizedAgencies, filterIgnored, showTerminated, filterLocale])

  const sortedAgencies = useMemo(() => {
    const arr = [...filteredAgencies]
    arr.sort((a, b) => {
      const c = compare(a, b)
      return sortDir === 'asc' ? c : -c
    })
    return arr
  }, [filteredAgencies, sortKey, sortDir, contactStats])

  const toggleSort = (key: 'name' | 'start' | 'end' | 'status' | 'postalCodes' | 'contactRate' | 'reminderRate' | 'avgReminders' | 'mandatsSigned' | 'leadsCount') => {
    if (sortKey === key) {
      setSortDir(prev => (prev === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const renderSort = (key: 'name' | 'start' | 'end' | 'status' | 'postalCodes' | 'contactRate' | 'reminderRate' | 'avgReminders' | 'mandatsSigned' | 'leadsCount') => {
    if (sortKey !== key) return null
    return <span className="ml-1 text-gray-400">{sortDir === 'asc' ? '▲' : '▼'}</span>
  }

  // Sélection et actions
  const toggleSelectAll = (checked: boolean) => {
    if (checked) {
      setSelectedIds(new Set(filteredAgencies.map(a => a.id)))
    } else {
      setSelectedIds(new Set())
    }
  }

  const toggleSelectOne = (id: string, checked: boolean) => {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (checked) next.add(id)
      else next.delete(id)
      return next
    })
  }

  const ignoreSelected = () => {
    const ids = Array.from(selectedIds)
    if (ids.length === 0) return
    ;(async () => {
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/ignored-agencies`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agency_ids: ids })
        })
        if (!response.ok) throw new Error('Erreur lors de la sauvegarde des agences ignorées')
        invalidateClientCache('ignored-agencies')
        setIgnoredManualIds(prev => {
          const next = new Set(prev)
          ids.forEach(id => next.add(id))
          return next
        })
        setSelectedIds(new Set())
      } catch (e) {
        console.error(e)
      }
    })()
  }

  const reintegrateSelected = () => {
    const ids = Array.from(selectedIds)
    if (ids.length === 0) return
    ;(async () => {
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/ignored-agencies`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agency_ids: ids })
        })
        if (!response.ok) throw new Error('Erreur lors de la réintégration des agences')
        invalidateClientCache('ignored-agencies')
        setIgnoredManualIds(prev => {
          const next = new Set(prev)
          ids.forEach(id => next.delete(id))
          return next
        })
        setSelectedIds(new Set())
      } catch (e) {
        console.error(e)
      }
    })()
  }

  const suspendSelected = () => {
    const ids = Array.from(selectedIds)
    if (ids.length === 0) return
    ;(async () => {
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/suspended-agencies`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agency_ids: ids })
        })
        if (!response.ok) throw new Error('Erreur lors de la suspension des agences')
        invalidateClientCache('suspended-agencies')
        setSuspendedIds(prev => {
          const next = new Set(prev)
          ids.forEach(id => next.add(id))
          return next
        })
        setSelectedIds(new Set())
      } catch (e) {
        console.error(e)
      }
    })()
  }

  const unsuspendSelected = () => {
    const ids = Array.from(selectedIds)
    if (ids.length === 0) return
    ;(async () => {
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/suspended-agencies`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agency_ids: ids })
        })
        if (!response.ok) throw new Error('Erreur lors de la réactivation des agences')
        invalidateClientCache('suspended-agencies')
        setSuspendedIds(prev => {
          const next = new Set(prev)
          ids.forEach(id => next.delete(id))
          return next
        })
        setSelectedIds(new Set())
      } catch (e) {
        console.error(e)
      }
    })()
  }

  // Edition des dates de fin manuelles
  const beginEditEndDate = (id: string, currentEnd?: Date | null) => {
    setEditingEndId(id)
    if (currentEnd) {
      setEditingEndValue(formatDateForInput(currentEnd))
    } else {
      setEditingEndValue('')
    }
  }

  const cancelEditEndDate = () => {
    setEditingEndId(null)
    setEditingEndValue('')
  }

  const saveEndDate = async (id: string) => {
    if (!editingEndValue) return
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/agency-end-dates`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: [{ agency_id: id, end_date: editingEndValue }] })
      })
      if (!response.ok) throw new Error('Erreur lors de l\'enregistrement de la date de fin')
      invalidateClientCache('agency-end-dates')
      const dateObj = coerceDate(editingEndValue)
      if (dateObj) {
        setManualEndDates(prev => {
          const next = new Map(prev)
          next.set(id, dateObj)
          return next
        })
      }
      cancelEditEndDate()
    } catch (e) {
      console.error(e)
    }
  }

  const clearEndDate = async (id: string) => {
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/agency-end-dates`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ agency_ids: [id] })
      })
      if (!response.ok) throw new Error('Erreur lors de la suppression de la date de fin')
      invalidateClientCache('agency-end-dates')
      setManualEndDates(prev => {
        const next = new Map(prev)
        next.delete(id)
        return next
      })
      if (editingEndId === id) cancelEditEndDate()
    } catch (e) {
      console.error(e)
    }
  }

  // Calcul de la durée moyenne de rétention des clients
  // Critères: date de début avant le 30 avril 2025 ET non ignorés
  const averageTenure = useMemo(() => {
    const cutoffDate = new Date(2025, 3, 30) // 30 avril 2025 (mois 3 = avril car 0-indexé)
    const today = new Date()

    const eligibleAgencies = normalizedAgencies.filter(a => {
      // Doit avoir une date de début
      if (!a.start) return false
      // Date de début doit être avant le 30 avril 2025
      if (a.start >= cutoffDate) return false
      // Ne doit pas être ignorée
      if (a.isIgnored) return false
      return true
    })

    if (eligibleAgencies.length === 0) {
      return { averageMonths: 0, count: 0 }
    }

    const totalMonths = eligibleAgencies.reduce((sum, a) => {
      const endDate = a.end || today
      const durationMs = endDate.getTime() - a.start!.getTime()
      const durationMonths = durationMs / (1000 * 60 * 60 * 24 * 30.44) // approximation 30.44 jours/mois
      return sum + durationMonths
    }, 0)

    return {
      averageMonths: totalMonths / eligibleAgencies.length,
      count: eligibleAgencies.length
    }
  }, [normalizedAgencies])

  // Calcul de l'évolution de la durée moyenne par année (5 dernières années)
  const tenureEvolution = useMemo(() => {
    const cutoffDate = new Date(2025, 3, 30) // 30 avril 2025
    const currentYear = new Date().getFullYear()
    const years = [currentYear - 4, currentYear - 3, currentYear - 2, currentYear - 1, currentYear]

    return years.map(year => {
      // Pour chaque année, calculer la moyenne à la fin de cette année
      const endOfYear = new Date(year, 11, 31) // 31 décembre de l'année

      const eligibleAgencies = normalizedAgencies.filter(a => {
        // Doit avoir une date de début
        if (!a.start) return false
        // Date de début doit être avant le 30 avril 2025
        if (a.start >= cutoffDate) return false
        // Ne doit pas être ignorée
        if (a.isIgnored) return false
        // Doit avoir commencé avant la fin de l'année considérée
        if (a.start > endOfYear) return false
        return true
      })

      if (eligibleAgencies.length === 0) {
        return { year: year.toString(), averageMonths: 0, count: 0 }
      }

      const totalMonths = eligibleAgencies.reduce((sum, a) => {
        const endDate = a.end && a.end <= endOfYear ? a.end : endOfYear
        const durationMs = endDate.getTime() - a.start!.getTime()
        const durationMonths = durationMs / (1000 * 60 * 60 * 24 * 30.44)
        return sum + Math.max(0, durationMonths)
      }, 0)

      return {
        year: year.toString(),
        averageMonths: parseFloat((totalMonths / eligibleAgencies.length).toFixed(1)),
        count: eligibleAgencies.length
      }
    })
  }, [normalizedAgencies])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="text-gray-600 mt-4">Chargement des statistiques agences...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-full overflow-hidden">
      {/* Header avec onglets */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">Stats agences</h2>
            <p className="text-sm text-gray-500 mt-1">
              {activeTab === 'general'
                ? 'Agences actives, résiliations et taux de résiliation par mois (BDD V2)'
                : 'Réactivité du traitement des leads - Clients actifs uniquement - Heures ouvrées (hors WE et jours fériés)'}
            </p>
          </div>
        </div>

        {/* Onglets de navigation */}
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8">
            <button
              onClick={() => setActiveTab('general')}
              className={`py-2 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === 'general'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Stats générales
            </button>
            <button
              onClick={() => setActiveTab('reactivity')}
              className={`py-2 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === 'reactivity'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Réactivité
            </button>
          </nav>
        </div>
      </div>

      {/* Contenu de l'onglet Réactivité */}
      {activeTab === 'reactivity' && (
        <>
          {loadingReactivity ? (
            <div className="flex items-center justify-center h-64">
              <div className="text-center">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
                <p className="text-gray-600 mt-4">Chargement des statistiques de réactivité...</p>
              </div>
            </div>
          ) : reactivityStats ? (
            <div className="space-y-6">
              {/* KPIs globaux */}
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-semibold text-gray-900">Statistiques globales de réactivité</h3>
                  <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                    {reactivityStats.active_clients_count} clients actifs
                  </span>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
                    <div className="text-xs text-blue-600 font-medium">Leads avec action</div>
                    <div className="text-2xl font-bold text-blue-900">{reactivityStats.global.total_leads_with_action.toLocaleString('fr-FR')}</div>
                  </div>
                  <div className="bg-green-50 rounded-lg p-4 border border-green-200">
                    <div className="text-xs text-green-600 font-medium">Temps moyen 1ère action (ouvrées)</div>
                    <div className="text-2xl font-bold text-green-900">{reactivityStats.global.avg_hours_to_first_action.toFixed(1)}h</div>
                  </div>
                  <div className="bg-purple-50 rounded-lg p-4 border border-purple-200">
                    <div className="text-xs text-purple-600 font-medium">Contactés en 24h</div>
                    <div className="text-2xl font-bold text-purple-900">{reactivityStats.global.pct_within_24h}%</div>
                    <div className="text-xs text-purple-600">{reactivityStats.global.contacted_within_24h.toLocaleString('fr-FR')} leads</div>
                  </div>
                  <div className="bg-orange-50 rounded-lg p-4 border border-orange-200">
                    <div className="text-xs text-orange-600 font-medium">Contactés en 1h</div>
                    <div className="text-2xl font-bold text-orange-900">{reactivityStats.global.pct_within_1h}%</div>
                    <div className="text-xs text-orange-600">{reactivityStats.global.contacted_within_1h.toLocaleString('fr-FR')} leads</div>
                  </div>
                </div>
              </div>

              {/* Distribution et évolution */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Histogramme de distribution */}
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-4">Distribution des temps de réponse (heures ouvrées)</h3>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={reactivityStats.distribution}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="bucket" tick={{ fontSize: 11 }} />
                        <YAxis tick={{ fontSize: 11 }} />
                        <Tooltip formatter={(value: any) => [Number(value).toLocaleString('fr-FR'), 'Leads']} />
                        <Bar dataKey="count" name="Leads">
                          {reactivityStats.distribution.map((_, index) => (
                            <Cell
                              key={`cell-${index}`}
                              fill={index < 2 ? '#10b981' : index < 4 ? '#f59e0b' : index < 6 ? '#f97316' : '#ef4444'}
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                {/* Évolution mensuelle */}
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-4">Évolution mensuelle (12 derniers mois)</h3>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={reactivityStats.monthly_trend}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                        <YAxis yAxisId="left" tick={{ fontSize: 11 }} />
                        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} domain={[0, 100]} />
                        <Tooltip />
                        <Legend />
                        <Line yAxisId="left" type="monotone" dataKey="avg_hours" name="Temps moyen (h)" stroke="#3b82f6" strokeWidth={2} />
                        <Line yAxisId="right" type="monotone" dataKey="pct_within_24h" name="% en 24h" stroke="#10b981" strokeWidth={2} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              {/* Stats par type d'action */}
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">Réactivité par type d'action</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
                  {reactivityStats.by_action_type.map((action) => (
                    <div key={action.type} className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <div className="text-xs text-gray-500 capitalize">{action.type || 'Autre'}</div>
                      <div className="text-lg font-bold text-gray-900">{action.avg_hours.toFixed(1)}h</div>
                      <div className="text-xs text-gray-400">{action.leads_count.toLocaleString('fr-FR')} leads</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Tableau par agence */}
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                <div className="px-6 py-4 border-b border-gray-200">
                  <h3 className="text-lg font-semibold text-gray-900">Réactivité par agence</h3>
                  <p className="text-sm text-gray-500">Agences avec au moins 5 leads traités</p>
                </div>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th
                          className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                          onClick={() => {
                            if (reactivitySortKey === 'name') setReactivitySortDir(d => d === 'asc' ? 'desc' : 'asc')
                            else { setReactivitySortKey('name'); setReactivitySortDir('asc') }
                          }}
                        >
                          Agence {reactivitySortKey === 'name' && (reactivitySortDir === 'asc' ? '↑' : '↓')}
                        </th>
                        <th
                          className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                          onClick={() => {
                            if (reactivitySortKey === 'leads') setReactivitySortDir(d => d === 'asc' ? 'desc' : 'asc')
                            else { setReactivitySortKey('leads'); setReactivitySortDir('desc') }
                          }}
                        >
                          Leads {reactivitySortKey === 'leads' && (reactivitySortDir === 'asc' ? '↑' : '↓')}
                        </th>
                        <th
                          className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                          onClick={() => {
                            if (reactivitySortKey === 'start_date') setReactivitySortDir(d => d === 'asc' ? 'desc' : 'asc')
                            else { setReactivitySortKey('start_date'); setReactivitySortDir('asc') }
                          }}
                        >
                          Date début {reactivitySortKey === 'start_date' && (reactivitySortDir === 'asc' ? '↑' : '↓')}
                        </th>
                        <th
                          className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                          onClick={() => {
                            if (reactivitySortKey === 'months') setReactivitySortDir(d => d === 'asc' ? 'desc' : 'asc')
                            else { setReactivitySortKey('months'); setReactivitySortDir('desc') }
                          }}
                        >
                          Nbre mois {reactivitySortKey === 'months' && (reactivitySortDir === 'asc' ? '↑' : '↓')}</th>
                        <th
                          className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                          onClick={() => {
                            if (reactivitySortKey === 'avg_hours') setReactivitySortDir(d => d === 'asc' ? 'desc' : 'asc')
                            else { setReactivitySortKey('avg_hours'); setReactivitySortDir('asc') }
                          }}
                        >
                          Temps moy. {reactivitySortKey === 'avg_hours' && (reactivitySortDir === 'asc' ? '↑' : '↓')}
                        </th>
                        <th
                          className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                          onClick={() => {
                            if (reactivitySortKey === 'pct_24h') setReactivitySortDir(d => d === 'asc' ? 'desc' : 'asc')
                            else { setReactivitySortKey('pct_24h'); setReactivitySortDir('desc') }
                          }}
                        >
                          % 24h {reactivitySortKey === 'pct_24h' && (reactivitySortDir === 'asc' ? '↑' : '↓')}
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">% 48h</th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">En 1h</th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">En 2h</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {sortedReactivityAgencies.map((agency) => (
                        <tr key={agency.id_client} className="hover:bg-gray-50">
                          <td className="px-4 py-3 text-sm font-medium text-gray-900">{agency.client_name}</td>
                          <td className="px-4 py-3 text-sm text-center text-gray-600">{agency.leads_with_action.toLocaleString('fr-FR')}</td>
                          <td className="px-4 py-3 text-sm text-center text-gray-600">
                            {agency.subscription_start_date
                              ? new Date(agency.subscription_start_date).toLocaleDateString('fr-FR', { day: '2-digit', month: 'short', year: 'numeric' })
                              : '-'}
                          </td>
                          <td className="px-4 py-3 text-sm text-center">
                            {agency.subscription_start_date ? (
                              <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                {calculateMonthsDiff(agency.subscription_start_date)} mois
                              </span>
                            ) : '-'}
                          </td>
                          <td className="px-4 py-3 text-sm text-center">
                            <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                              agency.avg_hours_to_first_action <= 2 ? 'bg-green-100 text-green-800' :
                              agency.avg_hours_to_first_action <= 24 ? 'bg-yellow-100 text-yellow-800' :
                              agency.avg_hours_to_first_action <= 72 ? 'bg-orange-100 text-orange-800' :
                              'bg-red-100 text-red-800'
                            }`}>
                              {agency.avg_hours_to_first_action.toFixed(1)}h
                            </span>
                          </td>
                          <td className="px-4 py-3 text-sm text-center">
                            <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                              agency.pct_within_24h >= 80 ? 'bg-green-100 text-green-800' :
                              agency.pct_within_24h >= 50 ? 'bg-yellow-100 text-yellow-800' :
                              'bg-red-100 text-red-800'
                            }`}>
                              {agency.pct_within_24h}%
                            </span>
                          </td>
                          <td className="px-4 py-3 text-sm text-center text-gray-600">{agency.pct_within_48h}%</td>
                          <td className="px-4 py-3 text-sm text-center text-gray-600">{agency.contacted_within_1h}</td>
                          <td className="px-4 py-3 text-sm text-center text-gray-600">{agency.contacted_within_2h}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Mise à jour */}
              <div className="text-xs text-gray-400 text-right">
                Dernière mise à jour: {new Date(reactivityStats.updated_at).toLocaleString('fr-FR')}
              </div>
            </div>
          ) : (
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 text-center text-gray-500">
              Aucune donnée de réactivité disponible
            </div>
          )}
        </>
      )}

      {/* Contenu de l'onglet Stats générales */}
      {activeTab === 'general' && (
        <>
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <div className="text-xs text-gray-500">Résiliations cumulées</div>
            <div className="text-xl font-bold text-gray-900">{totals.churned.toLocaleString('fr-FR')}</div>
          </div>
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <div className="text-xs text-gray-500">Parc actuel (fin dernier mois)</div>
            <div className="text-xl font-bold text-gray-900">{totals.active.toLocaleString('fr-FR')}</div>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900">Tableau mensuel</h3>
        </div>
        <div className="overflow-x-auto max-w-full">
          <div className="inline-block min-w-full align-middle">
            <div className="overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">
                      Métrique
                    </th>
                    {displayStats.map((m, idx) => (
                      <th key={idx} className="px-2 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[90px]">
                        {formatMonthLabel(m.label)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {displayStats.length === 0 ? (
                    <tr>
                      <td colSpan={displayStats.length + 1} className="px-6 py-8 text-center text-gray-500">
                        Aucune donnée disponible
                      </td>
                    </tr>
                  ) : (
                    <>
                      <tr className="hover:bg-gray-50 transition-colors">
                        <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                          Agences actives (fin de mois)
                        </td>
                        {displayStats.map((m, idx) => (
                          <td key={idx} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                              {m.active}
                            </span>
                          </td>
                        ))}
                      </tr>
                      <tr className="hover:bg-gray-50 transition-colors">
                        <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                          Agences résiliées (mois)
                        </td>
                        {displayStats.map((m, idx) => (
                          <td key={idx} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">
                              {m.churned}
                            </span>
                          </td>
                        ))}
                      </tr>
                      <tr className="hover:bg-gray-50 transition-colors">
                        <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                          Agences débutées (mois)
                        </td>
                        {displayStats.map((m, idx) => (
                          <td key={idx} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                              {m.started}
                            </span>
                          </td>
                        ))}
                      </tr>
                      <tr className="hover:bg-gray-50 transition-colors bg-purple-50">
                        <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-purple-50 z-10">
                          Taux de résiliation (sur parc début de mois)
                        </td>
                        {displayStats.map((m, idx) => (
                          <td key={idx} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-bold bg-purple-100 text-purple-800">
                              {m.churnRate.toFixed(2)}%
                            </span>
                          </td>
                        ))}
                      </tr>
                    </>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      {/* Carte durée moyenne de rétention */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Durée moyenne de rétention</h3>
            <p className="text-sm text-gray-500 mt-1">Clients ayant débuté avant le 30 avril 2025 (hors ignorés)</p>
          </div>
        </div>
        <div className="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="bg-gradient-to-br from-blue-50 to-cyan-50 rounded-lg p-6 border border-blue-200">
            <div className="text-sm text-blue-700 font-medium">Durée moyenne actuelle</div>
            <div className="text-3xl font-bold text-blue-900 mt-2">
              {averageTenure.averageMonths.toFixed(1)} mois
            </div>
            <div className="text-xs text-blue-600 mt-1">
              ≈ {(averageTenure.averageMonths / 12).toFixed(1)} années
            </div>
          </div>
          <div className="bg-gradient-to-br from-purple-50 to-pink-50 rounded-lg p-6 border border-purple-200">
            <div className="text-sm text-purple-700 font-medium">Nombre de clients</div>
            <div className="text-3xl font-bold text-purple-900 mt-2">
              {averageTenure.count.toLocaleString('fr-FR')}
            </div>
            <div className="text-xs text-purple-600 mt-1">
              Clients éligibles au calcul
            </div>
          </div>
        </div>

        {/* Graphique d'évolution */}
        <div className="mt-6">
          <h4 className="text-md font-semibold text-gray-900 mb-4">Évolution sur les 5 dernières années</h4>
          <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={tenureEvolution}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="year"
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                />
                <YAxis
                  stroke="#6b7280"
                  style={{ fontSize: '12px' }}
                  label={{ value: 'Mois', angle: -90, position: 'insideLeft', style: { fontSize: '12px' } }}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#ffffff',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    fontSize: '12px'
                  }}
                  formatter={(value: any) => [`${Number(value).toFixed(1)} mois`, 'Durée moyenne']}
                  labelFormatter={(label: any) => `Année ${label}`}
                />
                <Legend
                  wrapperStyle={{ fontSize: '12px' }}
                  formatter={() => 'Durée moyenne de rétention'}
                />
                <Line
                  type="monotone"
                  dataKey="averageMonths"
                  stroke="#2563eb"
                  strokeWidth={3}
                  dot={{ fill: '#2563eb', r: 5 }}
                  activeDot={{ r: 7 }}
                  name="Durée moyenne"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Tableau des agences V2 - Design moderne */}
      <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden">
        {/* Header avec titre et statistiques */}
        <div className="bg-gradient-to-r from-slate-50 to-blue-50 px-6 py-5 border-b border-gray-200">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-xl font-bold text-gray-900">Liste des agences</h3>
              <p className="text-sm text-gray-600 mt-1">
                {filteredAgencies.length} agence{filteredAgencies.length > 1 ? 's' : ''} affichée{filteredAgencies.length > 1 ? 's' : ''}
                {selectedIds.size > 0 && ` • ${selectedIds.size} sélectionnée${selectedIds.size > 1 ? 's' : ''}`}
              </p>
            </div>

            {/* Actions rapides */}
            {selectedIds.size > 0 && (
              <div className="flex items-center gap-3">
                <button
                  onClick={ignoreSelected}
                  className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-red-600 text-white hover:bg-red-700 shadow-md hover:shadow-lg transition-all duration-200"
                >
                  <span>🚫</span>
                  Ignorer ({selectedIds.size})
                </button>
                <button
                  onClick={reintegrateSelected}
                  className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 shadow-md hover:shadow-lg transition-all duration-200"
                >
                  <span>✓</span>
                  Réintégrer ({selectedIds.size})
                </button>
                <button
                  onClick={suspendSelected}
                  className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-orange-600 text-white hover:bg-orange-700 shadow-md hover:shadow-lg transition-all duration-200"
                >
                  <span>⏸️</span>
                  Suspendre ({selectedIds.size})
                </button>
                <button
                  onClick={unsuspendSelected}
                  className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700 shadow-md hover:shadow-lg transition-all duration-200"
                >
                  <span>▶️</span>
                  Réactiver ({selectedIds.size})
                </button>
              </div>
            )}
          </div>

          {/* Filtres améliorés */}
          <div className="flex flex-wrap items-center gap-4">
            {/* Filtres de statut */}
            <div className="flex items-center gap-2">
              <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Statut:</span>
              <div className="flex items-center gap-1 bg-white rounded-lg p-1 shadow-sm">
                <button
                  onClick={() => setFilterIgnored('all')}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    filterIgnored === 'all'
                      ? 'bg-blue-600 text-white shadow-sm'
                      : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  Toutes ({normalizedAgencies.length})
                </button>
                <button
                  onClick={() => setFilterIgnored('active')}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    filterIgnored === 'active'
                      ? 'bg-emerald-600 text-white shadow-sm'
                      : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  Actives ({normalizedAgencies.filter(a => !a.isIgnored).length})
                </button>
                <button
                  onClick={() => setFilterIgnored('ignored')}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    filterIgnored === 'ignored'
                      ? 'bg-red-600 text-white shadow-sm'
                      : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  Ignorées ({normalizedAgencies.filter(a => a.isIgnored).length})
                </button>
              </div>
            </div>

            {/* Filtre pays */}
            <div className="flex items-center gap-3 bg-white rounded-lg px-3 py-2 shadow-sm">
              <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Pays:</span>
              <label className="flex items-center gap-1.5 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={filterLocale === 'all' || filterLocale === 'fr_FR'}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setFilterLocale(filterLocale === 'es_ES' ? 'all' : 'fr_FR')
                    } else {
                      setFilterLocale('es_ES')
                    }
                  }}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                />
                <span className="select-none">France</span>
              </label>
              <label className="flex items-center gap-1.5 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={filterLocale === 'all' || filterLocale === 'es_ES'}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setFilterLocale(filterLocale === 'fr_FR' ? 'all' : 'es_ES')
                    } else {
                      setFilterLocale('fr_FR')
                    }
                  }}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                />
                <span className="select-none">Espagne</span>
              </label>
            </div>

            {/* Filtre résiliées avec switch moderne */}
            <div className="flex items-center gap-3 bg-white rounded-lg px-3 py-2 shadow-sm">
              <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Résiliées:</span>
              <label className="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  checked={showTerminated}
                  onChange={(e) => setShowTerminated(e.target.checked)}
                  className="sr-only peer"
                />
                <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                <span className="ml-2 text-sm font-medium text-gray-700">
                  {showTerminated ? 'Affichées' : 'Masquées'}
                </span>
              </label>
            </div>
          </div>
        </div>
        {loadingAgencies ? (
          <div className="flex flex-col items-center justify-center p-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            <p className="text-gray-600 mt-4">Chargement des agences…</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="bg-gradient-to-r from-gray-50 to-slate-50 border-b-2 border-gray-200">
                  <th className="px-6 py-4 text-left">
                    <input
                      type="checkbox"
                      aria-label="Sélectionner tout"
                      checked={selectedIds.size > 0 && selectedIds.size === filteredAgencies.length}
                      onChange={e => toggleSelectAll(e.target.checked)}
                      className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                    />
                  </th>
                  <th
                    onClick={() => toggleSort('name')}
                    className="px-6 py-4 text-left text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center gap-2">
                      Agence
                      {renderSort('name')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('leadsCount')}
                    className="px-6 py-4 text-center text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center justify-center gap-2">
                      Nb leads
                      {renderSort('leadsCount')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('contactRate')}
                    className="px-6 py-4 text-center text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center justify-center gap-2">
                      % Lead contacté
                      {renderSort('contactRate')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('reminderRate')}
                    className="px-6 py-4 text-center text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center justify-center gap-2">
                      % Relance prévu
                      {renderSort('reminderRate')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('avgReminders')}
                    className="px-6 py-4 text-center text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center justify-center gap-2">
                      Nb relance moy.
                      {renderSort('avgReminders')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('mandatsSigned')}
                    className="px-6 py-4 text-center text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center justify-center gap-2">
                      Mandats signés
                      {renderSort('mandatsSigned')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('start')}
                    className="px-6 py-4 text-left text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center gap-2">
                      Début
                      {renderSort('start')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('end')}
                    className="px-6 py-4 text-left text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center gap-2">
                      Fin
                      {renderSort('end')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('status')}
                    className="px-6 py-4 text-center text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center justify-center gap-2">
                      Statut
                      {renderSort('status')}
                    </div>
                  </th>
                  <th
                    onClick={() => toggleSort('postalCodes')}
                    className="px-6 py-4 text-left text-xs font-bold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                  >
                    <div className="flex items-center gap-2">
                      Zones
                      {renderSort('postalCodes')}
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sortedAgencies.length === 0 ? (
                  <tr>
                    <td colSpan={11} className="px-6 py-12 text-center">
                      <div className="flex flex-col items-center">
                        <span className="text-4xl mb-2">🔍</span>
                        <p className="text-gray-500 font-medium">Aucune agence trouvée</p>
                        <p className="text-sm text-gray-400 mt-1">Essayez de modifier vos filtres</p>
                      </div>
                    </td>
                  </tr>
                ) : (
                  sortedAgencies.map((ag, index) => {
                    const isEven = index % 2 === 0
                    const bgClass = ag.isIgnored
                      ? 'bg-red-50 hover:bg-red-100'
                      : isEven
                      ? 'bg-white hover:bg-blue-50'
                      : 'bg-gray-50 hover:bg-blue-50'

                    return (
                      <tr key={ag.id} className={`${bgClass} transition-all duration-150 border-l-4 ${ag.isIgnored ? 'border-red-500' : ag.isSuspended ? 'border-purple-500' : ag.end ? 'border-orange-400' : 'border-emerald-500'}`}>
                        <td className="px-6 py-4">
                          <input
                            type="checkbox"
                            checked={selectedIds.has(ag.id)}
                            onChange={e => toggleSelectOne(ag.id, e.target.checked)}
                            aria-label={`Sélectionner ${ag.name}`}
                            className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                          />
                        </td>
                        <td className="px-6 py-4">
                          <div className="flex items-center gap-3">
                            <div className="flex-shrink-0 w-10 h-10 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center text-white font-bold text-sm shadow-md">
                              {ag.name.charAt(0).toUpperCase()}
                            </div>
                            <div>
                              <p className="text-sm font-semibold text-gray-900">{ag.name}</p>
                            </div>
                          </div>
                        </td>
                        <td className="px-6 py-4 text-center">
                          {(() => {
                            const stats = contactStats.get(ag.rawName || '')
                            const count = stats?.leadsWithPhone || 0
                            if (count === 0) {
                              return <span className="text-gray-400 text-sm">-</span>
                            }
                            return <span className="text-sm font-medium text-gray-700">{count}</span>
                          })()}
                        </td>
                        <td className="px-6 py-4 text-center">
                          {(() => {
                            const stats = contactStats.get(ag.rawName || '')
                            if (!stats || stats.leadsWithPhone === 0) {
                              return <span className="text-gray-400 text-sm">-</span>
                            }
                            const rate = stats.contactRate
                            const colorClass = rate >= 70 ? 'bg-emerald-100 text-emerald-800 border-emerald-200' :
                                              rate >= 40 ? 'bg-yellow-100 text-yellow-800 border-yellow-200' :
                                              'bg-red-100 text-red-800 border-red-200'
                            return (
                              <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-bold ${colorClass} border shadow-sm`}
                                    title={`${stats.leadsContacted} / ${stats.leadsWithPhone} leads avec téléphone`}>
                                {rate.toFixed(1)}%
                              </span>
                            )
                          })()}
                        </td>
                        <td className="px-6 py-4 text-center">
                          {(() => {
                            const stats = contactStats.get(ag.rawName || '')
                            if (!stats || stats.leadsWithPhone === 0) {
                              return <span className="text-gray-400 text-sm">-</span>
                            }
                            const rate = stats.reminderRate
                            const colorClass = rate >= 50 ? 'bg-blue-100 text-blue-800 border-blue-200' :
                                              rate >= 20 ? 'bg-cyan-100 text-cyan-800 border-cyan-200' :
                                              'bg-gray-100 text-gray-800 border-gray-200'
                            return (
                              <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-bold ${colorClass} border shadow-sm`}
                                    title={`${stats.leadsWithReminder} / ${stats.leadsWithPhone} leads avec téléphone`}>
                                {rate.toFixed(1)}%
                              </span>
                            )
                          })()}
                        </td>
                        <td className="px-6 py-4 text-center">
                          {(() => {
                            const stats = contactStats.get(ag.rawName || '')
                            if (!stats || stats.leadsWithPhone === 0) {
                              return <span className="text-gray-400 text-sm">-</span>
                            }
                            const avg = stats.avgRemindersDone
                            const colorClass = avg >= 3 ? 'bg-purple-100 text-purple-800 border-purple-200' :
                                              avg >= 1 ? 'bg-indigo-100 text-indigo-800 border-indigo-200' :
                                              'bg-gray-100 text-gray-800 border-gray-200'
                            return (
                              <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-bold ${colorClass} border shadow-sm`}>
                                {avg.toFixed(1)}
                              </span>
                            )
                          })()}
                        </td>
                        <td className="px-6 py-4 text-center">
                          {(() => {
                            const stats = contactStats.get(ag.rawName || '')
                            if (!stats) {
                              return <span className="text-gray-400 text-sm">-</span>
                            }
                            const count = stats.mandatsSigned
                            if (count === 0) {
                              return <span className="text-gray-400 text-sm">0</span>
                            }
                            const colorClass = count >= 10 ? 'bg-green-100 text-green-800 border-green-200' :
                                              count >= 3 ? 'bg-lime-100 text-lime-800 border-lime-200' :
                                              'bg-amber-100 text-amber-800 border-amber-200'
                            return (
                              <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-bold ${colorClass} border shadow-sm`}>
                                {count}
                              </span>
                            )
                          })()}
                        </td>
                        <td className="px-6 py-4">
                          <div className="flex items-center gap-2">
                            <span className="text-sm text-gray-700 font-medium">
                              {ag.start ? ag.start.toLocaleDateString('fr-FR', { day: '2-digit', month: 'short', year: 'numeric' }) : '-'}
                            </span>
                          </div>
                        </td>
                        <td className="px-6 py-4">
                          {editingEndId === ag.id ? (
                            <div className="flex items-center gap-2">
                              <input
                                type="date"
                                value={editingEndValue}
                                onChange={(e) => setEditingEndValue(e.target.value)}
                                className="px-2 py-1 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                              />
                              <button
                                onClick={() => saveEndDate(ag.id)}
                                className="px-2 py-1 text-xs font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700"
                              >
                                Enregistrer
                              </button>
                              <button
                                onClick={cancelEditEndDate}
                                className="px-2 py-1 text-xs font-medium rounded-md bg-gray-100 text-gray-700 hover:bg-gray-200"
                              >
                                Annuler
                              </button>
                            </div>
                          ) : (
                            <div className="flex items-center gap-3">
                              <span className="text-sm text-gray-700 font-medium">
                                {ag.end ? ag.end.toLocaleDateString('fr-FR', { day: '2-digit', month: 'short', year: 'numeric' }) : '-'}
                              </span>
                              <div className="flex items-center gap-2">
                                <button
                                  onClick={() => beginEditEndDate(ag.id, ag.end)}
                                  className="px-2 py-1 text-xs font-medium rounded-md bg-gray-100 text-gray-700 hover:bg-gray-200"
                                >
                                  {ag.end ? 'Modifier' : 'Définir'}
                                </button>
                                {manualEndDates.has(ag.id) && (
                                  <button
                                    onClick={() => clearEndDate(ag.id)}
                                    className="px-2 py-1 text-xs font-medium rounded-md bg-red-100 text-red-700 hover:bg-red-200"
                                  >
                                    Effacer
                                  </button>
                                )}
                              </div>
                            </div>
                          )}
                        </td>
                        <td className="px-6 py-4 text-center">
                          {ag.isIgnored ? (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold bg-red-100 text-red-800 border border-red-200 shadow-sm">
                              <span>🚫</span>
                              Ignorée{ag.isIgnoredManual ? ' (manuel)' : ' (auto)'}
                            </span>
                          ) : ag.isSuspended ? (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold bg-purple-100 text-purple-800 border border-purple-200 shadow-sm">
                              <span>⏸️</span>
                              Suspendue
                            </span>
                          ) : ag.end ? (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold bg-orange-100 text-orange-800 border border-orange-200 shadow-sm">
                              <span>📋</span>
                              Résiliée{ag.isChurnedThisLatestMonth ? ' (ce mois)' : ''}
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold bg-emerald-100 text-emerald-800 border border-emerald-200 shadow-sm">
                              <span>✅</span>
                              Active
                            </span>
                          )}
                        </td>
                        <td className="px-6 py-4">
                          {ag.postalCodes && ag.postalCodes.length > 0 ? (
                            <div className="flex flex-wrap gap-1.5">
                              {ag.postalCodes.slice(0, 3).map((cp, idx) => (
                                <span key={idx} className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-semibold bg-gradient-to-r from-blue-100 to-cyan-100 text-blue-800 shadow-sm">
                                  📍 {cp}
                                </span>
                              ))}
                              {ag.postalCodes.length > 3 && (
                                <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium bg-gray-200 text-gray-700">
                                  +{ag.postalCodes.length - 3}
                                </span>
                              )}
                            </div>
                          ) : (
                            <span className="text-gray-400 text-sm italic">Aucune zone</span>
                          )}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
            {ignoredManualIds.size > 0 && (
              <div className="px-6 py-3 bg-gradient-to-r from-gray-50 to-slate-50 border-t border-gray-200">
                <p className="text-xs text-gray-600 flex items-center gap-2">
                  <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-red-100 text-red-600 font-bold text-xs">
                    {ignoredManualIds.size}
                  </span>
                  agence{ignoredManualIds.size > 1 ? 's' : ''} ignorée{ignoredManualIds.size > 1 ? 's' : ''} manuellement
                </p>
              </div>
            )}
          </div>
        )}
      </div>
        </>
      )}
    </div>
  )
}

export default StatsAgencesPage


