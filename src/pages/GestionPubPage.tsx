import { useState, useEffect, useMemo, useRef, useCallback, useTransition } from 'react'
import { createPortal } from 'react-dom'
import { cachedFetch } from '../lib/fetchCache'
import { cpLogements } from '../data/cpLogements'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  PieChart, Pie, Cell, ComposedChart, ReferenceLine
} from 'recharts'
import {
  TrendingUp, TrendingDown, DollarSign, Users, MapPin, Home, Building,
  Target, Award, AlertTriangle, ChevronDown, ChevronUp, Filter,
  Phone, FileCheck, Bell, BarChart3, Table, RefreshCw, Search, X,
  CheckCircle, XCircle, Zap, Eye
} from 'lucide-react'

// ── Toast notification system ──
interface Toast {
  id: number
  message: string
  type: 'success' | 'error' | 'info'
}

let toastId = 0
function ToastContainer({ toasts, onRemove }: { toasts: Toast[]; onRemove: (id: number) => void }) {
  return createPortal(
    <div className="fixed bottom-6 right-6 z-[9999] flex flex-col gap-2 pointer-events-none">
      {toasts.map(t => (
        <div
          key={t.id}
          className={`pointer-events-auto flex items-center gap-3 px-4 py-3 rounded-xl shadow-lg text-sm font-medium animate-slide-in-right backdrop-blur-sm border ${
            t.type === 'success' ? 'bg-emerald-50/95 text-emerald-800 border-emerald-200' :
            t.type === 'error' ? 'bg-red-50/95 text-red-800 border-red-200' :
            'bg-blue-50/95 text-blue-800 border-blue-200'
          }`}
        >
          {t.type === 'success' ? <CheckCircle className="w-4 h-4 text-emerald-500 shrink-0" /> :
           t.type === 'error' ? <XCircle className="w-4 h-4 text-red-500 shrink-0" /> :
           <Zap className="w-4 h-4 text-blue-500 shrink-0" />}
          <span>{t.message}</span>
          <button onClick={() => onRemove(t.id)} className="ml-2 text-gray-400 hover:text-gray-600 cursor-pointer">
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      ))}
    </div>,
    document.body
  )
}

const apiUrl = import.meta.env.VITE_API_URL || '/api'

// Helper : multiplicateur de période pour convertir budget mensuel en budget période
function nominalPeriodDays(period: string, nbMois: number): number {
  if (period === '5d') return 5
  if (period === '15d') return 15
  if (period === '30d') return 30
  if (period === '90d') return 90
  if (period === 'month') return new Date().getDate()
  return nbMois * 30.5
}

function periodMultiplier(period: string, nbMois: number): number {
  if (period === '5d') return 5 / 30
  if (period === '15d') return 15 / 30
  if (period === '90d') return 3
  if (period === '30d' || period === 'month') return 1
  return nbMois
}

// Types
// Normalisation pour matcher avec les données ads
function normalizeAdName(name: string): string {
  return name
    .replace(/^p\s*max\s*:\s*/i, '')
    .replace(/^leads-performance\s+max-\d*\s*/i, '')
    .replace(/\s*-\s*(copie|copy)\s*\d*/gi, '')
    .replace(/\s*-\s*new estim/gi, '')
    .replace(/\s*ad\s*group$/gi, '')
    .replace(/\s*ensemble de publicités$/gi, '')
    .replace(/\s*\(smart\)\s*$/gi, '')
    .replace(/^\?/, '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]/g, '')
    .trim()
}

interface AdMetrics {
  spend: number
  impressions: number
  clicks: number
  leads: number
  account_ids?: string[]
  first_stat_date?: string | null
}

interface AdsStatsResponse {
  meta: Record<string, AdMetrics>
  google: Record<string, AdMetrics>
}

interface AdsLastEditResponse {
  meta: Record<string, string>
  google: Record<string, string>
  metaCreations?: Record<string, string>
  googleCreations?: Record<string, string>
  metaStarts?: Record<string, string>
  googleStarts?: Record<string, string>
  metaAccounts?: Record<string, string>
  googleAccounts?: Record<string, string>
}

interface AdsEntry {
  raw_name: string
  spend: number
  leads: number
  impressions: number
  clicks: number
  daily_budget: number | null
  url: string | null
  id: string | null
  status: string | null
}

interface AdsEntriesResponse {
  meta: Record<string, AdsEntry[]>
  google: Record<string, AdsEntry[]>
}

interface MetaAdsetDetail {
  id: string
  name: string
  daily_budget: number
  status: string
  targeting: {
    age_min: number | null
    age_max: number | null
    locations: string[]
    interests: string[]
    custom_audiences: string[]
  }
  ads: {
    id: string
    name: string
    status: string
    titles: string[]
    bodies: string[]
    descriptions: string[]
    image_url: string | null
  }[]
}

interface GoogleCampaignDetail {
  id: string
  name: string
  daily_budget: number
  channel_type: string
  locations: string[]
  ad_groups: {
    name: string
    ads: {
      type: string
      status: string
      headlines: string[]
      descriptions: string[]
      final_urls: string[]
    }[]
  }[]
  asset_groups: {
    name: string
    headlines: string[]
    long_headlines: string[]
    descriptions: string[]
    images: string[]
  }[]
}

interface HistoryEvent {
  date: string
  source: 'meta' | 'google' | 'internal'
  type: string
  entity_name: string
  description: string
  details?: string
  actor?: string
}

interface AdsDetailResponse {
  meta: MetaAdsetDetail[]
  google: GoogleCampaignDetail[]
  history: HistoryEvent[]
}

interface AdsClientData {
  meta: AdMetrics
  google: AdMetrics
  metaAccountIds: string[]
  googleAccountIds: string[]
  metaLastEdit: string | null
  googleLastEdit: string | null
  metaCreatedDate: string | null
  googleCreatedDate: string | null
  metaFirstStatDate: string | null
  googleFirstStatDate: string | null
  matchedAdKeys: string[]  // normalized keys matched to this client
  earliestCampaignStart: string | null  // date de démarrage la plus ancienne parmi toutes les campagnes
}

interface ClientPubStats {
  id_client: string
  client_name: string
  locale: string
  id_gocardless: string | null
  estimateur_version: string
  // Stats alignées sync-agency-stats
  nb_leads_total: number
  nb_leads: number
  nb_leads_zone_total: number
  nb_leads_zone: number
  nb_leads_zone_phone_valid: number
  leads_contacted: number
  leads_with_reminder: number
  avg_reminders_done: number
  mandats_signed: number
  pct_lead_contacte: number
  pct_relance_prevu: number
  nombre_logements: number | null
  sector_postal_codes: string[]
  tarifs: { code_postal: string; tarif: string; places?: number }[]
  // Stats pub-stats spécifiques
  nb_leads_vendeur: number
  nb_leads_validated_phone: number
  pct_phone: number
  pct_validated_phone: number
  nb_apartments: number
  nb_houses: number
  pct_apartments: number
  pct_houses: number
  zone_size: number
  zone_size_category: string
  zone_type: string
  postal_codes: string[]
  budget_mensuel: number
  nb_mois: number
  budget_global: number
  start_date: string | null
  cpl: number | null
  cpl_12m: number | null
  first_lead_date: string
  last_lead_date: string
  leads_per_cp: Record<string, number>
}

interface PubStatsSummary {
  total_clients: number
  total_leads: number
  total_leads_total: number
  total_mandats_signed: number
  avg_pct_lead_contacte: number
  avg_pct_relance_prevu: number
  avg_cpl: number | null
  min_cpl: number | null
  max_cpl: number | null
  by_zone_type: { [key: string]: ClientPubStats[] }
  by_zone_size: { [key: string]: ClientPubStats[] }
  by_property_type: { [key: string]: ClientPubStats[] }
}

interface PubStatsResponse {
  clients: ClientPubStats[]
  summary: PubStatsSummary
}

// Couleurs pour les graphiques
const COLORS = {
  ville: '#3b82f6',
  campagne: '#22c55e',
  montagne: '#8b5cf6',
  littoral: '#06b6d4',
  périurbain: '#f59e0b',
  mixte: '#6b7280',
  petite: '#10b981',
  moyenne: '#f59e0b',
  grande: '#ef4444',
  apartments: '#6366f1',
  houses: '#ec4899'
}

const ZONE_LABELS: { [key: string]: string } = {
  ville: 'Ville',
  campagne: 'Campagne',
  montagne: 'Montagne',
  littoral: 'Littoral',
  périurbain: 'Périurbain',
  mixte: 'Mixte'
}

const SIZE_LABELS: { [key: string]: string } = {
  petite: 'Petite (<5 CP)',
  moyenne: 'Moyenne (5-15 CP)',
  grande: 'Grande (>15 CP)'
}

function GestionPubPage() {
  // Toast system
  const [toasts, setToasts] = useState<Toast[]>([])
  const showToast = useCallback((message: string, type: Toast['type'] = 'success') => {
    const id = ++toastId
    setToasts(prev => [...prev, { id, message, type }])
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000)
  }, [])
  const removeToast = useCallback((id: number) => {
    setToasts(prev => prev.filter(t => t.id !== id))
  }, [])

  const [stats, setStats] = useState<PubStatsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [sortField, setSortField] = useState<string>('nb_leads_total')
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')
  const [filterZoneType, setFilterZoneType] = useState<string>('all')
  const [filterZoneSize, setFilterZoneSize] = useState<string>('all')
  const [filterPropertyType, setFilterPropertyType] = useState<string>('all')
  const [filterAnciennete, setFilterAnciennete] = useState<string>('5d')
  const [filterLocale, setFilterLocale] = useState<string>('all')
  const [showFilters, setShowFilters] = useState(false)
  const [search, setSearch] = useState('')
  const [period, setPeriod] = useState<'all' | 'month' | '5d' | '15d' | '30d' | '90d'>('30d')
  const [activeTab, setActiveTab] = useState<'tableau' | 'statistiques' | 'rentabilite'>('rentabilite')
  const [adsRaw, setAdsRaw] = useState<AdsStatsResponse>({ meta: {}, google: {} })
  const [adsLastEdit, setAdsLastEdit] = useState<AdsLastEditResponse>({ meta: {}, google: {} })
  const [adsEntries, setAdsEntries] = useState<AdsEntriesResponse>({ meta: {}, google: {} })
  const [adsLastDelivery, setAdsLastDelivery] = useState<AdsLastEditResponse>({ meta: {}, google: {} })
  const [adMatchingOverrides, setAdMatchingOverrides] = useState<Record<string, string>>({})
  const [adAssignSearchMeta, setAdAssignSearchMeta] = useState('')
  const [adAssignSearchGoogle, setAdAssignSearchGoogle] = useState('')
  const [adAssignOpenMeta, setAdAssignOpenMeta] = useState(false)
  const [adAssignOpenGoogle, setAdAssignOpenGoogle] = useState(false)
  const adAssignMetaRef = useRef<HTMLInputElement>(null)
  const adAssignGoogleRef = useRef<HTMLInputElement>(null)
  const [expandedCats, setExpandedCats] = useState<Record<string, string | null>>({ cat1: null, cat2: null, cat3: null, cat4: null, cat5: null })
  const toggleExpandCat = useCallback((cat: string, clientId: string) => {
    setExpandedCats(prev => ({ ...prev, [cat]: prev[cat] === clientId ? null : clientId }))
  }, [])
  const [editingBudget, setEditingBudget] = useState<{ source: string; id: string; value: string } | null>(null)
  const [savingBudget, setSavingBudget] = useState<string | null>(null) // id en cours de sauvegarde
  const [detailModal, setDetailModal] = useState<{
    clientName: string
    stats: { cpl: number | null; metaCpl: number | null; googleCpl: number | null; margePct: number | null; cpl12m: number | null; spendPerDay: number; budgetMaxJour: number; budgetMensuel: number }
    loading: boolean
    data: AdsDetailResponse | null
    entriesById: Record<string, { spend: number; leads: number }> // id → spend/leads from adsEntries
  } | null>(null)
  const [detailModalSelected, setDetailModalSelected] = useState<string | null>(null)
  const [cplMonthlyData, setCplMonthlyData] = useState<{ month: string; budget: number; leads: number; cpl: number | null }[] | null>(null)
  const [cplMonthlyLoading, setCplMonthlyLoading] = useState(false)
  const [activeAlertTab, setActiveAlertTab] = useState<'cat1' | 'cat2' | 'cat3' | 'cat4' | 'cat5' | null>(null)
  const [pendingAlertTab, setPendingAlertTab] = useState<'cat1' | 'cat2' | 'cat3' | 'cat4' | 'cat5' | null>(null)
  const [alertTabAutoOpened, setAlertTabAutoOpened] = useState(false)
  const [isAlertTabPending, startAlertTabTransition] = useTransition()
  const switchAlertTab = useCallback((tab: 'cat1' | 'cat2' | 'cat3' | 'cat4' | 'cat5' | null) => {
    setPendingAlertTab(tab)
    startAlertTabTransition(() => {
      setActiveAlertTab(tab)
    })
  }, [])

  // Données calculées pour la modale détail
  const modalAllItems = useMemo(() => {
    const items: { source: 'meta' | 'google'; id: string; name: string; status: string; budget: number; spend: number; leads: number; cpl: number | null; type?: string }[] = []
    if (detailModal?.data?.meta) {
      for (const adset of detailModal.data.meta) {
        const e = detailModal.entriesById[adset.id]
        items.push({ source: 'meta', id: adset.id, name: adset.name, status: adset.status, budget: adset.daily_budget, spend: e?.spend || 0, leads: e?.leads || 0, cpl: e && e.leads > 0 ? e.spend / e.leads : null })
      }
    }
    if (detailModal?.data?.google) {
      for (const c of detailModal.data.google) {
        const e = detailModal.entriesById[c.id]
        items.push({ source: 'google', id: c.id, name: c.name, status: 'ACTIVE', budget: c.daily_budget, spend: e?.spend || 0, leads: e?.leads || 0, cpl: e && e.leads > 0 ? e.spend / e.leads : null, type: c.channel_type })
      }
    }
    return items
  }, [detailModal])
  const modalSelectedItem = modalAllItems.find(it => it.id === detailModalSelected) || null
  const modalSelectedMeta = detailModal?.data?.meta?.find(a => a.id === detailModalSelected) || null
  const modalSelectedGoogle = detailModal?.data?.google?.find(c => c.id === detailModalSelected) || null

  const cplMonthlyAverage = useMemo(() => {
    if (!cplMonthlyData) return null
    const withCpl = cplMonthlyData.filter(d => d.cpl !== null)
    if (withCpl.length === 0) return null
    return Math.round(withCpl.reduce((sum, d) => sum + d.cpl!, 0) / withCpl.length)
  }, [cplMonthlyData])

  const handleBudgetSave = async (source: 'meta' | 'google', id: string, newValue: string, entityName?: string, oldBudget?: number | null) => {
    const parsed = parseFloat(newValue.replace(',', '.'))
    if (isNaN(parsed) || parsed < 0) {
      setEditingBudget(null)
      return
    }
    // Pas de changement → annuler silencieusement
    if (oldBudget !== null && oldBudget !== undefined && Math.abs(parsed - oldBudget) < 0.005) {
      setEditingBudget(null)
      return
    }
    setSavingBudget(id)
    setEditingBudget(null)
    try {
      const r = await fetch(`${apiUrl}/ads-budget`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source, id, daily_budget: parsed, entity_name: entityName, old_budget: oldBudget })
      })
      const data = await r.json()
      if (!r.ok) {
        showToast(data.error || 'Échec de la mise à jour', 'error')
      } else {
        showToast(`Budget mis à jour : ${parsed.toFixed(2)}€/jour`, 'success')
        // Mettre à jour le budget localement dans adsEntries
        setAdsEntries(prev => {
          const updated = { ...prev }
          const sourceEntries = { ...updated[source] }
          for (const key of Object.keys(sourceEntries)) {
            sourceEntries[key] = sourceEntries[key].map(e =>
              e.id === id ? { ...e, daily_budget: parsed } : e
            )
          }
          updated[source] = sourceEntries
          return updated
        })
        // Note : on ne met plus à jour adsLastEdit côté client pour éviter
        // que la ligne disparaisse immédiatement des alertes.
        // Le filtre s'appliquera au prochain rechargement de la page.
        // Mettre à jour aussi dans detailModal.data si ouvert
        setDetailModal(prev => {
          if (!prev?.data) return prev
          const newData = { ...prev.data }
          if (source === 'meta') {
            newData.meta = newData.meta.map(a => a.id === id ? { ...a, daily_budget: parsed } : a)
          } else {
            newData.google = newData.google.map(c => c.id === id ? { ...c, daily_budget: parsed } : c)
          }
          return { ...prev, data: newData }
        })
      }
    } catch (e) {
      showToast(`Erreur réseau: ${(e as Error).message}`, 'error')
    } finally {
      setSavingBudget(null)
    }
  }

  const openDetailModal = async (
    clientName: string,
    stats: { cpl: number | null; metaCpl: number | null; googleCpl: number | null; margePct: number | null; cpl12m: number | null; spendPerDay: number; budgetMaxJour: number; budgetMensuel: number },
    adKeys: string[],
    clientId?: string
  ) => {
    // Collect IDs from entries + build spend/leads lookup by ID
    const metaIds: string[] = []
    const googleIds: string[] = []
    const entriesById: Record<string, { spend: number; leads: number }> = {}
    for (const key of adKeys) {
      if (adsEntries.meta[key]) {
        for (const e of adsEntries.meta[key]) {
          if (e.id) {
            metaIds.push(e.id)
            entriesById[e.id] = { spend: (entriesById[e.id]?.spend || 0) + e.spend, leads: (entriesById[e.id]?.leads || 0) + e.leads }
          }
        }
      }
      if (adsEntries.google[key]) {
        for (const e of adsEntries.google[key]) {
          if (e.id) {
            googleIds.push(e.id)
            entriesById[e.id] = { spend: (entriesById[e.id]?.spend || 0) + e.spend, leads: (entriesById[e.id]?.leads || 0) + e.leads }
          }
        }
      }
    }
    setDetailModal({ clientName, stats, loading: true, data: null, entriesById })
    setDetailModalSelected(null)
    setCplMonthlyData(null)
    setCplMonthlyLoading(true)
    try {
      const params = new URLSearchParams()
      if (metaIds.length) params.set('metaIds', [...new Set(metaIds)].join(','))
      if (googleIds.length) params.set('googleIds', [...new Set(googleIds)].join(','))
      if (adKeys.length) params.set('adKeys', adKeys.join(','))
      const [detailRes, cplRes] = await Promise.all([
        fetch(`${apiUrl}/ads-detail?${params}`),
        clientId && stats.budgetMensuel > 0
          ? fetch(`${apiUrl}/ads-cpl-monthly`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clientId, budgetMensuel: stats.budgetMensuel, months: 12 }) })
          : Promise.resolve(null)
      ])
      const data = await detailRes.json() as AdsDetailResponse
      setDetailModal(prev => prev ? { ...prev, loading: false, data } : null)
      // Auto-sélectionner le premier item
      const firstId = data.meta?.[0]?.id || data.google?.[0]?.id || (data.history?.length ? '__history__' : null)
      setDetailModalSelected(firstId)
      if (cplRes) {
        const cplData = await cplRes.json()
        setCplMonthlyData(Array.isArray(cplData) ? cplData : null)
      }
      setCplMonthlyLoading(false)
    } catch (err) {
      console.error('Error fetching ads detail:', err)
      setDetailModal(prev => prev ? { ...prev, loading: false } : null)
      setCplMonthlyLoading(false)
    }
  }

  // Helper : ouvrir la modale de détail pour n'importe quel client
  const openClientDetail = (client: ClientPubStats) => {
    const ads = adsPerClient.get(client.id_client)
    const adKeys = ads?.matchedAdKeys || []
    const totalSpend = ads ? ads.meta.spend + ads.google.spend : 0
    const budgetMaxJour = Math.round(client.budget_mensuel * 0.33 / 30.5 * 100) / 100
    const npd = nominalPeriodDays(period, client.nb_mois)
    const capDate = ads?.metaCreatedDate && ads?.googleCreatedDate
      ? (ads.metaCreatedDate < ads.googleCreatedDate ? ads.metaCreatedDate : ads.googleCreatedDate)
      : ads?.metaCreatedDate || ads?.googleCreatedDate || ads?.metaFirstStatDate || ads?.googleFirstStatDate || null
    const daysSinceCap = capDate ? Math.max(1, Math.ceil((Date.now() - new Date(capDate).getTime()) / (24 * 60 * 60 * 1000))) : npd
    const periodDays = Math.min(npd, daysSinceCap)
    const spendPerDay = periodDays > 0 ? totalSpend / periodDays : 0
    const pm = periodMultiplier(period, client.nb_mois)
    const budgetPeriode = client.budget_mensuel > 0 ? client.budget_mensuel * pm : 0
    const cpl = client.nb_leads_vendeur > 0 && budgetPeriode > 0 ? budgetPeriode / client.nb_leads_vendeur : null
    const metaCpl = ads && ads.meta.spend > 0 && ads.meta.leads > 0 ? ads.meta.spend / ads.meta.leads : null
    const googleCpl = ads && ads.google.spend > 0 && ads.google.leads > 0 ? ads.google.spend / ads.google.leads : null
    const margePct = budgetPeriode > 0 && totalSpend > 0 ? ((budgetPeriode - totalSpend) / budgetPeriode) * 100 : null
    openDetailModal(
      clientDisplayNames.get(client.id_client) || client.client_name,
      { cpl, metaCpl, googleCpl, margePct, cpl12m: client.cpl_12m, spendPerDay, budgetMaxJour: budgetMaxJour, budgetMensuel: client.budget_mensuel },
      adKeys,
      client.id_client
    )
  }

  // Helpers pour les overrides manuels de matching pub → client
  function saveAdOverride(normalizedName: string, idClient: string) {
    setAdMatchingOverrides(prev => ({ ...prev, [normalizedName]: idClient }))
    fetch(`${apiUrl}/ad-matching-overrides`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ normalized_name: normalizedName, id_client: idClient })
    }).catch(err => console.error('Erreur sauvegarde override:', err))
  }

  function deleteAdOverride(normalizedName: string) {
    setAdMatchingOverrides(prev => { const n = { ...prev }; delete n[normalizedName]; return n })
    fetch(`${apiUrl}/ad-matching-overrides`, {
      method: 'DELETE', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ normalized_name: normalizedName })
    }).catch(err => console.error('Erreur suppression override:', err))
  }

  // Helper : rendre le tableau détaillé Meta/Google pour une ligne dépliée d'alerte
  const renderExpandedEntries = (clientId: string, borderColor: string) => {
    const ads = adsPerClient.get(clientId)
    const adKeys = ads?.matchedAdKeys || []
    const isPaused = (e: AdsEntry) => e.status === 'PAUSED' || e.status === 'CAMPAIGN_PAUSED'
    const metaDetail: AdsEntry[] = []
    const googleDetail: AdsEntry[] = []
    for (const key of adKeys) {
      if (adsEntries.meta[key]) metaDetail.push(...adsEntries.meta[key].filter(e => !isPaused(e)))
      if (adsEntries.google[key]) googleDetail.push(...adsEntries.google[key].filter(e => !isPaused(e)))
    }

    const unmatchedAdKeys = globalUnmatchedAdKeys

    // Handler pour détacher une pub de ce client
    const handleDetach = (adKey: string) => {
      if (adMatchingOverrides[adKey]) {
        // C'était un override manuel → le supprimer
        deleteAdOverride(adKey)
      } else {
        // C'était un match auto → forcer "pas de match"
        saveAdOverride(adKey, '__none__')
      }
    }

    // Render une ligne campagne (Meta ou Google)
    const renderAdRow = (entry: AdsEntry, source: 'meta' | 'google', i: number) => {
      const entryAdKey = adKeys.find(k => adsEntries[source][k]?.some(e => e.raw_name === entry.raw_name && e.id === entry.id))
      const isOverride = entryAdKey ? !!adMatchingOverrides[entryAdKey] : false
      const cpl = entry.leads > 0 ? entry.spend / entry.leads : null
      const isMeta = source === 'meta'
      return (
        <div key={`${source}${i}`} className={`flex items-center gap-0 rounded-lg border ${isMeta ? 'border-blue-100' : 'border-orange-100'} bg-white text-xs overflow-hidden`}>
          {/* Barre de couleur à gauche */}
          <div className={`w-1 self-stretch shrink-0 ${isMeta ? 'bg-blue-500' : 'bg-orange-500'}`} />

          {/* Nom de la campagne + dates */}
          <div className="flex items-center gap-1.5 min-w-0 px-3 py-2 flex-1">
            {entry.url ? (
              <a href={entry.url} target="_blank" rel="noopener noreferrer" className={`font-medium ${isMeta ? 'text-blue-700' : 'text-orange-700'} hover:underline truncate`} onClick={ev => ev.stopPropagation()}>{entry.raw_name}</a>
            ) : (
              <span className="font-medium text-gray-800 truncate">{entry.raw_name}</span>
            )}
            {/* Nom du compte publicitaire */}
            {(() => {
              let accName: string | null = null
              // Source principale : adsLastEdit accounts (couvre tous les comptes, même sans dépenses)
              if (entryAdKey) {
                const accSrc = isMeta ? adsLastEdit.metaAccounts : adsLastEdit.googleAccounts
                accName = accSrc?.[entryAdKey] || null
              }
              // Fallback 1 : adsRaw account_ids
              if (!accName && entryAdKey) {
                const rawMetrics = adsRaw[source]?.[entryAdKey]
                const accId = rawMetrics?.account_ids?.[0]
                if (accId?.includes('|||')) accName = accId.split('|||')[0]
              }
              // Fallback 2 : adsPerClient (metaAccountIds / googleAccountIds)
              if (!accName && ads) {
                const accIds = isMeta ? ads.metaAccountIds : ads.googleAccountIds
                const first = accIds?.[0]
                if (first?.includes('|||')) accName = first.split('|||')[0]
              }
              if (!accName) return null
              return <span className={`text-[9px] px-1.5 py-px rounded shrink-0 ${isMeta ? 'bg-blue-50 text-blue-500' : 'bg-orange-50 text-orange-500'}`} title="Compte publicitaire">{accName}</span>
            })()}
            {isOverride && <span className="text-[9px] bg-purple-100 text-purple-600 rounded px-1 py-px shrink-0">manuel</span>}
            {/* Dates création / démarrage / dernière modif */}
            {entryAdKey && (() => {
              const creationSrc = isMeta ? adsLastEdit.metaCreations : adsLastEdit.googleCreations
              const editSrc = isMeta ? adsLastEdit.meta : adsLastEdit.google
              const startSrc = isMeta ? adsLastEdit.metaStarts : adsLastEdit.googleStarts
              const created = creationSrc?.[entryAdKey]
              const edited = editSrc?.[entryAdKey]
              const started = startSrc?.[entryAdKey]
              const fmtDate = (iso: string) => { const d = new Date(iso); return `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}` }
              if (!created && !edited && !started) return null
              const isFuture = started && new Date(started) > new Date()
              return (
                <span className="inline-flex items-center gap-1.5 text-[9px] text-gray-400 shrink-0 ml-1">
                  {created && <span title="Date de création">Créé {fmtDate(created)}</span>}
                  {started && <span title="Date de démarrage programmée" className={isFuture ? 'text-amber-500 font-semibold' : ''}>Début {fmtDate(started)}</span>}
                  {edited && <span title="Dernière modification">Modif {fmtDate(edited)}</span>}
                </span>
              )
            })()}
          </div>

          {/* KPIs compacts — blocs fixes */}
          <div className="flex items-center shrink-0">
            <div className="w-16 text-center py-2 border-l border-gray-100">
              <div className="text-[10px] text-gray-400 leading-none mb-0.5">Dépensé</div>
              <div className="font-semibold text-gray-800">{entry.spend.toFixed(0)}€</div>
            </div>
            <div className="w-14 text-center py-2 border-l border-gray-100">
              <div className="text-[10px] text-gray-400 leading-none mb-0.5">Leads</div>
              <div className="font-semibold text-gray-800">{entry.leads}</div>
            </div>
            <div className="w-16 text-center py-2 border-l border-gray-100">
              <div className="text-[10px] text-gray-400 leading-none mb-0.5">CPL</div>
              <div className={`font-bold ${cpl !== null ? (cpl > 45 ? 'text-red-600' : 'text-emerald-600') : 'text-gray-300'}`}>
                {cpl !== null ? `${cpl.toFixed(0)}€` : '—'}
              </div>
            </div>

            {/* Budget/jour — zone d'action principale */}
            <div className={`w-24 text-center py-2 border-l-2 ${isMeta ? 'border-blue-200 bg-blue-50/50' : 'border-orange-200 bg-orange-50/50'}`} onClick={ev => ev.stopPropagation()}>
              <div className="text-[10px] text-gray-400 leading-none mb-0.5">Budget/j</div>
              {savingBudget === entry.id ? (
                <div className="font-semibold text-gray-400 animate-pulse">...</div>
              ) : editingBudget?.id === entry.id ? (
                <input
                  type="text"
                  className={`w-16 mx-auto text-center border-2 ${isMeta ? 'border-blue-400' : 'border-orange-400'} rounded-md px-1 py-0.5 text-xs font-bold focus:outline-none`}
                  defaultValue={editingBudget.value}
                  autoFocus
                  onBlur={ev => handleBudgetSave(source, entry.id!, ev.target.value, entry.raw_name, entry.daily_budget)}
                  onKeyDown={ev => { if (ev.key === 'Enter') (ev.target as HTMLInputElement).blur(); if (ev.key === 'Escape') setEditingBudget(null) }}
                />
              ) : entry.id && entry.daily_budget !== null ? (
                <button
                  className={`font-bold text-sm ${isMeta ? 'text-blue-700 hover:bg-blue-100' : 'text-orange-700 hover:bg-orange-100'} rounded-md px-2 py-0.5 transition-colors cursor-pointer inline-flex items-center gap-0.5 mx-auto`}
                  onClick={() => setEditingBudget({ source, id: entry.id!, value: entry.daily_budget!.toFixed(2) })}
                  title="Cliquer pour modifier"
                >
                  {entry.daily_budget.toFixed(2)}€
                  <svg className="w-2.5 h-2.5 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" /></svg>
                </button>
              ) : (
                <div className="text-gray-300 font-medium">—</div>
              )}
            </div>

            {/* Détacher */}
            <div className="w-8 text-center py-2 border-l border-gray-100">
              {entryAdKey ? (
                <button
                  className="text-gray-300 hover:text-red-500 transition-colors cursor-pointer p-0.5 rounded hover:bg-red-50"
                  title="Détacher cette pub"
                  onClick={() => handleDetach(entryAdKey)}
                ><X className="w-3.5 h-3.5" /></button>
              ) : <span />}
            </div>
          </div>
        </div>
      )
    }

    return (
      <div className={`mt-1.5 ml-4 mr-1 mb-2 rounded-xl border ${borderColor} bg-gray-50/30 p-2 space-y-1.5`}>
        {(metaDetail.length > 0 || googleDetail.length > 0) ? (
          <div className="space-y-1.5">
            {metaDetail.map((entry, i) => renderAdRow(entry, 'meta', i))}
            {googleDetail.map((entry, i) => renderAdRow(entry, 'google', i))}
          </div>
        ) : (
          <p className="text-xs text-gray-400 italic py-3 text-center">Aucune campagne trouvée</p>
        )}
        {(() => {
          const unmatchedMeta = unmatchedAdKeys.filter(u => u.source === 'meta')
          const unmatchedGoogle = unmatchedAdKeys.filter(u => u.source === 'google')
          const searchNorm = (s: string) => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          const filteredMeta = unmatchedMeta.filter(u => !adAssignSearchMeta || searchNorm(u.rawName).includes(searchNorm(adAssignSearchMeta)))
          const filteredGoogle = unmatchedGoogle.filter(u => !adAssignSearchGoogle || searchNorm(u.rawName).includes(searchNorm(adAssignSearchGoogle)))
          return (unmatchedMeta.length > 0 || unmatchedGoogle.length > 0) ? (
            <div className="mt-2 pt-2 border-t border-gray-200 grid grid-cols-2 gap-3">
              {/* Meta selector */}
              <div>
                <label className="text-xs font-medium text-blue-700 mb-1 block">+ Meta ({unmatchedMeta.length})</label>
                {unmatchedMeta.length > 0 ? (
                  <>
                    <input
                      ref={adAssignMetaRef}
                      type="text"
                      className="w-full text-xs border border-blue-200 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-blue-400"
                      placeholder="Rechercher une campagne Meta..."
                      value={adAssignSearchMeta}
                      onChange={ev => setAdAssignSearchMeta(ev.target.value)}
                      onFocus={() => setAdAssignOpenMeta(true)}
                      onBlur={() => setTimeout(() => setAdAssignOpenMeta(false), 200)}
                    />
                    {adAssignOpenMeta && (filteredMeta.length > 0 || adAssignSearchMeta) && createPortal(
                      (() => {
                        const rect = adAssignMetaRef.current?.getBoundingClientRect()
                        if (!rect) return null
                        return (
                          <div
                            style={{ position: 'fixed', top: rect.bottom + 2, left: rect.left, width: rect.width, zIndex: 9999 }}
                            className="bg-white border border-blue-200 rounded shadow-lg max-h-48 overflow-y-auto"
                          >
                            {filteredMeta.length > 0 ? filteredMeta.map(u => (
                              <div
                                key={u.adKey}
                                className="px-2 py-1.5 text-xs hover:bg-blue-50 cursor-pointer border-b border-gray-50 truncate"
                                onMouseDown={ev => { ev.preventDefault(); saveAdOverride(u.adKey, clientId); setAdAssignSearchMeta(''); setAdAssignOpenMeta(false) }}
                              >
                                <span className="text-gray-800">{u.rawName}</span>
                                <span className="text-gray-400 ml-1">({u.spend.toFixed(0)}€)</span>
                              </div>
                            )) : (
                              <div className="px-2 py-2 text-xs text-gray-400 italic">Aucun résultat</div>
                            )}
                          </div>
                        )
                      })(),
                      document.body
                    )}
                  </>
                ) : (
                  <span className="text-xs text-gray-400 italic">Aucune pub Meta disponible</span>
                )}
              </div>
              {/* Google selector */}
              <div>
                <label className="text-xs font-medium text-orange-700 mb-1 block">+ Google ({unmatchedGoogle.length})</label>
                {unmatchedGoogle.length > 0 ? (
                  <>
                    <input
                      ref={adAssignGoogleRef}
                      type="text"
                      className="w-full text-xs border border-orange-200 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-orange-400"
                      placeholder="Rechercher une campagne Google..."
                      value={adAssignSearchGoogle}
                      onChange={ev => setAdAssignSearchGoogle(ev.target.value)}
                      onFocus={() => setAdAssignOpenGoogle(true)}
                      onBlur={() => setTimeout(() => setAdAssignOpenGoogle(false), 200)}
                    />
                    {adAssignOpenGoogle && (filteredGoogle.length > 0 || adAssignSearchGoogle) && createPortal(
                      (() => {
                        const rect = adAssignGoogleRef.current?.getBoundingClientRect()
                        if (!rect) return null
                        return (
                          <div
                            style={{ position: 'fixed', top: rect.bottom + 2, left: rect.left, width: rect.width, zIndex: 9999 }}
                            className="bg-white border border-orange-200 rounded shadow-lg max-h-48 overflow-y-auto"
                          >
                            {filteredGoogle.length > 0 ? filteredGoogle.map(u => (
                              <div
                                key={u.adKey}
                                className="px-2 py-1.5 text-xs hover:bg-orange-50 cursor-pointer border-b border-gray-50 truncate"
                                onMouseDown={ev => { ev.preventDefault(); saveAdOverride(u.adKey, clientId); setAdAssignSearchGoogle(''); setAdAssignOpenGoogle(false) }}
                              >
                                <span className="text-gray-800">{u.rawName}</span>
                                <span className="text-gray-400 ml-1">({u.spend.toFixed(0)}€)</span>
                              </div>
                            )) : (
                              <div className="px-2 py-2 text-xs text-gray-400 italic">Aucun résultat</div>
                            )}
                          </div>
                        )
                      })(),
                      document.body
                    )}
                  </>
                ) : (
                  <span className="text-xs text-gray-400 italic">Aucune pub Google disponible</span>
                )}
              </div>
            </div>
          ) : null
        })()}
      </div>
    )
  }

  useEffect(() => {
    const fetchAll = async () => {
      try {
        if (!stats) setLoading(true)
        else setRefreshing(true)
        const periodParam = period === 'all' ? '' : `?period=${period}`
        const [pubData, adsData, lastEditData, entriesData, lastDeliveryData, overridesData] = await Promise.all([
          cachedFetch<PubStatsResponse>(`${apiUrl}/pub-stats${periodParam}`),
          cachedFetch<AdsStatsResponse>(`${apiUrl}/ads-stats${periodParam}`).catch(() => ({ meta: {}, google: {} } as AdsStatsResponse)),
          cachedFetch<AdsLastEditResponse>(`${apiUrl}/ads-last-edit`).catch(() => ({ meta: {}, google: {} } as AdsLastEditResponse)),
          cachedFetch<AdsEntriesResponse>(`${apiUrl}/ads-entries${periodParam}`).catch(() => ({ meta: {}, google: {} } as AdsEntriesResponse)),
          cachedFetch<AdsLastEditResponse>(`${apiUrl}/ads-last-delivery`).catch(() => ({ meta: {}, google: {} } as AdsLastEditResponse)),
          cachedFetch<Array<{ normalized_name: string; id_client: string }>>(`${apiUrl}/ad-matching-overrides`).catch(() => [] as Array<{ normalized_name: string; id_client: string }>)
        ])
        setStats(pubData)
        setAdsRaw(adsData)
        setAdsLastEdit(lastEditData)
        setAdsEntries(entriesData)
        setAdsLastDelivery(lastDeliveryData)
        const overridesMap: Record<string, string> = {}
        for (const o of overridesData) overridesMap[o.normalized_name] = o.id_client
        setAdMatchingOverrides(overridesMap)
        setError(null)
      } catch (err) {
        console.error('Erreur lors de la récupération des stats pub:', err)
        setError('Impossible de charger les données publicitaires')
      } finally {
        setLoading(false)
        setRefreshing(false)
      }
    }

    fetchAll()
  }, [period])

  // Mapping pré-calculé : chaque adKey est assigné au MEILLEUR client uniquement (évite le double comptage)
  // Clé = id_client (et non client_name) pour différencier les homonymes (ex: plusieurs "Safti")
  const adsPerClient = useMemo(() => {
    if (!stats?.clients) return new Map<string, AdsClientData>()

    // Construire la liste des clients par clé normalisée (garder TOUS les clients, pas de dédup)
    const clientsByNormKey = new Map<string, Array<{ name: string; id: string }>>()
    for (const c of stats.clients) {
      const key = normalizeAdName(c.client_name)
      if (key.length < 2) continue
      if (!clientsByNormKey.has(key)) clientsByNormKey.set(key, [])
      clientsByNormKey.get(key)!.push({ name: c.client_name, id: c.id_client })
    }

    // Identifier les clés normalisées qui ont plusieurs clients (homonymes)
    const duplicateKeys = new Set<string>()
    for (const [key, clients] of clientsByNormKey) {
      if (clients.length > 1) duplicateKeys.add(key)
    }

    // Pour le matching classique (clients uniques), garder le premier par clé
    const uniqueClientsByKey = new Map<string, { name: string; id: string }>()
    for (const [key, clients] of clientsByNormKey) {
      if (!duplicateKeys.has(key)) uniqueClientsByKey.set(key, clients[0])
    }

    // Pour chaque adKey, trouver le MEILLEUR client non-dupliqué, ou la clé dupliquée correspondante
    // Calcule un score de correspondance entre deux clés normalisées
    function matchScore(adKey: string, refKey: string): number {
      if (adKey === refKey) return 10000

      // Prefix-based matching
      const minLen = Math.min(adKey.length, refKey.length)
      let common = 0
      while (common < minLen && adKey[common] === refKey[common]) common++

      // For short keys (2-3 chars like "iad"), only match if both keys are similar length
      // to avoid "iad" greedily matching "iadbentolila" (different iad agent)
      if (minLen <= 3) {
        const maxLen = Math.max(adKey.length, refKey.length)
        if (maxLen > minLen + 3) return 0 // trop de différence de longueur
        if (adKey.startsWith(refKey)) return 2000 + refKey.length
        if (refKey.startsWith(adKey)) return 1000 + adKey.length
        return 0
      }

      if (common >= 4) {
        if (adKey.startsWith(refKey)) return 2000 + refKey.length
        if (refKey.startsWith(adKey)) return 1000 + adKey.length
        if (common >= 8 && common >= minLen * 0.55) return common
      }

      // Substring containment fallback (ex: "bonnenfant" inside "cabinetbonnenfant")
      const shorter = adKey.length <= refKey.length ? adKey : refKey
      const longer = adKey.length <= refKey.length ? refKey : adKey
      if (shorter.length >= 8 && longer.includes(shorter)) {
        return 500 + shorter.length
      }

      return 0
    }

    function findBestClient(adKey: string): { type: 'unique'; id: string } | { type: 'duplicate'; normKey: string } | null {
      let bestResult: { type: 'unique'; id: string } | { type: 'duplicate'; normKey: string } | null = null
      let bestScore = 0

      // Chercher d'abord dans les clients uniques
      for (const [clientKey, client] of uniqueClientsByKey) {
        const score = matchScore(adKey, clientKey)
        if (score === 10000) return { type: 'unique', id: client.id }
        if (score > bestScore) {
          bestScore = score
          bestResult = { type: 'unique', id: client.id }
        }
      }

      // Chercher aussi dans les clés dupliquées
      for (const dupKey of duplicateKeys) {
        const score = matchScore(adKey, dupKey)
        if (score === 10000) return { type: 'duplicate', normKey: dupKey }
        if (score > bestScore) {
          bestScore = score
          bestResult = { type: 'duplicate', normKey: dupKey }
        }
      }

      return bestResult
    }

    // Second pass : pour les adKeys non matchées, chercher un client UNIQUE partageant un préfixe >= 5 chars
    function findUniqueRootClient(adKey: string): string | null {
      const candidates = new Set<string>()
      for (const [clientKey, client] of uniqueClientsByKey) {
        const minLen = Math.min(adKey.length, clientKey.length)
        let common = 0
        while (common < minLen && adKey[common] === clientKey[common]) common++
        // For short keys (2-3 chars), require similar length to avoid false matches
        if (minLen <= 3) {
          const maxLen = Math.max(adKey.length, clientKey.length)
          if (maxLen > minLen + 3) continue
          if (common >= minLen) candidates.add(client.id)
        } else if (common >= 5) {
          candidates.add(client.id)
        }
      }
      return candidates.size === 1 ? [...candidates][0] : null
    }

    // Agréger : clé = id_client
    const result = new Map<string, AdsClientData>()
    const unmatchedEntries: Array<{ adKey: string; source: 'meta' | 'google'; metrics: AdMetrics }> = []
    // Ads en attente d'assignation pour les clients homonymes
    const duplicateAds = new Map<string, Array<{ adKey: string; source: 'meta' | 'google'; metrics: AdMetrics }>>()

    function addToClient(clientId: string, source: 'meta' | 'google', metrics: AdMetrics, adKey: string) {
      if (!result.has(clientId)) {
        result.set(clientId, {
          meta: { spend: 0, impressions: 0, clicks: 0, leads: 0 },
          google: { spend: 0, impressions: 0, clicks: 0, leads: 0 },
          metaAccountIds: [],
          googleAccountIds: [],
          metaLastEdit: null,
          googleLastEdit: null,
          metaCreatedDate: null,
          googleCreatedDate: null,
          metaFirstStatDate: null,
          googleFirstStatDate: null,
          matchedAdKeys: [],
          earliestCampaignStart: null
        })
      }
      const entry = result.get(clientId)!
      entry[source].spend += metrics.spend
      entry[source].impressions += metrics.impressions
      entry[source].clicks += metrics.clicks
      entry[source].leads += metrics.leads
      if (metrics.account_ids) {
        const targetIds = source === 'meta' ? entry.metaAccountIds : entry.googleAccountIds
        for (const id of metrics.account_ids) {
          if (!targetIds.includes(id)) targetIds.push(id)
        }
      }
      const lastEditSource = source === 'meta' ? adsLastEdit.meta : adsLastEdit.google
      const editDate = lastEditSource[adKey]
      if (editDate) {
        const field = source === 'meta' ? 'metaLastEdit' : 'googleLastEdit'
        if (!entry[field] || new Date(editDate) > new Date(entry[field]!)) {
          entry[field] = editDate
        }
      }
      // Track creation date (MAX = adset le plus récent, la campagne active)
      const creationSource = source === 'meta' ? adsLastEdit.metaCreations : adsLastEdit.googleCreations
      const creationDate = creationSource?.[adKey]
      if (creationDate) {
        const createdField = source === 'meta' ? 'metaCreatedDate' : 'googleCreatedDate'
        if (!entry[createdField] || new Date(creationDate) > new Date(entry[createdField]!)) {
          entry[createdField] = creationDate
        }
      }
      // Track first stat date (MIN from Supabase first_stat_date)
      if (metrics.first_stat_date) {
        const statField = source === 'meta' ? 'metaFirstStatDate' : 'googleFirstStatDate'
        if (!entry[statField] || metrics.first_stat_date < entry[statField]!) {
          entry[statField] = metrics.first_stat_date
        }
      }
      // Track earliest campaign start date (date de programmation de diffusion)
      const startSource = source === 'meta' ? adsLastEdit.metaStarts : adsLastEdit.googleStarts
      const campaignStart = startSource?.[adKey]
      if (campaignStart) {
        if (!entry.earliestCampaignStart || new Date(campaignStart) < new Date(entry.earliestCampaignStart)) {
          entry.earliestCampaignStart = campaignStart
        }
      }
      if (!entry.matchedAdKeys.includes(adKey)) entry.matchedAdKeys.push(adKey)
    }

    // Pass 1 : matching (les overrides manuels ont priorité)
    for (const source of ['meta', 'google'] as const) {
      for (const [adKey, val] of Object.entries(adsRaw[source])) {
        // Override manuel : skip le matching auto
        if (adMatchingOverrides[adKey]) {
          if (adMatchingOverrides[adKey] !== '__none__') {
            addToClient(adMatchingOverrides[adKey], source, val, adKey)
          }
          continue
        }
        const best = findBestClient(adKey)
        if (!best) {
          unmatchedEntries.push({ adKey, source, metrics: val })
        } else if (best.type === 'unique') {
          addToClient(best.id, source, val, adKey)
        } else {
          // Ad matche un client homonyme → collecter pour traitement groupé
          if (!duplicateAds.has(best.normKey)) duplicateAds.set(best.normKey, [])
          duplicateAds.get(best.normKey)!.push({ adKey, source, metrics: val })
        }
      }
    }

    // Pass 2 : pour les non matchés, chercher un client unique par racine commune
    const stillUnmatched: typeof unmatchedEntries = []
    for (const entry of unmatchedEntries) {
      const fallbackId = findUniqueRootClient(entry.adKey)
      if (fallbackId) addToClient(fallbackId, entry.source, entry.metrics, entry.adKey)
      else stillUnmatched.push(entry)
    }
    if (stillUnmatched.length > 0) {
      console.log(`[adsPerClient] ${stillUnmatched.length} pubs non matchées:`, stillUnmatched.map(e => `${e.source}:${e.adKey} (${e.metrics.spend.toFixed(0)}€)`).slice(0, 15))
    }

    // Pass 3 : distribuer les ads des clients homonymes par account_id
    for (const [normKey, ads] of duplicateAds) {
      const clients = clientsByNormKey.get(normKey)!

      // Grouper les ads par nom de compte (partie avant |||)
      const adsByAccount = new Map<string, Array<{ adKey: string; source: 'meta' | 'google'; metrics: AdMetrics }>>()
      for (const ad of ads) {
        const accountName = ad.metrics.account_ids?.[0]?.split('|||')[0] || '__unknown__'
        if (!adsByAccount.has(accountName)) adsByAccount.set(accountName, [])
        adsByAccount.get(accountName)!.push(ad)
      }

      // Assigner chaque groupe de compte à un client différent (tri déterministe)
      const sortedAccounts = [...adsByAccount.keys()].sort()
      const sortedClients = [...clients].sort((a, b) => a.id.localeCompare(b.id))

      for (let i = 0; i < sortedAccounts.length; i++) {
        const clientIdx = Math.min(i, sortedClients.length - 1)
        const clientId = sortedClients[clientIdx].id
        for (const ad of adsByAccount.get(sortedAccounts[i])!) {
          addToClient(clientId, ad.source, ad.metrics, ad.adKey)
        }
      }
    }

    // Pass supplémentaire : injecter les dates de modif/création depuis adsLastEdit
    // pour les adKeys pas encore dans adsRaw (campagnes très récentes)
    const processedAdKeys = new Set<string>()
    for (const entry of result.values()) {
      for (const k of entry.matchedAdKeys) processedAdKeys.add(k)
    }
    const lastEditSources: Array<{ keys: Record<string, string>; source: 'meta' | 'google'; field: 'metaLastEdit' | 'googleLastEdit' }> = [
      { keys: adsLastEdit.meta, source: 'meta', field: 'metaLastEdit' },
      { keys: adsLastEdit.google, source: 'google', field: 'googleLastEdit' },
    ]
    const creationSources: Array<{ keys: Record<string, string> | undefined; source: 'meta' | 'google'; field: 'metaCreatedDate' | 'googleCreatedDate' }> = [
      { keys: adsLastEdit.metaCreations, source: 'meta', field: 'metaCreatedDate' },
      { keys: adsLastEdit.googleCreations, source: 'google', field: 'googleCreatedDate' },
    ]
    for (const { keys, field } of [...lastEditSources, ...creationSources]) {
      if (!keys) continue
      for (const [adKey, dateStr] of Object.entries(keys)) {
        if (processedAdKeys.has(adKey)) continue
        // Matcher cette clé à un client
        const match = findBestClient(adKey) || (() => { const id = findUniqueRootClient(adKey); return id ? { type: 'unique' as const, id } : null })()
        if (!match || match.type !== 'unique') continue
        const clientId = match.id
        if (!result.has(clientId)) continue
        const entry = result.get(clientId)!
        if (!entry[field] || new Date(dateStr) > new Date(entry[field]!)) {
          entry[field] = dateStr
        }
      }
    }
    // Pass supplémentaire : injecter les dates de démarrage de campagnes (starts)
    const startSources: Array<{ keys: Record<string, string> | undefined }> = [
      { keys: adsLastEdit.metaStarts },
      { keys: adsLastEdit.googleStarts },
    ]
    for (const { keys } of startSources) {
      if (!keys) continue
      for (const [adKey, dateStr] of Object.entries(keys)) {
        // Trouver le client associé à cette clé
        let clientId: string | null = null
        for (const [cid, entry] of result.entries()) {
          if (entry.matchedAdKeys.includes(adKey)) { clientId = cid; break }
        }
        if (!clientId) continue
        const entry = result.get(clientId)!
        if (!entry.earliestCampaignStart || new Date(dateStr) < new Date(entry.earliestCampaignStart)) {
          entry.earliestCampaignStart = dateStr
        }
      }
    }

    return result
  }, [stats?.clients, adsRaw, adsLastEdit, adMatchingOverrides])

  // Noms d'affichage enrichis pour les clients homonymes (ex: "Safti" → "Safti - Florian Fenech")
  const clientDisplayNames = useMemo(() => {
    const names = new Map<string, string>()
    if (!stats?.clients) return names

    // Détecter les noms en double
    const nameCount = new Map<string, number>()
    for (const c of stats.clients) {
      nameCount.set(c.client_name, (nameCount.get(c.client_name) || 0) + 1)
    }

    for (const c of stats.clients) {
      if ((nameCount.get(c.client_name) || 0) > 1) {
        // Client homonyme : différencier par département (2 premiers chiffres du code postal)
        const firstCp = c.postal_codes?.[0] || c.sector_postal_codes?.[0]
        const dept = firstCp ? firstCp.substring(0, 2) : null
        names.set(c.id_client, dept ? `${c.client_name} (${dept})` : c.client_name)
      } else {
        names.set(c.id_client, c.client_name)
      }
    }

    return names
  }, [stats?.clients])

  // Helper: récupérer les données ads pré-calculées pour un client (par id_client)
  // Pré-calcul global des pubs non assignées (évite de recalculer à chaque expand)
  const globalUnmatchedAdKeys = useMemo(() => {
    const allAssignedKeys = new Set<string>()
    for (const [, clientAds] of adsPerClient) {
      for (const k of clientAds.matchedAdKeys) allAssignedKeys.add(k)
    }
    for (const [k, v] of Object.entries(adMatchingOverrides)) {
      if (v !== '__none__') allAssignedKeys.add(k)
    }
    const result: Array<{ adKey: string; source: 'meta' | 'google'; spend: number; rawName: string }> = []
    for (const source of ['meta', 'google'] as const) {
      for (const [adKey, val] of Object.entries(adsRaw[source])) {
        if (!allAssignedKeys.has(adKey)) {
          const entries = adsEntries[source][adKey]
          const rawName = entries?.length ? entries.map(e => e.raw_name).join(', ') : adKey
          result.push({ adKey, source, spend: val.spend, rawName })
        }
      }
    }
    result.sort((a, b) => b.spend - a.spend)
    return result
  }, [adsPerClient, adMatchingOverrides, adsRaw, adsEntries])


  // Filtrage et tri des clients
  const filteredClients = useMemo(() => {
    if (!stats?.clients) return []

    let filtered = [...stats.clients]

    // Exclure les clients dont la date de démarrage est dans le futur
    const now = new Date()
    filtered = filtered.filter(c => !c.start_date || new Date(c.start_date) <= now)

    // Exclure les clients pas encore actifs : 0 dépenses ET au moins une campagne programmée dans le futur
    filtered = filtered.filter(c => {
      const ads = adsPerClient.get(c.id_client)
      if (!ads || !ads.matchedAdKeys.length) return true // pas de campagne matchée = on garde
      const totalSpend = ads.meta.spend + ads.google.spend
      if (totalSpend > 0) return true // a des dépenses = actif
      // 0 dépenses : vérifier si au moins une campagne est programmée dans le futur
      const hasFutureStart = ads.matchedAdKeys.some(k => {
        const metaStart = adsLastEdit.metaStarts?.[k]
        const googleStart = adsLastEdit.googleStarts?.[k]
        return (metaStart && new Date(metaStart) > now) || (googleStart && new Date(googleStart) > now)
      })
      if (hasFutureStart) return false // pas encore actif → masquer
      return true
    })

    // Recherche (inclut le displayName enrichi pour les homonymes)
    if (search.trim()) {
      const q = search.toLowerCase()
      filtered = filtered.filter(c => {
        const displayName = clientDisplayNames.get(c.id_client) || c.client_name
        return displayName.toLowerCase().includes(q) || c.client_name.toLowerCase().includes(q)
      })
    }

    // Filtres
    if (filterZoneType !== 'all') {
      filtered = filtered.filter(c => c.zone_type === filterZoneType)
    }
    if (filterZoneSize !== 'all') {
      filtered = filtered.filter(c => c.zone_size_category === filterZoneSize)
    }
    if (filterPropertyType !== 'all') {
      if (filterPropertyType === 'apartments') {
        filtered = filtered.filter(c => c.pct_apartments > 60)
      } else if (filterPropertyType === 'houses') {
        filtered = filtered.filter(c => c.pct_houses > 60)
      } else {
        filtered = filtered.filter(c => c.pct_apartments <= 60 && c.pct_houses <= 60)
      }
    }
    if (filterAnciennete === '5d') {
      const cutoff = new Date()
      cutoff.setDate(cutoff.getDate() - 5)
      filtered = filtered.filter(c => c.start_date && new Date(c.start_date) <= cutoff)
    } else if (filterAnciennete !== 'all') {
      const minMonths = parseInt(filterAnciennete)
      filtered = filtered.filter(c => c.nb_mois >= minMonths)
    }
    if (filterLocale !== 'all') {
      filtered = filtered.filter(c => (c.locale || 'fr') === filterLocale)
    }

    // Helper pour calculer les valeurs dérivées (CPL vendeurs, CPL zone, marge %)
    const computedVal = (c: ClientPubStats, field: string): number | null => {
      const pm = periodMultiplier(period, c.nb_mois)
      const bp = c.budget_mensuel > 0 ? c.budget_mensuel * pm : 0
      if (field === 'cpl_vendeurs') return c.nb_leads_vendeur > 0 && bp > 0 ? bp / c.nb_leads_vendeur : null
      if (field === 'cpl_zone') return c.nb_leads_zone_total > 0 && bp > 0 ? bp / c.nb_leads_zone_total : null
      if (field === 'marge_pct') {
        const ads = adsPerClient.get(c.id_client)
        const totalSpend = ads ? ads.meta.spend + ads.google.spend : 0
        return bp > 0 && totalSpend > 0 ? ((bp - totalSpend) / bp) * 100 : null
      }
      if (field === 'total_ads') {
        const ads = adsPerClient.get(c.id_client)
        return ads ? ads.meta.spend + ads.google.spend : 0
      }
      if (field === 'fb_ads') {
        const ads = adsPerClient.get(c.id_client)
        return ads ? ads.meta.spend : 0
      }
      if (field === 'google_ads') {
        const ads = adsPerClient.get(c.id_client)
        return ads ? ads.google.spend : 0
      }
      if (field === 'leads_per_cp') {
        return c.zone_size > 0 ? c.nb_leads_total / c.zone_size : null
      }
      if (field === 'tx_mandat') {
        return c.nb_leads_total > 0 ? (c.mandats_signed / c.nb_leads_total) * 100 : null
      }
      if (field === 'cpl_utile') {
        const nbValidated = c.nb_leads_validated_phone || 0
        return nbValidated > 0 && c.budget_mensuel > 0 && c.cpl ? c.cpl * c.nb_leads_total / nbValidated : null
      }
      return null
    }

    const computedFields = ['cpl_vendeurs', 'cpl_zone', 'marge_pct', 'total_ads', 'fb_ads', 'google_ads', 'leads_per_cp', 'tx_mandat', 'cpl_utile']

    // Tri
    filtered.sort((a, b) => {
      let aVal: number | string | null
      let bVal: number | string | null

      if (computedFields.includes(sortField)) {
        aVal = computedVal(a, sortField)
        bVal = computedVal(b, sortField)
      } else {
        aVal = (a as any)[sortField]
        bVal = (b as any)[sortField]
      }

      // Gérer les valeurs nulles
      if (aVal === null || aVal === undefined) aVal = sortDirection === 'desc' ? -Infinity : Infinity
      if (bVal === null || bVal === undefined) bVal = sortDirection === 'desc' ? -Infinity : Infinity

      if (typeof aVal === 'string') {
        return sortDirection === 'asc'
          ? aVal.localeCompare(bVal as string)
          : (bVal as string).localeCompare(aVal)
      }

      return sortDirection === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number)
    })

    return filtered
  }, [stats, sortField, sortDirection, filterZoneType, filterZoneSize, filterPropertyType, filterAnciennete, filterLocale, search, period, adsPerClient, adsLastEdit, clientDisplayNames])

  // Alertes : comptes à travailler (onglet Performances)
  const alertCategories = useMemo(() => {
    if (!filteredClients.length) return { cat1: [], cat2: [], cat3: [], cat4: [], cat5: [] }

    const pm = (c: ClientPubStats) => periodMultiplier(period, c.nb_mois)

    type CpFlag = { cp: string; logements: number; leadsReels: number; densite: number; densiteMoyenne: number; ecartPct: number }
    type AlertClient = { client: ClientPubStats; cplVendeurs: number | null; margePct: number | null; metaCpl: number | null; googleCpl: number | null; cplZone: number | null; bestChannel?: 'meta' | 'google'; flaggedCps?: CpFlag[] }

    const enriched: AlertClient[] = filteredClients.map(c => {
      const ads = adsPerClient.get(c.id_client)
      const budgetPeriode = c.budget_mensuel > 0 ? c.budget_mensuel * pm(c) : 0
      const totalSpend = ads ? ads.meta.spend + ads.google.spend : 0
      const cplVendeurs = c.nb_leads_vendeur > 0 && budgetPeriode > 0 ? budgetPeriode / c.nb_leads_vendeur : null
      const margePct = budgetPeriode > 0 && totalSpend > 0 ? ((budgetPeriode - totalSpend) / budgetPeriode) * 100 : null
      const metaCpl = ads && ads.meta.spend > 0 && ads.meta.leads > 0 ? ads.meta.spend / ads.meta.leads : null
      const googleCpl = ads && ads.google.spend > 0 && ads.google.leads > 0 ? ads.google.spend / ads.google.leads : null
      const cplZone = c.nb_leads_zone_total > 0 && budgetPeriode > 0 ? budgetPeriode / c.nb_leads_zone_total : null
      return { client: c, cplVendeurs, margePct, metaCpl, googleCpl, cplZone }
    })

    // Cat 1 : CPL > 45 ET marge > 67% ET CPL 12m >= 45 → augmenter budget pub
    // (le filtre isNotRecentlyModified est appliqué dans le return ci-dessous)
    const cat1 = enriched.filter(e => e.cplVendeurs !== null && e.cplVendeurs > 45 && e.margePct !== null && e.margePct > 67 && (e.client.cpl_12m === null || e.client.cpl_12m >= 45))

    // Cat 2 : Un canal CPL < 50% de l'autre (les deux avec du spend), sauf si les deux CPL < 21€
    const cat2 = enriched
      .filter(e => e.metaCpl !== null && e.googleCpl !== null)
      .filter(e => !(e.metaCpl! < 21 && e.googleCpl! < 21))
      .filter(e => {
        const ratio = e.metaCpl! / e.googleCpl!
        return ratio < 0.5 || ratio > 2
      })
      .map(e => ({
        ...e,
        bestChannel: (e.metaCpl! < e.googleCpl! ? 'meta' : 'google') as 'meta' | 'google'
      }))

    // Cat 3 : CPL > 45 ET marge < 68% → revoir ciblage
    const cat3 = enriched.filter(e => e.cplVendeurs !== null && e.cplVendeurs > 45 && e.margePct !== null && e.margePct < 68)

    // Cat 4 : Gros CP sous-performants — densité de leads réelle < 50% de la moyenne du client
    // Basé sur les leads réels par CP (leads_per_cp) et les logements effectifs (pondérés par places)
    // Places totales par CP : >80k→5, >60k→4, >40k→3, >30k→2, sinon 1
    const placesTotalesCp = (logements: number) => logements > 80000 ? 5 : logements > 60000 ? 4 : logements > 40000 ? 3 : logements > 30000 ? 2 : 1
    const cat4: AlertClient[] = []
    for (const e of enriched) {
      if (e.client.nb_leads_vendeur < 3) continue // pas assez de leads pour être significatif
      const cps = e.client.postal_codes
      if (cps.length < 2) continue // besoin d'au moins 2 CP pour comparer

      const leadsPerCp = e.client.leads_per_cp || {}

      // Places du client par CP depuis les tarifs
      const clientPlaces = new Map<string, number>()
      for (const t of e.client.tarifs) {
        const p = t.places || 1
        clientPlaces.set(t.code_postal, (clientPlaces.get(t.code_postal) || 0) + p)
      }

      // Calculer la densité (leads / 10k logements effectifs) par CP
      const cpDensities = cps
        .map(cp => {
          const logementsRaw = cpLogements[cp] || 0
          if (logementsRaw < 5000) return null // ignorer les petits CP
          const totalPlaces = placesTotalesCp(logementsRaw)
          const nbPlacesClient = clientPlaces.get(cp) || 1
          const logementsEffectifs = Math.round(logementsRaw * nbPlacesClient / totalPlaces)
          const leads = leadsPerCp[cp] || 0
          const densite = leads / (logementsEffectifs / 10000)
          return { cp, logementsRaw, logementsEffectifs, leads, densite, nbPlacesClient, totalPlaces }
        })
        .filter((c): c is NonNullable<typeof c> => c !== null)

      if (cpDensities.length < 2) continue

      // Densité moyenne du client (pondérée par les logements effectifs)
      const totalLeads = cpDensities.reduce((s, c) => s + c.leads, 0)
      const totalLogementsEff = cpDensities.reduce((s, c) => s + c.logementsEffectifs, 0)
      const densiteMoyenne = totalLeads / (totalLogementsEff / 10000)
      if (densiteMoyenne <= 0) continue

      // Flaguer les gros CP (> 10k logements effectifs) avec une densité < 50% de la moyenne
      const flagged: CpFlag[] = []
      for (const { cp, logementsEffectifs, leads, densite } of cpDensities) {
        if (logementsEffectifs <= 10000) continue
        const ecartPct = ((densite - densiteMoyenne) / densiteMoyenne) * 100
        if (ecartPct < -50) {
          flagged.push({
            cp,
            logements: logementsEffectifs,
            leadsReels: leads,
            densite: Math.round(densite * 10) / 10,
            densiteMoyenne: Math.round(densiteMoyenne * 10) / 10,
            ecartPct: Math.round(ecartPct)
          })
        }
      }
      if (flagged.length > 0) {
        // Trier par écart décroissant (les plus sous-performants d'abord)
        flagged.sort((a, b) => a.ecartPct - b.ecartPct)
        cat4.push({ ...e, flaggedCps: flagged })
      }
    }

    // Filtrer : ne garder que les comptes non modifiés/créés depuis > 10 jours
    const tenDaysAgo = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000)
    const isNotRecentlyModified = (e: AlertClient): boolean => {
      const ads = adsPerClient.get(e.client.id_client)
      if (!ads) return true
      // Dates depuis adsPerClient (matching via adsRaw)
      const dates = [ads.metaLastEdit, ads.googleLastEdit, ads.metaCreatedDate, ads.googleCreatedDate].filter(Boolean) as string[]
      // Aussi vérifier directement dans adsLastEdit pour les campagnes pas encore dans adsRaw
      for (const key of ads.matchedAdKeys) {
        if (adsLastEdit.meta[key]) dates.push(adsLastEdit.meta[key])
        if (adsLastEdit.google[key]) dates.push(adsLastEdit.google[key])
        if (adsLastEdit.metaCreations?.[key]) dates.push(adsLastEdit.metaCreations[key])
        if (adsLastEdit.googleCreations?.[key]) dates.push(adsLastEdit.googleCreations[key])
      }
      return !dates.some(d => new Date(d) >= tenDaysAgo)
    }

    // Cat 5 : Comptes qui ne diffusent pas (budget actif mais pas de dépense récente)
    // Meta : pas de dépense depuis 48h — Google : pas de dépense depuis 6 jours
    type StaleChannel = { channel: 'meta' | 'google'; lastDelivery: string | null; daysSince: number }
    type AlertClientCat5 = AlertClient & { staleChannels: StaleChannel[] }
    const cat5: AlertClientCat5[] = []
    const now48h = Date.now() - 48 * 60 * 60 * 1000
    const now6d = Date.now() - 6 * 24 * 60 * 60 * 1000

    const nowDate = new Date()
    for (const e of enriched) {
      const ads = adsPerClient.get(e.client.id_client)
      if (!ads) continue
      const adKeys = ads.matchedAdKeys
      if (!adKeys.length) continue

      const staleChannels: StaleChannel[] = []

      // Vérifier Meta : a-t-on un adset Meta avec budget > 0 ?
      const hasMetaBudget = adKeys.some(k => {
        const entries = adsEntries.meta[k]
        return entries?.some(entry => entry.daily_budget !== null && entry.daily_budget > 0)
      })
      // Exclure les campagnes Meta programmées dans le futur (pas encore démarrées)
      const metaAllFuture = hasMetaBudget && adKeys.every(k => {
        const startDate = adsLastEdit.metaStarts?.[k]
        return startDate && new Date(startDate) > nowDate
      })
      if (hasMetaBudget && !metaAllFuture) {
        // Trouver la dernière date de diffusion Meta pour cet ensemble de clés
        let latestMeta: string | null = null
        for (const k of adKeys) {
          const d = adsLastDelivery.meta[k]
          if (d && (!latestMeta || d > latestMeta)) latestMeta = d
        }
        const lastTs = latestMeta ? new Date(latestMeta).getTime() : 0
        if (lastTs < now48h) {
          const daysSince = latestMeta ? Math.floor((Date.now() - lastTs) / (24 * 60 * 60 * 1000)) : 999
          staleChannels.push({ channel: 'meta', lastDelivery: latestMeta, daysSince })
        }
      }

      // Vérifier Google : a-t-on une campagne Google avec budget > 0 ?
      const hasGoogleBudget = adKeys.some(k => {
        const entries = adsEntries.google[k]
        return entries?.some(entry => entry.daily_budget !== null && entry.daily_budget > 0)
      })
      // Exclure les campagnes Google programmées dans le futur
      const googleAllFuture = hasGoogleBudget && adKeys.every(k => {
        const startDate = adsLastEdit.googleStarts?.[k]
        return startDate && new Date(startDate) > nowDate
      })
      if (hasGoogleBudget && !googleAllFuture) {
        let latestGoogle: string | null = null
        for (const k of adKeys) {
          const d = adsLastDelivery.google[k]
          if (d && (!latestGoogle || d > latestGoogle)) latestGoogle = d
        }
        const lastTs = latestGoogle ? new Date(latestGoogle).getTime() : 0
        if (lastTs < now6d) {
          const daysSince = latestGoogle ? Math.floor((Date.now() - lastTs) / (24 * 60 * 60 * 1000)) : 999
          staleChannels.push({ channel: 'google', lastDelivery: latestGoogle, daysSince })
        }
      }

      if (staleChannels.length > 0) {
        cat5.push({ ...e, staleChannels })
      }
    }

    // Tri par CPL décroissant (plus grand en premier)
    const sortByCplDesc = <T extends { cplVendeurs: number | null }>(arr: T[]) =>
      arr.sort((a, b) => (b.cplVendeurs ?? 0) - (a.cplVendeurs ?? 0))

    // Tri cat5 par nombre de jours sans diffusion décroissant
    cat5.sort((a, b) => {
      const maxA = Math.max(...a.staleChannels.map(c => c.daysSince))
      const maxB = Math.max(...b.staleChannels.map(c => c.daysSince))
      return maxB - maxA
    })

    return {
      cat1: sortByCplDesc(cat1.filter(isNotRecentlyModified)),
      cat2: sortByCplDesc(cat2.filter(isNotRecentlyModified)),
      cat3: sortByCplDesc(cat3.filter(isNotRecentlyModified)),
      cat4: sortByCplDesc(cat4.filter(isNotRecentlyModified)),
      // Filtrer cat5 : retirer les comptes modifiés depuis < 5 jours
      cat5: cat5.filter(e => {
        const ads = adsPerClient.get(e.client.id_client)
        if (!ads) return true
        const fiveDaysAgo = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000)
        const dates: string[] = []
        for (const key of ads.matchedAdKeys) {
          if (adsLastEdit.meta[key]) dates.push(adsLastEdit.meta[key])
          if (adsLastEdit.google[key]) dates.push(adsLastEdit.google[key])
          if (adsLastEdit.metaCreations?.[key]) dates.push(adsLastEdit.metaCreations[key])
          if (adsLastEdit.googleCreations?.[key]) dates.push(adsLastEdit.googleCreations[key])
        }
        return !dates.some(d => new Date(d) >= fiveDaysAgo)
      })
    }
  }, [filteredClients, adsPerClient, period, adsEntries, adsLastDelivery, adsLastEdit])

  // Pré-calcul des métriques par client pour les panneaux d'alertes (évite les recalculs dans le JSX)
  const alertClientMetrics = useMemo(() => {
    const map = new Map<string, { totalSpend: number; budgetPubMaxJour: number; spendPerDay: number; configuredDailyBudget: number }>()
    const allClients = [
      ...alertCategories.cat1.map(e => e.client),
      ...alertCategories.cat2.map(e => e.client),
      ...alertCategories.cat3.map(e => e.client),
      ...alertCategories.cat4.map(e => e.client),
      ...alertCategories.cat5.map(e => e.client),
    ]
    for (const client of allClients) {
      if (map.has(client.id_client)) continue
      const ads = adsPerClient.get(client.id_client)
      const totalSpend = ads ? ads.meta.spend + ads.google.spend : 0
      const budgetPubMaxJour = Math.round(client.budget_mensuel * 0.33 / 30.5 * 100) / 100
      const npd = nominalPeriodDays(period, client.nb_mois)
      const adKeys = ads?.matchedAdKeys || []
      let configuredDailyBudget = 0
      for (const key of adKeys) {
        if (adsEntries.meta[key]) for (const ae of adsEntries.meta[key]) if (ae.daily_budget) configuredDailyBudget += ae.daily_budget
        if (adsEntries.google[key]) for (const ae of adsEntries.google[key]) if (ae.daily_budget) configuredDailyBudget += ae.daily_budget
      }
      const createdDates = [ads?.metaCreatedDate, ads?.googleCreatedDate].filter(Boolean) as string[]
      const createdDate = createdDates.length > 0 ? createdDates.reduce((a, b) => a < b ? a : b) : null
      const historyDates = [ads?.metaFirstStatDate, ads?.googleFirstStatDate].filter(Boolean) as string[]
      const historyDate = historyDates.length > 0 ? historyDates.reduce((a, b) => a < b ? a : b) : null
      const capDate = createdDate || historyDate
      const daysSinceCap = capDate ? Math.max(1, Math.ceil((Date.now() - new Date(capDate).getTime()) / (24 * 60 * 60 * 1000))) : npd
      const periodDays = Math.min(npd, daysSinceCap)
      const spendPerDay = periodDays > 0 ? totalSpend / periodDays : 0
      map.set(client.id_client, { totalSpend, budgetPubMaxJour, spendPerDay, configuredDailyBudget })
    }
    return map
  }, [alertCategories, adsPerClient, period, adsEntries])

  // Auto-ouvrir le premier onglet d'alertes qui a des lignes
  useEffect(() => {
    if (alertTabAutoOpened) return
    const order: Array<'cat5' | 'cat1' | 'cat2' | 'cat3' | 'cat4'> = ['cat5', 'cat2', 'cat4', 'cat1', 'cat3']
    for (const key of order) {
      if (alertCategories[key]?.length > 0) {
        setActiveAlertTab(key)
        setPendingAlertTab(key)
        setAlertTabAutoOpened(true)
        return
      }
    }
  }, [alertCategories, alertTabAutoOpened])

  // Clients filtrés par ancienneté pour l'onglet Statistiques
  const statsClients = useMemo(() => {
    if (!stats?.clients) return []
    let filtered = stats.clients
    if (filterAnciennete === '5d') {
      const cutoff = new Date()
      cutoff.setDate(cutoff.getDate() - 5)
      filtered = filtered.filter(c => c.start_date && new Date(c.start_date) <= cutoff)
    } else if (filterAnciennete !== 'all') {
      const minMonths = parseInt(filterAnciennete)
      filtered = filtered.filter(c => c.nb_mois >= minMonths)
    }
    if (filterLocale !== 'all') {
      filtered = filtered.filter(c => (c.locale || 'fr_FR') === filterLocale)
    }
    return filtered
  }, [stats, filterAnciennete, filterLocale])

  // Données pour les graphiques
  const zoneTypeData = useMemo(() => {
    if (!statsClients.length) return []

    const grouped = new Map<string, { count: number; leads: number; leadsTotal: number; totalCPL: number; cplCount: number; totalPctPhone: number }>()

    statsClients.forEach(c => {
      const key = c.zone_type
      if (!grouped.has(key)) {
        grouped.set(key, { count: 0, leads: 0, leadsTotal: 0, totalCPL: 0, cplCount: 0, totalPctPhone: 0 })
      }
      const g = grouped.get(key)!
      g.count++
      g.leads += c.nb_leads
      g.leadsTotal += c.nb_leads_total
      g.totalPctPhone += c.pct_phone
      if (c.cpl !== null && c.cpl > 0) {
        g.totalCPL += c.cpl
        g.cplCount++
      }
    })

    return Array.from(grouped.entries()).map(([zone, data]) => ({
      name: ZONE_LABELS[zone] || zone,
      clients: data.count,
      leads: data.leads,
      cpl: data.cplCount > 0 ? Math.round((data.totalCPL / data.cplCount) * 100) / 100 : 0,
      avgPctPhone: data.count > 0 ? Math.round((data.totalPctPhone / data.count) * 10) / 10 : 0,
      fill: COLORS[zone as keyof typeof COLORS] || COLORS.mixte
    })).sort((a, b) => b.leads - a.leads)
  }, [statsClients])

  const zoneSizeData = useMemo(() => {
    if (!statsClients.length) return []

    const grouped = new Map<string, { count: number; leads: number; totalCPL: number; cplCount: number; totalPctPhone: number }>()

    statsClients.forEach(c => {
      const key = c.zone_size_category
      if (!grouped.has(key)) {
        grouped.set(key, { count: 0, leads: 0, totalCPL: 0, cplCount: 0, totalPctPhone: 0 })
      }
      const g = grouped.get(key)!
      g.count++
      g.leads += c.nb_leads
      g.totalPctPhone += c.pct_phone
      if (c.cpl !== null && c.cpl > 0) {
        g.totalCPL += c.cpl
        g.cplCount++
      }
    })

    return Array.from(grouped.entries()).map(([size, data]) => ({
      name: SIZE_LABELS[size] || size,
      clients: data.count,
      leads: data.leads,
      cpl: data.cplCount > 0 ? Math.round((data.totalCPL / data.cplCount) * 100) / 100 : 0,
      avgPctPhone: data.count > 0 ? Math.round((data.totalPctPhone / data.count) * 10) / 10 : 0,
      fill: COLORS[size as keyof typeof COLORS] || COLORS.mixte
    }))
  }, [statsClients])

  const propertyTypeData = useMemo(() => {
    if (!statsClients.length) return []

    const apartments = statsClients.filter(c => c.pct_apartments > 60)
    const houses = statsClients.filter(c => c.pct_houses > 60)
    const mixed = statsClients.filter(c => c.pct_apartments <= 60 && c.pct_houses <= 60)

    const calcAvgCPL = (clients: ClientPubStats[]) => {
      const withCPL = clients.filter(c => c.cpl !== null && c.cpl > 0)
      if (withCPL.length === 0) return 0
      return Math.round((withCPL.reduce((s, c) => s + (c.cpl || 0), 0) / withCPL.length) * 100) / 100
    }
    const calcAvgPctPhone = (clients: ClientPubStats[]) => {
      if (clients.length === 0) return 0
      return Math.round((clients.reduce((s, c) => s + c.pct_phone, 0) / clients.length) * 10) / 10
    }

    return [
      {
        name: 'Majorité Appartements',
        clients: apartments.length,
        leads: apartments.reduce((s, c) => s + c.nb_leads, 0),
        cpl: calcAvgCPL(apartments),
        avgPctPhone: calcAvgPctPhone(apartments),
        fill: COLORS.apartments
      },
      {
        name: 'Majorité Maisons',
        clients: houses.length,
        leads: houses.reduce((s, c) => s + c.nb_leads, 0),
        cpl: calcAvgCPL(houses),
        avgPctPhone: calcAvgPctPhone(houses),
        fill: COLORS.houses
      },
      {
        name: 'Mixte',
        clients: mixed.length,
        leads: mixed.reduce((s, c) => s + c.nb_leads, 0),
        cpl: calcAvgCPL(mixed),
        avgPctPhone: calcAvgPctPhone(mixed),
        fill: COLORS.mixte
      }
    ]
  }, [statsClients])

  // Top et Flop clients par CPL
  const topClients = useMemo(() => {
    if (!statsClients.length) return []
    return statsClients
      .filter(c => c.cpl !== null && c.cpl > 0 && c.nb_leads >= 5)
      .sort((a, b) => (a.cpl || 0) - (b.cpl || 0))
      .slice(0, 5)
  }, [statsClients])

  const flopClients = useMemo(() => {
    if (!statsClients.length) return []
    return statsClients
      .filter(c => c.cpl !== null && c.cpl > 0 && c.nb_leads >= 5)
      .sort((a, b) => (b.cpl || 0) - (a.cpl || 0))
      .slice(0, 5)
  }, [statsClients])

  // Top / Flop taux tel vendeur
  const topPhoneClients = useMemo(() => {
    if (!statsClients.length) return []
    return statsClients
      .filter(c => c.nb_leads_total >= 5)
      .sort((a, b) => b.pct_phone - a.pct_phone)
      .slice(0, 5)
  }, [statsClients])

  const flopPhoneClients = useMemo(() => {
    if (!statsClients.length) return []
    return statsClients
      .filter(c => c.nb_leads_total >= 5)
      .sort((a, b) => a.pct_phone - b.pct_phone)
      .slice(0, 5)
  }, [statsClients])

  // KPI : Leads avec tel. pour 20 000 logements
  // Sous-ensembles par version estimateur
  const statsV1 = useMemo(() => statsClients.filter(c => c.estimateur_version !== 'V2'), [statsClients])
  const statsV2 = useMemo(() => statsClients.filter(c => c.estimateur_version === 'V2'), [statsClients])

  const calc20k = (clients: ClientPubStats[], field: 'nb_leads_vendeur' | 'nb_leads') => {
    const eligible = clients.filter(c => c.nombre_logements != null && c.nombre_logements > 0 && c[field] > 0)
    if (eligible.length === 0) return 0
    const ratios = eligible.map(c => (c[field] / c.nombre_logements!) * 20000)
    return Math.round((ratios.reduce((s, r) => s + r, 0) / ratios.length) * 10) / 10
  }

  const leadsFor20k = useMemo(() => ({
    avg: calc20k(statsClients, 'nb_leads_vendeur'),
    avgTel: calc20k(statsClients, 'nb_leads'),
    avgV1: calc20k(statsV1, 'nb_leads_vendeur'),
    avgV2: calc20k(statsV2, 'nb_leads_vendeur'),
    avgTelV1: calc20k(statsV1, 'nb_leads'),
    avgTelV2: calc20k(statsV2, 'nb_leads'),
  }), [statsClients, statsV1, statsV2])

  const handleSort = (field: string) => {
    if (sortField === field) {
      setSortDirection(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDirection('desc')
    }
  }

  const SortIcon = ({ field }: { field: string }) => {
    if (sortField !== field) return null
    return sortDirection === 'asc' ? <ChevronUp className="w-4 h-4 inline" /> : <ChevronDown className="w-4 h-4 inline" />
  }

  // Mémoisation du tableau+footer Performances — ne re-rend PAS quand expandedCat/activeAlertTab change
  const perfTableJsx = useMemo(() => {
    const SortIcn = ({ field: f }: { field: string }) => {
      if (sortField !== f) return null
      return sortDirection === 'asc' ? <ChevronUp className="w-4 h-4 inline" /> : <ChevronDown className="w-4 h-4 inline" />
    }
    const doSort = (f: string) => {
      if (sortField === f) setSortDirection(d => d === 'asc' ? 'desc' : 'asc')
      else { setSortField(f); setSortDirection('desc') }
    }
    return (<>
      <div className="overflow-auto max-h-[75vh]">
        <table className="w-full">
          <thead className="bg-gray-50 sticky top-0 z-20">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 sticky left-0 z-30 bg-gray-50" onClick={() => doSort('client_name')}>Client <SortIcn field="client_name" /></th>
              <th className="px-4 py-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('estimateur_version')}>Esti <SortIcn field="estimateur_version" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('budget_mensuel')}>Budget mens. <SortIcn field="budget_mensuel" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('nb_leads_vendeur')}>Leads vend. <SortIcn field="nb_leads_vendeur" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('cpl_vendeurs')} title="Budget / leads vendeurs">CPL vend. <SortIcn field="cpl_vendeurs" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('nb_leads_zone_total')}>Leads zone <SortIcn field="nb_leads_zone_total" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('cpl_zone')} title="Budget / leads vendeurs dans la zone">CPL zone <SortIcn field="cpl_zone" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('nombre_logements')}>Logements <SortIcn field="nombre_logements" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('pct_phone')}>% tel vend. <SortIcn field="pct_phone" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('pct_validated_phone')}>% tel validé <SortIcn field="pct_validated_phone" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-blue-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('fb_ads')}>FB Ads <SortIcn field="fb_ads" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-green-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('google_ads')}>Google Ads <SortIcn field="google_ads" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-orange-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('total_ads')}>Total Ads <SortIcn field="total_ads" /></th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider" title="Budget quotidien configuré (Meta + Google)">Budget/j</th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-emerald-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100" onClick={() => doSort('marge_pct')} title="(Budget période − Total Ads) / Budget période × 100">Marge % <SortIcn field="marge_pct" /></th>
              <th className="px-4 py-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider">Comptes</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {filteredClients.map((client) => {
              const ads = adsPerClient.get(client.id_client) || null
              const adKeys = ads?.matchedAdKeys || []
              let configuredDailyBudget = 0
              for (const key of adKeys) {
                if (adsEntries.meta[key]) for (const ae of adsEntries.meta[key]) if (ae.daily_budget) configuredDailyBudget += ae.daily_budget
                if (adsEntries.google[key]) for (const ae of adsEntries.google[key]) if (ae.daily_budget) configuredDailyBudget += ae.daily_budget
              }
              const totalSpend = ads ? ads.meta.spend + ads.google.spend : 0
              const periodMonths = periodMultiplier(period, client.nb_mois)
              const budgetPeriode = client.budget_mensuel > 0 ? client.budget_mensuel * periodMonths : 0
              const cplVendeurs = client.nb_leads_vendeur > 0 && budgetPeriode > 0 ? budgetPeriode / client.nb_leads_vendeur : null
              const cplZone = client.nb_leads_zone_total > 0 && budgetPeriode > 0 ? budgetPeriode / client.nb_leads_zone_total : null
              const margePct = budgetPeriode > 0 && totalSpend > 0 ? ((budgetPeriode - totalSpend) / budgetPeriode) * 100 : null
              const healthColor = cplVendeurs === null ? 'border-l-gray-200' : cplVendeurs <= 30 ? 'border-l-emerald-400' : cplVendeurs <= 45 ? 'border-l-green-400' : cplVendeurs <= 60 ? 'border-l-amber-400' : cplVendeurs <= 80 ? 'border-l-orange-400' : 'border-l-red-400'
              const avgCpl = stats?.summary.avg_cpl || 0
              return (
                <tr key={client.id_client} className={`hover:bg-gray-50 group border-l-4 ${healthColor} transition-colors`}>
                  <td className="px-6 py-4 sticky left-0 z-10 bg-white group-hover:bg-gray-50">
                    <div className="flex items-center gap-2">
                      <div className="font-medium text-gray-900 cursor-pointer hover:text-blue-600 hover:underline" onClick={() => openClientDetail(client)}>{clientDisplayNames.get(client.id_client) || client.client_name}</div>
                      <button onClick={() => openClientDetail(client)} className="opacity-0 group-hover:opacity-100 transition-opacity text-gray-400 hover:text-blue-600 cursor-pointer" title="Voir le détail"><Eye className="w-3.5 h-3.5" /></button>
                    </div>
                    <div className="text-xs text-gray-500 max-w-[220px]">{client.postal_codes.join(', ')}</div>
                  </td>
                  <td className="px-4 py-4 text-center"><span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold ${client.estimateur_version === 'V2' ? 'bg-green-100 text-green-800' : 'bg-orange-100 text-orange-800'}`}>{client.estimateur_version}</span></td>
                  <td className="px-4 py-4 text-right">{client.budget_mensuel > 0 ? <span className="font-medium text-gray-900">{client.budget_mensuel.toLocaleString()} €</span> : <span className="text-gray-400">N/A</span>}</td>
                  <td className="px-4 py-4 text-right"><span className="font-semibold text-gray-900">{client.nb_leads_vendeur}</span></td>
                  <td className="px-4 py-4 text-right">{cplVendeurs ? <span className={`font-bold ${cplVendeurs < avgCpl ? 'text-green-600' : 'text-red-600'}`}>{cplVendeurs.toFixed(2)} €</span> : <span className="text-gray-400">N/A</span>}</td>
                  <td className="px-4 py-4 text-right"><span className="font-medium text-gray-900">{client.nb_leads_zone_total}</span></td>
                  <td className="px-4 py-4 text-right">{cplZone ? <span className={`font-bold ${cplZone < avgCpl * 1.5 ? 'text-green-600' : 'text-red-600'}`}>{cplZone.toFixed(2)} €</span> : <span className="text-gray-400">N/A</span>}</td>
                  <td className="px-4 py-4 text-right">{client.nombre_logements != null ? <span className="font-medium text-gray-900">{client.nombre_logements.toLocaleString()}</span> : <span className="text-gray-400">N/A</span>}</td>
                  <td className="px-4 py-4 text-right"><span className={`font-medium ${client.pct_phone >= 70 ? 'text-green-600' : client.pct_phone >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>{client.pct_phone}%</span></td>
                  <td className="px-4 py-4 text-right"><span className={`font-medium ${client.pct_validated_phone >= 70 ? 'text-green-600' : client.pct_validated_phone >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>{client.pct_validated_phone}%</span></td>
                  <td className="px-4 py-4 text-right">{ads && ads.meta.spend > 0 ? <><span className="font-medium text-blue-700">{Math.round(ads.meta.spend).toLocaleString()} €</span><span className="block text-xs text-gray-400">{ads.meta.clicks.toLocaleString()} clics</span></> : <span className="text-gray-300">-</span>}</td>
                  <td className="px-4 py-4 text-right">{ads && ads.google.spend > 0 ? <><span className="font-medium text-green-700">{Math.round(ads.google.spend).toLocaleString()} €</span><span className="block text-xs text-gray-400">{ads.google.clicks.toLocaleString()} clics</span></> : <span className="text-gray-300">-</span>}</td>
                  <td className="px-4 py-4 text-right">{totalSpend > 0 ? <><span className="font-bold text-orange-700">{Math.round(totalSpend).toLocaleString()} €</span>{ads && <span className="block text-xs text-gray-400">{(ads.meta.clicks + ads.google.clicks).toLocaleString()} clics</span>}</> : <span className="text-gray-300">-</span>}</td>
                  <td className="px-4 py-4 text-right">{configuredDailyBudget > 0 ? <span className="font-medium text-gray-800">{configuredDailyBudget.toFixed(0)} €</span> : <span className="text-gray-300">-</span>}</td>
                  <td className="px-4 py-4 text-right">{margePct !== null ? <span className={`font-bold ${margePct >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>{margePct >= 0 ? '+' : ''}{margePct.toFixed(1)}%</span> : <span className="text-gray-300">-</span>}</td>
                  <td className="px-4 py-4 text-center">
                    {ads && (ads.metaAccountIds.length > 0 || ads.googleAccountIds.length > 0) ? (
                      <div className="flex flex-wrap gap-1 max-w-[200px] max-h-20 overflow-y-auto justify-center">
                        {ads.metaAccountIds.length > 0 && [...new Map(ads.metaAccountIds.map(raw => { const [name, actId] = raw.includes('|||') ? raw.split('|||') : [raw, '']; return { name, numericId: actId.replace(/^act_/, ''), raw } }).filter(x => x.numericId).map(x => [x.numericId, x] as const)).values()].map(({ name, numericId, raw }) => (
                          <a key={`meta-${raw}`} href={`https://adsmanager.facebook.com/adsmanager/manage/adsets?act=${numericId}&columns=name%2Cdelivery%2Ccampaign_name%2Cbid%2Cbudget%2Clast_significant_edit%2Cresults%2Creach%2Cimpressions%2Cfrequency%2Ccpm%2Ccost_per_result%2Cactions%3Alink_click%2Cspend%2Cend_time%2Cschedule&nav_source=no_referrer`} target="_blank" rel="noopener noreferrer" className="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-blue-50 text-blue-700 hover:bg-blue-100 cursor-pointer transition-colors truncate max-w-[180px]" title={name}>{name}</a>
                        ))}
                        {ads.googleAccountIds.length > 0 && [...new Map(ads.googleAccountIds.map(raw => { const [name, cid] = raw.includes('|||') ? raw.split('|||') : [raw, '']; return { name, cid, raw } }).filter(x => x.cid).map(x => [x.cid, x] as const)).values()].map(({ name, cid, raw }) => (
                          <a key={`google-${raw}`} href={`https://ads.google.com/aw/campaigns?ocid=${cid}`} target="_blank" rel="noopener noreferrer" className="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-green-50 text-green-700 hover:bg-green-100 cursor-pointer transition-colors truncate max-w-[180px]" title={name}>{name}</a>
                        ))}
                      </div>
                    ) : <span className="text-gray-300">-</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        {filteredClients.length === 0 && (
          <div className="text-center py-16">
            <Search className="w-10 h-10 text-gray-300 mx-auto mb-3" />
            <p className="text-gray-500 font-medium">Aucun client trouvé</p>
            <p className="text-sm text-gray-400 mt-1">Essayez de modifier vos filtres ou votre recherche</p>
          </div>
        )}
      </div>
      <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
        <p className="text-sm text-gray-600">{filteredClients.length} client(s) affiché(s) sur {stats?.clients.length || 0}</p>
      </div>
    </>)
  }, [filteredClients, adsPerClient, adsEntries, clientDisplayNames, stats, period, sortField, sortDirection]) // openClientDetail excluded intentionally (stable via deps)

  // Compteurs d'alertes pour badges onglets
  const totalAlerts = (alertCategories.cat1?.length || 0) + (alertCategories.cat2?.length || 0) + (alertCategories.cat3?.length || 0) + (alertCategories.cat4?.length || 0) + (alertCategories.cat5?.length || 0)

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
        <div className="relative">
          <div className="animate-spin rounded-full h-14 w-14 border-4 border-blue-100 border-t-blue-600"></div>
          <DollarSign className="w-6 h-6 text-blue-600 absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2" />
        </div>
        <div className="text-center">
          <p className="text-sm font-medium text-gray-700">Chargement des campagnes...</p>
          <p className="text-xs text-gray-400 mt-1">Analyse des performances en cours</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-xl p-8 text-center max-w-md mx-auto mt-12">
        <div className="w-16 h-16 bg-red-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
          <AlertTriangle className="w-8 h-8 text-red-500" />
        </div>
        <p className="text-red-800 font-semibold mb-1">Oups, quelque chose a planté</p>
        <p className="text-red-600 text-sm">{error}</p>
        <button
          onClick={() => window.location.reload()}
          className="mt-4 px-4 py-2 bg-red-600 text-white rounded-lg text-sm font-medium hover:bg-red-700 transition-colors cursor-pointer"
        >
          Réessayer
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Toast container */}
      <ToastContainer toasts={toasts} onRemove={removeToast} />

      {/* Onglets */}
      <div className="flex items-center gap-3">
        <div className="flex gap-1 bg-gray-100 rounded-xl p-1">
          <button
            onClick={() => setActiveTab('rentabilite')}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all duration-200 cursor-pointer ${
              activeTab === 'rentabilite'
                ? 'bg-white text-blue-700 shadow-sm ring-1 ring-black/5'
                : 'text-gray-500 hover:text-gray-700 hover:bg-white/50'
            }`}
          >
            <Zap className="w-4 h-4" />
            Performances
            {totalAlerts > 0 && (
              <span className={`ml-1 min-w-[20px] h-5 flex items-center justify-center rounded-full text-xs font-bold ${
                activeTab === 'rentabilite' ? 'bg-red-100 text-red-700' : 'bg-gray-200 text-gray-600'
              }`}>{totalAlerts}</span>
            )}
          </button>
          <button
            onClick={() => setActiveTab('tableau')}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all duration-200 cursor-pointer ${
              activeTab === 'tableau'
                ? 'bg-white text-blue-700 shadow-sm ring-1 ring-black/5'
                : 'text-gray-500 hover:text-gray-700 hover:bg-white/50'
            }`}
          >
            <Table className="w-4 h-4" />
            Tableau
            <span className={`ml-1 min-w-[20px] h-5 flex items-center justify-center rounded-full text-xs font-bold ${
              activeTab === 'tableau' ? 'bg-blue-100 text-blue-700' : 'bg-gray-200 text-gray-600'
            }`}>{filteredClients.length}</span>
          </button>
          <button
            onClick={() => setActiveTab('statistiques')}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all duration-200 cursor-pointer ${
              activeTab === 'statistiques'
                ? 'bg-white text-blue-700 shadow-sm ring-1 ring-black/5'
                : 'text-gray-500 hover:text-gray-700 hover:bg-white/50'
            }`}
          >
            <BarChart3 className="w-4 h-4" />
            Statistiques
          </button>
        </div>
        {refreshing && (
          <div className="flex items-center gap-2 text-xs text-blue-600 bg-blue-50 px-3 py-1.5 rounded-full">
            <div className="animate-spin rounded-full h-3.5 w-3.5 border-2 border-blue-600 border-t-transparent" />
            Mise à jour...
          </div>
        )}
      </div>

      {activeTab === 'statistiques' && (<>
      {/* Filtres rapides Statistiques */}
      <div className="flex items-center gap-4 flex-wrap">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-gray-600">Période :</span>
          <div className="flex rounded-lg border border-gray-300 overflow-hidden text-sm">
            {([['all', 'Depuis le début'], ['month', 'Mois en cours'], ['30d', '30 jours'], ['90d', '90 jours']] as const).map(([key, label]) => (
              <button
                key={key}
                onClick={() => setPeriod(key)}
                disabled={refreshing}
                className={`px-3 py-1.5 transition-colors cursor-pointer ${period === key ? 'bg-blue-600 text-white font-medium' : 'bg-white text-gray-600 hover:bg-gray-50'} disabled:opacity-60`}
              >
                {label}
              </button>
            ))}
          </div>
          {refreshing && <div className="animate-spin rounded-full h-4 w-4 border-2 border-blue-600 border-t-transparent" />}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-gray-600">Ancienneté :</span>
          <select
            value={filterAnciennete}
            onChange={(e) => setFilterAnciennete(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 cursor-pointer"
          >
            <option value="all">Tous</option>
            <option value="5d">+ de 5 jours</option>
            <option value="1">+ de 1 mois</option>
            <option value="2">+ de 2 mois</option>
            <option value="3">+ de 3 mois</option>
            <option value="6">+ de 6 mois</option>
            <option value="12">+ de 12 mois</option>
          </select>
        </div>
        <span className="text-sm text-gray-400">{statsClients.length} client(s)</span>
      </div>

{/* KPIs principaux */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* Clients analysés */}
        <div className="bg-gradient-to-br from-blue-50 to-blue-100/50 rounded-xl p-4 border border-blue-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-blue-500 rounded-lg">
                <Users className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-blue-700 uppercase tracking-wide">Clients</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">{statsClients.length}</p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">Clients avec une campagne pub active sur la période</p>
          <p className="text-[11px] text-gray-400 mt-0.5">V1: {statsV1.length} | V2: {statsV2.length}</p>
        </div>

        {/* Total Leads */}
        <div className="bg-gradient-to-br from-green-50 to-green-100/50 rounded-xl p-4 border border-green-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-green-500 rounded-lg">
                <Target className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-green-700 uppercase tracking-wide">Leads</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">{statsClients.reduce((s, c) => s + c.nb_leads, 0).toLocaleString()}</p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">Total des leads reçus — <span className="font-medium text-green-700">avec tel. uniquement</span></p>
          <p className="text-[11px] text-gray-400 mt-0.5">V1: {statsV1.reduce((s, c) => s + c.nb_leads, 0).toLocaleString()} | V2: {statsV2.reduce((s, c) => s + c.nb_leads, 0).toLocaleString()}</p>
        </div>

        {/* CPL moyen */}
        <div className="bg-gradient-to-br from-purple-50 to-purple-100/50 rounded-xl p-4 border border-purple-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-purple-500 rounded-lg">
                <DollarSign className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-purple-700 uppercase tracking-wide">CPL moy.</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {(() => {
                const withCpl = statsClients.filter(c => c.cpl !== null && c.cpl > 0)
                if (withCpl.length === 0) return 'N/A'
                const avg = withCpl.reduce((s, c) => s + (c.cpl || 0), 0) / withCpl.length
                return `${avg.toFixed(0)} €`
              })()}
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">
            Coût moyen par lead — <span className="font-medium text-purple-700">avec ou sans tel.</span>{' '}
            <span className="text-purple-600 font-medium">
              {(() => {
                const cpls = statsClients.filter(c => c.cpl !== null && c.cpl > 0).map(c => c.cpl!)
                if (cpls.length === 0) return ''
                return `Min ${Math.min(...cpls).toFixed(0)} € — Max ${Math.max(...cpls).toFixed(0)} €`
              })()}
            </span>
          </p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            V1: {(() => { const w = statsV1.filter(c => c.cpl !== null && c.cpl > 0); return w.length > 0 ? `${Math.round(w.reduce((s, c) => s + (c.cpl || 0), 0) / w.length)} €` : 'N/A' })()}
            {' | '}
            V2: {(() => { const w = statsV2.filter(c => c.cpl !== null && c.cpl > 0); return w.length > 0 ? `${Math.round(w.reduce((s, c) => s + (c.cpl || 0), 0) / w.length)} €` : 'N/A' })()}
          </p>
        </div>

        {/* CPL réel (basé sur dépenses ads réelles) */}
        <div className="bg-gradient-to-br from-rose-50 to-rose-100/50 rounded-xl p-4 border border-rose-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-rose-500 rounded-lg">
                <DollarSign className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-rose-700 uppercase tracking-wide">CPL réel</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {(() => {
                let totalSpend = 0, totalLeads = 0
                statsClients.forEach(c => {
                  const ads = adsPerClient.get(c.id_client)
                  if (ads) {
                    const spend = ads.meta.spend + ads.google.spend
                    if (spend > 0 && c.nb_leads_vendeur > 0) {
                      totalSpend += spend
                      totalLeads += c.nb_leads_vendeur
                    }
                  }
                })
                return totalLeads > 0 ? `${Math.round(totalSpend / totalLeads)} €` : 'N/A'
              })()}
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">
            Dépenses ads réelles / leads — <span className="font-medium text-rose-700">avec ou sans tel.</span>
          </p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            V1: {(() => {
              let totalSpend = 0, totalLeads = 0
              statsV1.forEach(c => {
                const ads = adsPerClient.get(c.id_client)
                if (ads) { const spend = ads.meta.spend + ads.google.spend; if (spend > 0 && c.nb_leads_vendeur > 0) { totalSpend += spend; totalLeads += c.nb_leads_vendeur } }
              })
              return totalLeads > 0 ? `${Math.round(totalSpend / totalLeads)} €` : 'N/A'
            })()}
            {' | '}
            V2: {(() => {
              let totalSpend = 0, totalLeads = 0
              statsV2.forEach(c => {
                const ads = adsPerClient.get(c.id_client)
                if (ads) { const spend = ads.meta.spend + ads.google.spend; if (spend > 0 && c.nb_leads_vendeur > 0) { totalSpend += spend; totalLeads += c.nb_leads_vendeur } }
              })
              return totalLeads > 0 ? `${Math.round(totalSpend / totalLeads)} €` : 'N/A'
            })()}
          </p>
        </div>

        {/* Leads / 20k avec ou sans tel */}
        <div className="bg-gradient-to-br from-cyan-50 to-cyan-100/50 rounded-xl p-4 border border-cyan-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-cyan-500 rounded-lg">
                <TrendingUp className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-cyan-700 uppercase tracking-wide">Leads / 20k</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {leadsFor20k.avg > 0 ? leadsFor20k.avg : 'N/A'}
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">
            Moy. leads vendeurs / 20k logements — <span className="font-medium text-cyan-700">avec ou sans tel.</span>
          </p>
          <p className="text-[11px] text-gray-400 mt-0.5">V1: {leadsFor20k.avgV1 || 'N/A'} | V2: {leadsFor20k.avgV2 || 'N/A'}</p>
        </div>

        {/* Leads / 20k avec tel uniquement */}
        <div className="bg-gradient-to-br from-teal-50 to-teal-100/50 rounded-xl p-4 border border-teal-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-teal-500 rounded-lg">
                <Phone className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide">Leads / 20k</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {leadsFor20k.avgTel > 0 ? leadsFor20k.avgTel : 'N/A'}
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">
            Moy. leads vendeurs / 20k logements — <span className="font-medium text-teal-700">avec tel. uniquement</span>
          </p>
          <p className="text-[11px] text-gray-400 mt-0.5">V1: {leadsFor20k.avgTelV1 || 'N/A'} | V2: {leadsFor20k.avgTelV2 || 'N/A'}</p>
        </div>

        {/* Leads vendeur moyen */}
        <div className="bg-gradient-to-br from-indigo-50 to-indigo-100/50 rounded-xl p-4 border border-indigo-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-indigo-500 rounded-lg">
                <Users className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-indigo-700 uppercase tracking-wide">Leads vend.</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {statsClients.length > 0
                ? Math.round(statsClients.reduce((s, c) => s + c.nb_leads_vendeur, 0) / statsClients.length)
                : 0}
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">
            Moy. leads vendeurs par client — <span className="font-medium text-indigo-700">avec ou sans tel.</span>{' '}
            <span className="text-indigo-600 font-medium">
              {statsClients.length > 0
                ? Math.round(statsClients.reduce((s, c) => s + c.nb_leads, 0) / statsClients.length)
                : 0} avec tel.
            </span>
          </p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            V1: {statsV1.length > 0 ? Math.round(statsV1.reduce((s, c) => s + c.nb_leads_vendeur, 0) / statsV1.length) : 0}
            {' | '}
            V2: {statsV2.length > 0 ? Math.round(statsV2.reduce((s, c) => s + c.nb_leads_vendeur, 0) / statsV2.length) : 0}
          </p>
        </div>

        {/* % Leads contactés */}
        <div className="bg-gradient-to-br from-orange-50 to-orange-100/50 rounded-xl p-4 border border-orange-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-orange-500 rounded-lg">
                <Phone className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-orange-700 uppercase tracking-wide">Contactés</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {statsClients.length > 0
                ? (statsClients.reduce((s, c) => s + c.pct_lead_contacte, 0) / statsClients.length).toFixed(1)
                : 0}%
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">% moyen de leads contactés (appel/message) — <span className="font-medium text-orange-700">avec tel. uniquement</span></p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            V1: {statsV1.length > 0 ? (statsV1.reduce((s, c) => s + c.pct_lead_contacte, 0) / statsV1.length).toFixed(1) : 0}%
            {' | '}
            V2: {statsV2.length > 0 ? (statsV2.reduce((s, c) => s + c.pct_lead_contacte, 0) / statsV2.length).toFixed(1) : 0}%
          </p>
        </div>

        {/* % Relances prévues */}
        <div className="bg-gradient-to-br from-amber-50 to-amber-100/50 rounded-xl p-4 border border-amber-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-amber-500 rounded-lg">
                <Bell className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-amber-700 uppercase tracking-wide">Relances</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {statsClients.length > 0
                ? (statsClients.reduce((s, c) => s + c.pct_relance_prevu, 0) / statsClients.length).toFixed(1)
                : 0}%
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">% moyen de leads avec relance programmée — <span className="font-medium text-amber-700">avec tel. uniquement</span></p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            V1: {statsV1.length > 0 ? (statsV1.reduce((s, c) => s + c.pct_relance_prevu, 0) / statsV1.length).toFixed(1) : 0}%
            {' | '}
            V2: {statsV2.length > 0 ? (statsV2.reduce((s, c) => s + c.pct_relance_prevu, 0) / statsV2.length).toFixed(1) : 0}%
          </p>
        </div>

        {/* Mandats signés */}
        <div className="bg-gradient-to-br from-emerald-50 to-emerald-100/50 rounded-xl p-4 border border-emerald-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-emerald-500 rounded-lg">
                <FileCheck className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-emerald-700 uppercase tracking-wide">Mandats</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {statsClients.reduce((s, c) => s + c.mandats_signed, 0)}
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">Total des mandats signés issus des campagnes — <span className="font-medium text-emerald-700">avec ou sans tel.</span></p>
          <p className="text-[11px] text-gray-400 mt-0.5">V1: {statsV1.reduce((s, c) => s + c.mandats_signed, 0)} | V2: {statsV2.reduce((s, c) => s + c.mandats_signed, 0)}</p>
        </div>

        {/* % Tel vendeur moyen */}
        <div className="bg-gradient-to-br from-cyan-50 to-cyan-100/50 rounded-xl p-4 border border-cyan-200/50">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-cyan-500 rounded-lg">
                <Phone className="w-4 h-4 text-white" />
              </div>
              <p className="text-xs font-semibold text-cyan-700 uppercase tracking-wide">% Tel vend.</p>
            </div>
            <p className="text-2xl font-bold text-gray-900">
              {statsClients.length > 0
                ? (statsClients.reduce((s, c) => s + c.pct_phone, 0) / statsClients.length).toFixed(1)
                : 0}%
            </p>
          </div>
          <p className="text-[11px] text-gray-500 leading-snug">% moyen de leads vendeurs avec téléphone — <span className="font-medium text-cyan-700">conversion page tel.</span></p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            V1: {statsV1.length > 0 ? (statsV1.reduce((s, c) => s + c.pct_phone, 0) / statsV1.length).toFixed(1) : 0}%
            {' | '}
            V2: {statsV2.length > 0 ? (statsV2.reduce((s, c) => s + c.pct_phone, 0) / statsV2.length).toFixed(1) : 0}%
          </p>
          <p className="text-[11px] text-gray-400 mt-0.5">
            Min: {statsClients.length > 0 ? Math.min(...statsClients.map(c => c.pct_phone)) : 0}%
            {' | '}
            Max: {statsClients.length > 0 ? Math.max(...statsClients.map(c => c.pct_phone)) : 0}%
          </p>
        </div>
      </div>

      {/* Graphiques d'analyse */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* CPL par type de zone */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <MapPin className="w-5 h-5 text-blue-600" />
            Performance par type de zone
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={zoneTypeData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="name" type="category" width={100} />
              <Tooltip
                formatter={(value: any, name: any) => {
                  const v = Number(value)
                  if (name === 'CPL moyen (€)') return [`${v.toFixed(2)} €`, name]
                  if (name === '% tel vendeur') return [`${v.toFixed(1)}%`, name]
                  if (name === 'Leads') return [v.toLocaleString(), name]
                  return [v, name]
                }}
              />
              <Legend />
              <Bar dataKey="leads" fill="#3b82f6" name="Leads" />
              <Bar dataKey="cpl" fill="#8b5cf6" name="CPL moyen (€)" />
              <Bar dataKey="avgPctPhone" fill="#14b8a6" name="% tel vendeur" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* CPL par taille de zone */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <Target className="w-5 h-5 text-green-600" />
            Performance par taille de zone
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={zoneSizeData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" />
              <YAxis yAxisId="left" orientation="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip
                formatter={(value: any, name: any) => {
                  const v = Number(value)
                  if (name === 'CPL moyen (€)') return [`${v.toFixed(2)} €`, name]
                  if (name === '% tel vendeur') return [`${v.toFixed(1)}%`, name]
                  return [v.toLocaleString(), name]
                }}
              />
              <Legend />
              <Bar yAxisId="left" dataKey="leads" fill="#10b981" name="Leads" />
              <Bar yAxisId="right" dataKey="cpl" fill="#f59e0b" name="CPL moyen (€)" />
              <Bar yAxisId="right" dataKey="avgPctPhone" fill="#14b8a6" name="% tel vendeur" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Répartition par type de bien */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <Home className="w-5 h-5 text-indigo-600" />
            Performance par type de bien
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={propertyTypeData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" />
              <YAxis yAxisId="left" orientation="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip
                formatter={(value: any, name: any) => {
                  const v = Number(value)
                  if (name === 'CPL moyen (€)') return [`${v.toFixed(2)} €`, name]
                  if (name === '% tel vendeur') return [`${v.toFixed(1)}%`, name]
                  return [v.toLocaleString(), name]
                }}
              />
              <Legend />
              <Bar yAxisId="left" dataKey="leads" name="Leads">
                {propertyTypeData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.fill} />
                ))}
              </Bar>
              <Bar yAxisId="right" dataKey="cpl" fill="#9ca3af" name="CPL moyen (€)" />
              <Bar yAxisId="right" dataKey="avgPctPhone" fill="#14b8a6" name="% tel vendeur" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Distribution des CPL */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <DollarSign className="w-5 h-5 text-purple-600" />
            Distribution des leads par zone
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={zoneTypeData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }: any) => `${name} ${((percent || 0) * 100).toFixed(0)}%`}
                outerRadius={100}
                dataKey="leads"
              >
                {zoneTypeData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.fill} />
                ))}
              </Pie>
              <Tooltip formatter={(value: any) => [Number(value).toLocaleString(), 'Leads']} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Top / Flop */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top 5 meilleurs CPL */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <Award className="w-5 h-5 text-green-600" />
            Top 5 - Meilleurs CPL
            <span className="text-xs text-gray-500 font-normal">(min. 5 leads)</span>
          </h3>
          <div className="space-y-3">
            {topClients.map((client, index) => (
              <div key={client.id_client} className="flex items-center justify-between p-3 bg-green-50 rounded-lg">
                <div className="flex items-center gap-3">
                  <span className="w-6 h-6 bg-green-600 text-white rounded-full flex items-center justify-center text-sm font-bold">
                    {index + 1}
                  </span>
                  <div>
                    <p className="font-medium text-gray-900">{clientDisplayNames.get(client.id_client) || client.client_name}</p>
                    <p className="text-xs text-gray-500">
                      {client.nb_leads} leads | {ZONE_LABELS[client.zone_type]} | {client.zone_size} CP
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="font-bold text-green-700">{client.cpl?.toFixed(2)} €</p>
                  <p className="text-xs text-gray-500">CPL</p>
                </div>
              </div>
            ))}
            {topClients.length === 0 && (
              <p className="text-gray-500 text-center py-4">Aucune donnée disponible</p>
            )}
          </div>
        </div>

        {/* Flop 5 pires CPL */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <TrendingDown className="w-5 h-5 text-red-600" />
            Flop 5 - CPL à optimiser
            <span className="text-xs text-gray-500 font-normal">(min. 5 leads)</span>
          </h3>
          <div className="space-y-3">
            {flopClients.map((client, index) => (
              <div key={client.id_client} className="flex items-center justify-between p-3 bg-red-50 rounded-lg">
                <div className="flex items-center gap-3">
                  <span className="w-6 h-6 bg-red-600 text-white rounded-full flex items-center justify-center text-sm font-bold">
                    {index + 1}
                  </span>
                  <div>
                    <p className="font-medium text-gray-900">{clientDisplayNames.get(client.id_client) || client.client_name}</p>
                    <p className="text-xs text-gray-500">
                      {client.nb_leads} leads | {ZONE_LABELS[client.zone_type]} | {client.zone_size} CP
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="font-bold text-red-700">{client.cpl?.toFixed(2)} €</p>
                  <p className="text-xs text-gray-500">CPL</p>
                </div>
              </div>
            ))}
            {flopClients.length === 0 && (
              <p className="text-gray-500 text-center py-4">Aucune donnée disponible</p>
            )}
          </div>
        </div>
      </div>

      {/* Top / Flop taux tel vendeur */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top 5 meilleurs taux tel */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <TrendingUp className="w-5 h-5 text-cyan-600" />
            Top 5 - Meilleur % tel vendeur
            <span className="text-xs text-gray-500 font-normal">(min. 5 leads)</span>
          </h3>
          <div className="space-y-3">
            {topPhoneClients.map((client, index) => (
              <div key={client.id_client} className="flex items-center justify-between p-3 bg-cyan-50 rounded-lg">
                <div className="flex items-center gap-3">
                  <span className="w-6 h-6 bg-cyan-600 text-white rounded-full flex items-center justify-center text-sm font-bold">
                    {index + 1}
                  </span>
                  <div>
                    <p className="font-medium text-gray-900">{clientDisplayNames.get(client.id_client) || client.client_name}</p>
                    <p className="text-xs text-gray-500">
                      {client.nb_leads_total} leads ({client.nb_leads} avec tel) | {ZONE_LABELS[client.zone_type]} | {client.zone_size} CP | {client.pct_houses}% maisons
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="font-bold text-cyan-700">{client.pct_phone}%</p>
                  <p className="text-xs text-gray-500">taux tel</p>
                </div>
              </div>
            ))}
            {topPhoneClients.length === 0 && (
              <p className="text-gray-500 text-center py-4">Aucune donnée disponible</p>
            )}
          </div>
        </div>

        {/* Flop 5 pires taux tel */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <TrendingDown className="w-5 h-5 text-red-600" />
            Flop 5 - % tel vendeur à améliorer
            <span className="text-xs text-gray-500 font-normal">(min. 5 leads)</span>
          </h3>
          <div className="space-y-3">
            {flopPhoneClients.map((client, index) => (
              <div key={client.id_client} className="flex items-center justify-between p-3 bg-red-50 rounded-lg">
                <div className="flex items-center gap-3">
                  <span className="w-6 h-6 bg-red-600 text-white rounded-full flex items-center justify-center text-sm font-bold">
                    {index + 1}
                  </span>
                  <div>
                    <p className="font-medium text-gray-900">{clientDisplayNames.get(client.id_client) || client.client_name}</p>
                    <p className="text-xs text-gray-500">
                      {client.nb_leads_total} leads ({client.nb_leads} avec tel) | {ZONE_LABELS[client.zone_type]} | {client.zone_size} CP | {client.pct_houses}% maisons
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="font-bold text-red-700">{client.pct_phone}%</p>
                  <p className="text-xs text-gray-500">taux tel</p>
                </div>
              </div>
            ))}
            {flopPhoneClients.length === 0 && (
              <p className="text-gray-500 text-center py-4">Aucune donnée disponible</p>
            )}
          </div>
        </div>
      </div>
      </>)}

      {activeTab === 'tableau' && (
      <div className="bg-white rounded-xl shadow-sm border border-gray-100">
        <div className="p-6 border-b border-gray-200">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <h3 className="text-lg font-semibold text-gray-900">Détail par client <span className="text-sm font-normal text-gray-500">({filteredClients.length})</span></h3>
              <div className="flex items-center gap-2">
                <div className="flex rounded-lg border border-gray-300 overflow-hidden text-sm">
                  {([['all', 'Depuis le début'], ['month', 'Mois en cours'], ['30d', '30 derniers jours'], ['90d', '90 derniers jours']] as const).map(([key, label]) => (
                    <button
                      key={key}
                      onClick={() => setPeriod(key)}
                      disabled={refreshing}
                      className={`px-3 py-1.5 transition-colors cursor-pointer ${period === key ? 'bg-blue-600 text-white font-medium' : 'bg-white text-gray-600 hover:bg-gray-50'} disabled:opacity-60`}
                    >
                      {label}
                    </button>
                  ))}
                </div>
                {refreshing && <div className="animate-spin rounded-full h-4 w-4 border-2 border-blue-600 border-t-transparent" />}
              </div>
            </div>
            <div className="flex items-center gap-3">
              <div className="relative">
                <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                <input
                  type="text"
                  placeholder="Rechercher un client..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="border border-gray-300 rounded-lg pl-9 pr-8 py-2 text-sm w-56 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200"
                />
                {search && (
                  <button
                    onClick={() => setSearch('')}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 cursor-pointer"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                )}
              </div>
              <button
                onClick={() => setShowFilters(!showFilters)}
                className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg text-sm font-medium transition-colors cursor-pointer"
              >
                <Filter className="w-4 h-4" />
                Filtres
                {showFilters ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
              </button>
            </div>
          </div>

          {/* Filtres */}
          {showFilters && (
            <div className="mt-4 grid grid-cols-1 md:grid-cols-5 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Pays</label>
                <select
                  value={filterLocale}
                  onChange={(e) => setFilterLocale(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="all">Tous les pays</option>
                  <option value="fr_FR">France</option>
                  <option value="es_ES">Espagne</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Type de zone</label>
                <select
                  value={filterZoneType}
                  onChange={(e) => setFilterZoneType(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="all">Toutes les zones</option>
                  {Object.entries(ZONE_LABELS).map(([key, label]) => (
                    <option key={key} value={key}>{label}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Taille de zone</label>
                <select
                  value={filterZoneSize}
                  onChange={(e) => setFilterZoneSize(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="all">Toutes les tailles</option>
                  {Object.entries(SIZE_LABELS).map(([key, label]) => (
                    <option key={key} value={key}>{label}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Type de bien</label>
                <select
                  value={filterPropertyType}
                  onChange={(e) => setFilterPropertyType(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="all">Tous les types</option>
                  <option value="apartments">Majorité Appartements</option>
                  <option value="houses">Majorité Maisons</option>
                  <option value="mixed">Mixte</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Ancienneté</label>
                <select
                  value={filterAnciennete}
                  onChange={(e) => setFilterAnciennete(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="all">Tous les comptes</option>
                  <option value="5d">+ de 5 jours</option>
                  <option value="1">+ de 1 mois</option>
                  <option value="2">+ de 2 mois</option>
                  <option value="3">+ de 3 mois</option>
                  <option value="6">+ de 6 mois</option>
                  <option value="12">+ de 12 mois</option>
                </select>
              </div>
            </div>
          )}
        </div>

        <div className="overflow-auto max-h-[75vh]">
          <table className="w-full">
            <thead className="bg-gray-50 sticky top-0 z-20">
              <tr>
                <th
                  className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 sticky left-0 z-30 bg-gray-50"
                  onClick={() => handleSort('client_name')}
                >
                  Client <SortIcon field="client_name" />
                </th>
                <th
                  className="px-6 py-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('estimateur_version')}
                >
                  Esti. <SortIcon field="estimateur_version" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('nb_leads_total')}
                >
                  Leads <SortIcon field="nb_leads_total" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('cpl')}
                >
                  CPL <SortIcon field="cpl" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('cpl_utile')}
                  title="Budget / leads avec tel. validé"
                >
                  CPL utile <SortIcon field="cpl_utile" />
                </th>
                <th
                  className="px-6 py-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('zone_type')}
                >
                  Zone <SortIcon field="zone_type" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('nombre_logements')}
                >
                  Logements <SortIcon field="nombre_logements" />
                </th>
                <th
                  className="px-6 py-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('pct_apartments')}
                >
                  Appart. <SortIcon field="pct_apartments" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('pct_phone')}
                >
                  % tel <SortIcon field="pct_phone" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('pct_validated_phone')}
                >
                  Tel. validé <SortIcon field="pct_validated_phone" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('pct_lead_contacte')}
                >
                  % contacté <SortIcon field="pct_lead_contacte" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('pct_relance_prevu')}
                >
                  % relance <SortIcon field="pct_relance_prevu" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('mandats_signed')}
                >
                  Mandats <SortIcon field="mandats_signed" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('tx_mandat')}
                  title="Mandats signés / leads vendeurs total"
                >
                  Tx mandat <SortIcon field="tx_mandat" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('nb_leads_zone')}
                  title="Leads vendeurs avec téléphone dans la zone du client"
                >
                  Leads zone tel <SortIcon field="nb_leads_zone" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('budget_mensuel')}
                >
                  Budget mens. <SortIcon field="budget_mensuel" />
                </th>
                <th
                  className="px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('budget_global')}
                >
                  Budget global <SortIcon field="budget_global" />
                </th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-blue-700 uppercase tracking-wider">
                  Fb Ads
                </th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-green-700 uppercase tracking-wider">
                  Google Ads
                </th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-orange-700 uppercase tracking-wider">
                  Total Ads
                </th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-purple-700 uppercase tracking-wider">
                  CPL réel
                </th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-emerald-700 uppercase tracking-wider"
                    title="Revenus (budget mensuel × mois) − dépenses pub réelles">
                  Marge brute
                </th>
                <th className="px-6 py-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider">
                  Comptes
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredClients.map((client) => (
                <tr key={client.id_client} className="hover:bg-gray-50 group">
                  <td className="px-6 py-4 sticky left-0 z-10 bg-white group-hover:bg-gray-50">
                    <div
                      className="font-medium text-gray-900 cursor-pointer hover:text-blue-600 hover:underline"
                      onClick={() => openClientDetail(client)}
                    >{clientDisplayNames.get(client.id_client) || client.client_name}</div>
                    <div className="text-xs text-gray-500">{client.postal_codes.slice(0, 3).join(', ')}{client.postal_codes.length > 3 ? '...' : ''}</div>
                  </td>
                  <td className="px-6 py-4 text-center">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold ${
                      client.estimateur_version === 'V2'
                        ? 'bg-green-100 text-green-800'
                        : 'bg-orange-100 text-orange-800'
                    }`}>
                      {client.estimateur_version}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className="font-semibold text-gray-900">{client.nb_leads_total}</span>
                    <span className="block text-xs text-gray-500">{client.nb_leads} avec tel.</span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    {client.cpl ? (
                      <span className={`font-bold ${client.cpl < (stats?.summary.avg_cpl || 0) ? 'text-green-600' : 'text-red-600'}`}>
                        {client.cpl.toFixed(2)} €
                      </span>
                    ) : (
                      <span className="text-gray-400">N/A</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-right">
                    {(() => {
                      const nbValidated = client.nb_leads_validated_phone || 0
                      const cplUtile = nbValidated > 0 && client.budget_mensuel > 0
                        ? (client.cpl ? (client.cpl * client.nb_leads_total / nbValidated) : null)
                        : null
                      return cplUtile ? (
                        <span className={`font-bold ${cplUtile < (stats?.summary.avg_cpl || 0) * 1.5 ? 'text-green-600' : 'text-red-600'}`}>
                          {cplUtile.toFixed(2)} €
                        </span>
                      ) : (
                        <span className="text-gray-400">N/A</span>
                      )
                    })()}
                  </td>
                  <td className="px-6 py-4 text-center">
                    <span
                      className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
                      style={{ backgroundColor: `${COLORS[client.zone_type as keyof typeof COLORS] || COLORS.mixte}20`, color: COLORS[client.zone_type as keyof typeof COLORS] || COLORS.mixte }}
                    >
                      {ZONE_LABELS[client.zone_type] || client.zone_type}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    {client.nombre_logements != null ? (
                      <span className="font-medium text-gray-900">{client.nombre_logements.toLocaleString()}</span>
                    ) : (
                      <span className="text-gray-400">N/A</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-center">
                    <div className="flex items-center justify-center gap-1">
                      <Building className="w-3 h-3 text-indigo-500" />
                      <span className="text-sm">{client.pct_apartments}%</span>
                      <span className="text-gray-300 mx-1">|</span>
                      <Home className="w-3 h-3 text-pink-500" />
                      <span className="text-sm">{client.pct_houses}%</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className={`font-medium ${client.pct_phone >= 70 ? 'text-green-600' : client.pct_phone >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>
                      {client.pct_phone}%
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className={`font-medium ${client.pct_validated_phone >= 70 ? 'text-green-600' : client.pct_validated_phone >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>
                      {client.pct_validated_phone}%
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className={`font-medium ${client.pct_lead_contacte >= 70 ? 'text-green-600' : client.pct_lead_contacte >= 40 ? 'text-yellow-600' : 'text-red-600'}`}>
                      {client.pct_lead_contacte}%
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className={`font-medium ${client.pct_relance_prevu >= 70 ? 'text-green-600' : client.pct_relance_prevu >= 40 ? 'text-yellow-600' : 'text-red-600'}`}>
                      {client.pct_relance_prevu}%
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className="font-semibold text-gray-900">{client.mandats_signed}</span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    {client.nb_leads_total > 0 ? (
                      <span className={`font-medium ${(client.mandats_signed / client.nb_leads_total * 100) >= 5 ? 'text-green-600' : (client.mandats_signed / client.nb_leads_total * 100) >= 2 ? 'text-yellow-600' : 'text-red-600'}`}>
                        {(client.mandats_signed / client.nb_leads_total * 100).toFixed(1)}%
                      </span>
                    ) : (
                      <span className="text-gray-400">-</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className="font-medium text-gray-900">{client.nb_leads_zone}</span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    {client.budget_mensuel > 0 ? (
                      <span className="font-medium text-gray-900">{client.budget_mensuel.toLocaleString()} €</span>
                    ) : (
                      <span className="text-gray-400">N/A</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-right">
                    {client.budget_global > 0 ? (
                      <>
                        <span className="font-medium text-gray-900">{client.budget_global.toLocaleString()} €</span>
                        <span className="block text-xs text-gray-500">{client.nb_mois} mois</span>
                      </>
                    ) : (
                      <span className="text-gray-400">N/A</span>
                    )}
                  </td>
                  {(() => {
                    const ads = adsPerClient.get(client.id_client) || null
                    const totalSpend = ads ? ads.meta.spend + ads.google.spend : 0
                    const cplReel = totalSpend > 0 && client.nb_leads_vendeur > 0
                      ? Math.round((totalSpend / client.nb_leads_vendeur) * 100) / 100
                      : null
                    // Nb de mois selon la période sélectionnée
                    const periodMonths = period === '90d' ? 3 : period === '30d' ? 1 : period === 'month' ? 1 : client.nb_mois
                    const revenuPeriode = client.budget_mensuel > 0 ? client.budget_mensuel * periodMonths : 0
                    const margeBrute = totalSpend > 0 && revenuPeriode > 0 ? revenuPeriode - totalSpend : null
                    return (
                      <>
                        <td className="px-6 py-4 text-right">
                          {ads && ads.meta.spend > 0 ? (
                            <>
                              <span className="font-medium text-blue-700">{Math.round(ads.meta.spend).toLocaleString()} €</span>
                              <span className="block text-xs text-gray-400">{ads.meta.clicks.toLocaleString()} clics</span>
                            </>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                        <td className="px-6 py-4 text-right">
                          {ads && ads.google.spend > 0 ? (
                            <>
                              <span className="font-medium text-green-700">{Math.round(ads.google.spend).toLocaleString()} €</span>
                              <span className="block text-xs text-gray-400">{ads.google.clicks.toLocaleString()} clics</span>
                            </>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                        <td className="px-6 py-4 text-right">
                          {totalSpend > 0 ? (
                            <>
                              <span className="font-bold text-orange-700">{Math.round(totalSpend).toLocaleString()} €</span>
                              {ads && <span className="block text-xs text-gray-400">{(ads.meta.clicks + ads.google.clicks).toLocaleString()} clics</span>}
                            </>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                        <td className="px-6 py-4 text-right">
                          {cplReel ? (
                            <span className={`font-bold ${cplReel < (stats?.summary.avg_cpl || 0) ? 'text-green-600' : 'text-red-600'}`}>
                              {cplReel.toFixed(2)} €
                            </span>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                        <td className="px-6 py-4 text-right">
                          {margeBrute !== null ? (
                            <>
                              <span className={`font-bold ${margeBrute >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                                {margeBrute >= 0 ? '+' : ''}{Math.round(margeBrute).toLocaleString()} €
                              </span>
                              <span className="block text-xs text-gray-400">
                                {revenuPeriode > 0 ? Math.round((margeBrute / revenuPeriode) * 100) : 0}% marge
                              </span>
                            </>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                        <td className="px-4 py-4 text-center">
                          {ads && (ads.metaAccountIds.length > 0 || ads.googleAccountIds.length > 0) ? (
                            <div className="flex flex-wrap gap-1 max-w-[200px] max-h-20 overflow-y-auto justify-center">
                              {ads.metaAccountIds.length > 0 && [...new Map(
                                ads.metaAccountIds
                                  .map(raw => {
                                    const [name, actId] = raw.includes('|||') ? raw.split('|||') : [raw, '']
                                    const numericId = actId.replace(/^act_/, '')
                                    return { name, numericId, raw }
                                  })
                                  .filter(x => x.numericId)
                                  .map(x => [x.numericId, x] as const)
                              ).values()].map(({ name, numericId, raw }) => (
                                <a key={`meta-${raw}`} href={`https://adsmanager.facebook.com/adsmanager/manage/adsets?act=${numericId}&columns=name%2Cdelivery%2Ccampaign_name%2Cbid%2Cbudget%2Clast_significant_edit%2Cresults%2Creach%2Cimpressions%2Cfrequency%2Ccpm%2Ccost_per_result%2Cactions%3Alink_click%2Cspend%2Cend_time%2Cschedule&nav_source=no_referrer`} target="_blank" rel="noopener noreferrer"
                                  className="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-blue-50 text-blue-700 hover:bg-blue-100 cursor-pointer transition-colors truncate max-w-[180px]" title={name}>
                                  {name}
                                </a>
                              ))}
                              {ads.googleAccountIds.length > 0 && [...new Map(
                                ads.googleAccountIds
                                  .map(raw => {
                                    const [name, cid] = raw.includes('|||') ? raw.split('|||') : [raw, '']
                                    return { name, cid, raw }
                                  })
                                  .filter(x => x.cid)
                                  .map(x => [x.cid, x] as const)
                              ).values()].map(({ name, cid, raw }) => (
                                <a key={`google-${raw}`} href={`https://ads.google.com/aw/campaigns?ocid=${cid}`} target="_blank" rel="noopener noreferrer"
                                  className="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-green-50 text-green-700 hover:bg-green-100 cursor-pointer transition-colors truncate max-w-[180px]" title={name}>
                                  {name}
                                </a>
                              ))}
                            </div>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                      </>
                    )
                  })()}
                </tr>
              ))}
            </tbody>
          </table>

          {filteredClients.length === 0 && (
            <div className="text-center py-16">
              <Search className="w-10 h-10 text-gray-300 mx-auto mb-3" />
              <p className="text-gray-500 font-medium">Aucun client trouvé</p>
              <p className="text-sm text-gray-400 mt-1">Essayez de modifier vos filtres ou votre recherche</p>
              {search && (
                <button
                  onClick={() => setSearch('')}
                  className="mt-3 px-4 py-1.5 bg-blue-50 text-blue-600 rounded-lg text-sm font-medium hover:bg-blue-100 transition-colors cursor-pointer"
                >
                  Effacer la recherche
                </button>
              )}
            </div>
          )}
        </div>

        <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
          <p className="text-sm text-gray-600">
            {filteredClients.length} client(s) affiché(s) sur {stats?.clients.length || 0}
          </p>
        </div>
      </div>
      )}

      {activeTab === 'statistiques' && (
      <div className="bg-gradient-to-r from-blue-600 to-cyan-600 rounded-xl shadow-sm p-6 text-white">
        <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
          <TrendingUp className="w-5 h-5" />
          Insights & Recommandations
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {zoneTypeData.length > 0 && (
            <div className="bg-white/10 rounded-lg p-4">
              <p className="text-sm opacity-90 mb-1">Zone la plus performante</p>
              <p className="font-bold text-lg">
                {zoneTypeData.reduce((best, curr) =>
                  (curr.cpl > 0 && (best.cpl === 0 || curr.cpl < best.cpl)) ? curr : best
                , zoneTypeData[0]).name}
              </p>
              <p className="text-xs opacity-75 mt-1">
                CPL le plus bas observé
              </p>
            </div>
          )}
          {zoneSizeData.length > 0 && (
            <div className="bg-white/10 rounded-lg p-4">
              <p className="text-sm opacity-90 mb-1">Taille optimale</p>
              <p className="font-bold text-lg">
                {zoneSizeData.reduce((best, curr) =>
                  (curr.cpl > 0 && (best.cpl === 0 || curr.cpl < best.cpl)) ? curr : best
                , zoneSizeData[0]).name}
              </p>
              <p className="text-xs opacity-75 mt-1">
                Meilleur ratio leads/coût
              </p>
            </div>
          )}
          <div className="bg-white/10 rounded-lg p-4">
            <p className="text-sm opacity-90 mb-1">Taux de tél. validé moyen</p>
            <p className="font-bold text-lg">
              {statsClients.length > 0
                ? Math.round(statsClients.reduce((s, c) => s + c.pct_validated_phone, 0) / statsClients.length)
                : 0}%
            </p>
            <p className="text-xs opacity-75 mt-1">
              Qualité des leads
            </p>
          </div>
        </div>
      </div>
      )}

      {activeTab === 'rentabilite' && (<>

      <div className="bg-white rounded-xl shadow-sm border border-gray-100">
        <div className="px-5 py-3 border-b border-gray-200">
          <div className="flex items-center gap-3 overflow-x-auto">
            <h3 className="text-sm font-semibold text-gray-900 whitespace-nowrap shrink-0">{filteredClients.length} clients</h3>
            <div className="w-px h-5 bg-gray-200 shrink-0" />
            <div className="flex rounded-lg border border-gray-300 overflow-hidden text-xs shrink-0">
              {([['5d', '5j'], ['15d', '15j'], ['30d', '30j'], ['90d', '90j'], ['all', 'Tout']] as const).map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setPeriod(key)}
                  disabled={refreshing}
                  className={`px-2.5 py-1.5 transition-colors cursor-pointer ${period === key ? 'bg-blue-600 text-white font-medium' : 'bg-white text-gray-600 hover:bg-gray-50'} disabled:opacity-60`}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="w-px h-5 bg-gray-200 shrink-0" />
            <select
              value={filterAnciennete}
              onChange={(e) => setFilterAnciennete(e.target.value)}
              className="border border-gray-300 rounded-lg px-2 py-1.5 text-xs focus:ring-2 focus:ring-blue-500 focus:border-blue-500 cursor-pointer shrink-0"
            >
              <option value="all">Ancienneté : tous</option>
              <option value="5d">+ de 5 jours</option>
              <option value="1">+ de 1 mois</option>
              <option value="2">+ de 2 mois</option>
              <option value="3">+ de 3 mois</option>
              <option value="6">+ de 6 mois</option>
              <option value="12">+ de 12 mois</option>
            </select>
            <div className="flex items-center gap-2 border border-gray-300 rounded-lg px-2.5 py-1.5 shrink-0">
              <label className="flex items-center gap-1 text-xs cursor-pointer">
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
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer w-3.5 h-3.5"
                />
                <span className="select-none">FR</span>
              </label>
              <label className="flex items-center gap-1 text-xs cursor-pointer">
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
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer w-3.5 h-3.5"
                />
                <span className="select-none">ES</span>
              </label>
            </div>
            <div className="ml-auto relative shrink-0">
              <Search className="w-3.5 h-3.5 text-gray-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
              <input
                type="text"
                placeholder="Rechercher..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="border border-gray-300 rounded-lg pl-8 pr-7 py-1.5 text-xs w-44 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200"
              />
              {search && (
                <button
                  onClick={() => setSearch('')}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 cursor-pointer"
                >
                  <X className="w-3 h-3" />
                </button>
              )}
            </div>
          </div>
        </div>

        {/* ── Actions recommandées — tabs horizontaux ── */}
        {totalAlerts > 0 && (
          <div className="border-b border-gray-200">
            {/* Tab bar */}
            <div className="px-6 pt-4 pb-0 flex items-center gap-1 overflow-x-auto">
              {([
                { key: 'cat5' as const, icon: AlertTriangle, label: 'Ne diffuse pas', count: alertCategories.cat5.length, color: 'red', pulse: true },
                { key: 'cat2' as const, icon: RefreshCw, label: 'Rééquilibrer', count: alertCategories.cat2.length, color: 'blue', pulse: false },
                { key: 'cat4' as const, icon: MapPin, label: 'Codes postaux', count: alertCategories.cat4.length, color: 'purple', pulse: false },
                { key: 'cat1' as const, icon: TrendingUp, label: 'Augmenter budget', count: alertCategories.cat1.length, color: 'emerald', pulse: false },
                { key: 'cat3' as const, icon: Target, label: 'Revoir ciblage', count: alertCategories.cat3.length, color: 'amber', pulse: false },
              ].filter(t => t.count > 0).map(tab => {
                const Icon = tab.icon
                const isActive = (pendingAlertTab ?? activeAlertTab) === tab.key
                const colorMap: Record<string, { bg: string; activeBg: string; text: string; badge: string; border: string }> = {
                  red:     { bg: 'hover:bg-red-50',     activeBg: 'bg-red-50 border-red-400',       text: 'text-red-700',     badge: 'bg-red-500',     border: 'border-transparent' },
                  emerald: { bg: 'hover:bg-emerald-50', activeBg: 'bg-emerald-50 border-emerald-400', text: 'text-emerald-700', badge: 'bg-emerald-500', border: 'border-transparent' },
                  blue:    { bg: 'hover:bg-blue-50',    activeBg: 'bg-blue-50 border-blue-400',     text: 'text-blue-700',    badge: 'bg-blue-500',    border: 'border-transparent' },
                  amber:   { bg: 'hover:bg-amber-50',   activeBg: 'bg-amber-50 border-amber-400',   text: 'text-amber-700',   badge: 'bg-amber-500',   border: 'border-transparent' },
                  purple:  { bg: 'hover:bg-purple-50',  activeBg: 'bg-purple-50 border-purple-400', text: 'text-purple-700',  badge: 'bg-purple-500',  border: 'border-transparent' },
                }
                const c = colorMap[tab.color]
                return (
                  <button
                    key={tab.key}
                    onClick={() => switchAlertTab(isActive ? null : tab.key)}
                    className={`flex items-center gap-2 px-4 py-2.5 rounded-t-xl text-xs font-semibold transition-all duration-150 cursor-pointer border-b-2 whitespace-nowrap ${
                      isActive || pendingAlertTab === tab.key
                        ? `${c.activeBg} ${c.text} shadow-sm`
                        : `bg-white ${c.bg} text-gray-500 ${c.border}`
                    }`}
                  >
                    <Icon className={`w-3.5 h-3.5 ${tab.pulse && !isActive ? 'animate-pulse' : ''}`} />
                    {tab.label}
                    <span className={`min-w-[20px] h-5 flex items-center justify-center rounded-full text-[10px] font-bold text-white ${c.badge}`}>
                      {tab.count}
                    </span>
                  </button>
                )
              }))}
            </div>

            {/* Active panel content */}
            {(activeAlertTab || isAlertTabPending) && (
              <div className={`px-6 py-4 bg-gray-50/50 transition-opacity duration-150 ${isAlertTabPending ? 'opacity-60' : 'opacity-100'}`}>
                {/* Description contextuelle */}
                <p className="text-xs text-gray-500 mb-3">
                  {activeAlertTab === 'cat1' && 'CPL > 45€ et marge > 67% — marge suffisante pour augmenter les dépenses pub.'}
                  {activeAlertTab === 'cat2' && 'Un canal performe 2x mieux que l\'autre — transférer du budget vers le meilleur.'}
                  {activeAlertTab === 'cat3' && 'CPL > 45€ et marge < 68% — refaire le ciblage ou le découpage des pubs.'}
                  {activeAlertTab === 'cat4' && 'Gros CP (> 10k logements) avec une densité de leads < 50% de la moyenne du client.'}
                  {activeAlertTab === 'cat5' && 'Budget actif mais aucune diffusion depuis 48h (Meta) ou 6 jours (Google).'}
                </p>

                <div className="space-y-1.5 overflow-y-auto max-h-[50vh]">
                  {/* Cat 1 content */}
                  {activeAlertTab === 'cat1' && alertCategories.cat1.map(e => {
                    const isExpanded = expandedCats.cat1 === e.client.id_client
                    const m = alertClientMetrics.get(e.client.id_client) || { totalSpend: 0, budgetPubMaxJour: 0, spendPerDay: 0, configuredDailyBudget: 0 }
                    const { budgetPubMaxJour, spendPerDay, configuredDailyBudget } = m
                    return (
                      <div key={e.client.id_client}>
                        <div
                          className="flex items-center justify-between text-xs bg-white rounded-lg px-3 py-2 cursor-pointer hover:shadow-sm transition-all border border-gray-100"
                          onClick={() => toggleExpandCat('cat1', e.client.id_client)}
                        >
                          <div className="flex items-center gap-2">
                            <ChevronDown className={`w-3.5 h-3.5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                            <span
                              className="font-semibold text-gray-900 cursor-pointer hover:text-blue-600 hover:underline"
                              onClick={(ev) => { ev.stopPropagation(); openClientDetail(e.client) }}
                            >{clientDisplayNames.get(e.client.id_client) || e.client.client_name}</span>
                          </div>
                          <div className="flex items-center gap-3 shrink-0 text-gray-600">
                            <span>CPL <strong className="text-red-600">{e.cplVendeurs?.toFixed(0)}€</strong></span>
                            {e.metaCpl !== null && <span>Meta <strong className="text-blue-600">{e.metaCpl.toFixed(0)}€</strong></span>}
                            {e.googleCpl !== null && <span>Google <strong className="text-orange-600">{e.googleCpl.toFixed(0)}€</strong></span>}
                            <span>Marge <strong className="text-emerald-600">+{e.margePct?.toFixed(0)}%</strong></span>
                            {e.client.cpl_12m !== null && <span>12m <strong className="text-gray-800">{e.client.cpl_12m.toFixed(0)}€</strong></span>}
                            <span className="text-gray-400">|</span>
                            <span>Dépensé/j <strong className={(configuredDailyBudget > 0 ? configuredDailyBudget : spendPerDay) > budgetPubMaxJour ? 'text-red-600' : 'text-blue-600'}>{spendPerDay.toFixed(1)}€</strong>{configuredDailyBudget > 0 && <>/<strong className="text-gray-800">{configuredDailyBudget.toFixed(1)}€</strong></>}</span>
                            <span>Max/j <strong className="text-gray-800">{budgetPubMaxJour.toFixed(1)}€</strong></span>
                          </div>
                        </div>
                        {isExpanded && renderExpandedEntries(e.client.id_client, 'border-emerald-100')}
                      </div>
                    )
                  })}

                  {/* Cat 2 content */}
                  {activeAlertTab === 'cat2' && alertCategories.cat2.map(e => {
                    const isExpanded = expandedCats.cat2 === e.client.id_client
                    const m = alertClientMetrics.get(e.client.id_client) || { totalSpend: 0, budgetPubMaxJour: 0, spendPerDay: 0, configuredDailyBudget: 0 }
                    const { budgetPubMaxJour, spendPerDay, configuredDailyBudget } = m
                    return (
                      <div key={e.client.id_client}>
                        <div
                          className="flex items-center justify-between text-xs bg-white rounded-lg px-3 py-2 cursor-pointer hover:shadow-sm transition-all border border-gray-100"
                          onClick={() => toggleExpandCat('cat2', e.client.id_client)}
                        >
                          <div className="flex items-center gap-2">
                            <ChevronDown className={`w-3.5 h-3.5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                            <span
                              className="font-semibold text-gray-900 cursor-pointer hover:text-blue-600 hover:underline"
                              onClick={(ev) => { ev.stopPropagation(); openClientDetail(e.client) }}
                            >{clientDisplayNames.get(e.client.id_client) || e.client.client_name}</span>
                          </div>
                          <div className="flex items-center gap-3 shrink-0 text-gray-600">
                            <span>CPL <strong className="text-red-600">{e.cplVendeurs?.toFixed(0)}€</strong></span>
                            <span className={e.bestChannel === 'meta' ? 'text-green-600 font-bold' : 'text-red-600'}>Meta {e.metaCpl?.toFixed(0)}€</span>
                            <span className={e.bestChannel === 'google' ? 'text-green-600 font-bold' : 'text-red-600'}>Google {e.googleCpl?.toFixed(0)}€</span>
                            {e.margePct !== null && <span>Marge <strong className="text-emerald-600">{e.margePct > 0 ? '+' : ''}{e.margePct.toFixed(0)}%</strong></span>}
                            {e.client.cpl_12m !== null && <span>12m <strong className="text-gray-800">{e.client.cpl_12m.toFixed(0)}€</strong></span>}
                            <span className="text-gray-400">|</span>
                            <span>Dépensé/j <strong className={(configuredDailyBudget > 0 ? configuredDailyBudget : spendPerDay) > budgetPubMaxJour ? 'text-red-600' : 'text-blue-600'}>{spendPerDay.toFixed(1)}€</strong>{configuredDailyBudget > 0 && <>/<strong className="text-gray-800">{configuredDailyBudget.toFixed(1)}€</strong></>}</span>
                            <span>Max/j <strong className="text-gray-800">{budgetPubMaxJour.toFixed(1)}€</strong></span>
                          </div>
                        </div>
                        {isExpanded && renderExpandedEntries(e.client.id_client, 'border-blue-100')}
                      </div>
                    )
                  })}

                  {/* Cat 3 content */}
                  {activeAlertTab === 'cat3' && alertCategories.cat3.map(e => {
                    const isExpanded = expandedCats.cat3 === e.client.id_client
                    const m = alertClientMetrics.get(e.client.id_client) || { totalSpend: 0, budgetPubMaxJour: 0, spendPerDay: 0, configuredDailyBudget: 0 }
                    const { budgetPubMaxJour, spendPerDay, configuredDailyBudget } = m
                    return (
                      <div key={e.client.id_client}>
                        <div
                          className="flex items-center justify-between text-xs bg-white rounded-lg px-3 py-2 cursor-pointer hover:shadow-sm transition-all border border-gray-100"
                          onClick={() => toggleExpandCat('cat3', e.client.id_client)}
                        >
                          <div className="flex items-center gap-2">
                            <ChevronDown className={`w-3.5 h-3.5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                            <span
                              className="font-semibold text-gray-900 cursor-pointer hover:text-blue-600 hover:underline"
                              onClick={(ev) => { ev.stopPropagation(); openClientDetail(e.client) }}
                            >{clientDisplayNames.get(e.client.id_client) || e.client.client_name}</span>
                          </div>
                          <div className="flex items-center gap-3 shrink-0 text-gray-600">
                            <span>CPL <strong className="text-red-600">{e.cplVendeurs?.toFixed(0)}€</strong></span>
                            {e.metaCpl !== null && <span>Meta <strong className="text-blue-600">{e.metaCpl.toFixed(0)}€</strong></span>}
                            {e.googleCpl !== null && <span>Google <strong className="text-orange-600">{e.googleCpl.toFixed(0)}€</strong></span>}
                            <span>Marge <strong className="text-amber-600">{e.margePct?.toFixed(0)}%</strong></span>
                            {e.client.cpl_12m !== null && <span>12m <strong className="text-gray-800">{e.client.cpl_12m.toFixed(0)}€</strong></span>}
                            <span className="text-gray-400">|</span>
                            <span>Dépensé/j <strong className={(configuredDailyBudget > 0 ? configuredDailyBudget : spendPerDay) > budgetPubMaxJour ? 'text-red-600' : 'text-blue-600'}>{spendPerDay.toFixed(1)}€</strong>{configuredDailyBudget > 0 && <>/<strong className="text-gray-800">{configuredDailyBudget.toFixed(1)}€</strong></>}</span>
                            <span>Max/j <strong className="text-gray-800">{budgetPubMaxJour.toFixed(1)}€</strong></span>
                          </div>
                        </div>
                        {isExpanded && renderExpandedEntries(e.client.id_client, 'border-amber-100')}
                      </div>
                    )
                  })}

                  {/* Cat 4 content */}
                  {activeAlertTab === 'cat4' && alertCategories.cat4.map(e => {
                    const isExpanded = expandedCats.cat4 === e.client.id_client
                    const m = alertClientMetrics.get(e.client.id_client) || { totalSpend: 0, budgetPubMaxJour: 0, spendPerDay: 0, configuredDailyBudget: 0 }
                    const { budgetPubMaxJour, spendPerDay, configuredDailyBudget } = m
                    return (
                      <div key={e.client.id_client}>
                        <div
                          className="bg-white rounded-lg px-3 py-2 cursor-pointer hover:shadow-sm transition-all border border-gray-100"
                          onClick={() => toggleExpandCat('cat4', e.client.id_client)}
                        >
                          <div className="flex items-center justify-between text-xs mb-1.5">
                            <div className="flex items-center gap-2">
                              <ChevronDown className={`w-3.5 h-3.5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                              <span
                                className="font-semibold text-gray-900 cursor-pointer hover:text-blue-600 hover:underline"
                                onClick={(ev) => { ev.stopPropagation(); openClientDetail(e.client) }}
                              >{clientDisplayNames.get(e.client.id_client) || e.client.client_name}</span>
                            </div>
                            <div className="flex items-center gap-3 text-gray-600">
                              <span>CPL global <strong className="text-green-600">{e.cplVendeurs?.toFixed(0)}€</strong></span>
                              {e.metaCpl !== null && <span>Meta <strong className="text-blue-600">{e.metaCpl.toFixed(0)}€</strong></span>}
                              {e.googleCpl !== null && <span>Google <strong className="text-orange-600">{e.googleCpl.toFixed(0)}€</strong></span>}
                              {e.margePct !== null && <span>Marge <strong className="text-emerald-600">{e.margePct > 0 ? '+' : ''}{e.margePct.toFixed(0)}%</strong></span>}
                              {e.client.cpl_12m !== null && <span>12m <strong className="text-gray-800">{e.client.cpl_12m.toFixed(0)}€</strong></span>}
                              <span className="text-gray-400">|</span>
                              <span>Dépensé/j <strong className={(configuredDailyBudget > 0 ? configuredDailyBudget : spendPerDay) > budgetPubMaxJour ? 'text-red-600' : 'text-blue-600'}>{spendPerDay.toFixed(1)}€</strong>{configuredDailyBudget > 0 && <>/<strong className="text-gray-800">{configuredDailyBudget.toFixed(1)}€</strong></>}</span>
                              <span>Max/j <strong className="text-gray-800">{budgetPubMaxJour.toFixed(1)}€</strong></span>
                            </div>
                          </div>
                          <div className="flex flex-wrap gap-1.5 ml-5">
                            {e.flaggedCps?.map(f => (
                              <span key={f.cp} className="inline-flex items-center gap-1.5 px-2 py-1 rounded-lg text-xs bg-purple-50 text-purple-800 border border-purple-200">
                                <strong>{f.cp}</strong>
                                <span className="text-purple-500">{f.logements.toLocaleString()} log.</span>
                                <span className="text-gray-600">{f.leadsReels} lead{f.leadsReels !== 1 ? 's' : ''}</span>
                                <span className="text-red-600 font-bold">{f.ecartPct}%</span>
                                <span className="text-gray-400" title={`Densité: ${f.densite}/10k vs moyenne ${f.densiteMoyenne}/10k`}>({f.densite} vs {f.densiteMoyenne}/10k)</span>
                              </span>
                            ))}
                          </div>
                        </div>
                        {isExpanded && renderExpandedEntries(e.client.id_client, 'border-purple-100')}
                      </div>
                    )
                  })}

                  {/* Cat 5 content */}
                  {activeAlertTab === 'cat5' && alertCategories.cat5.map(e => {
                    const isExpanded = expandedCats.cat5 === e.client.id_client
                    const m = alertClientMetrics.get(e.client.id_client) || { totalSpend: 0, budgetPubMaxJour: 0, spendPerDay: 0, configuredDailyBudget: 0 }
                    const { budgetPubMaxJour, spendPerDay, configuredDailyBudget } = m
                    return (
                      <div key={e.client.id_client}>
                        <div
                          className="flex items-center justify-between text-xs bg-white rounded-lg px-3 py-2 cursor-pointer hover:shadow-sm transition-all border border-gray-100"
                          onClick={() => toggleExpandCat('cat5', e.client.id_client)}
                        >
                          <div className="flex items-center gap-2">
                            <ChevronDown className={`w-3.5 h-3.5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                            <span
                              className="font-semibold text-gray-900 cursor-pointer hover:text-blue-600 hover:underline"
                              onClick={(ev) => { ev.stopPropagation(); openClientDetail(e.client) }}
                            >{clientDisplayNames.get(e.client.id_client) || e.client.client_name}</span>
                          </div>
                          <div className="flex items-center gap-2 shrink-0 text-gray-600">
                            {e.staleChannels.map(sc => (
                              <span key={sc.channel} className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-medium ${sc.channel === 'meta' ? 'bg-blue-50 text-blue-800 border border-blue-200' : 'bg-orange-50 text-orange-800 border border-orange-200'}`}>
                                {sc.channel === 'meta' ? 'Meta' : 'Google'}
                                <strong className="text-red-600">{sc.daysSince === 999 ? 'jamais' : `${sc.daysSince}j`}</strong>
                              </span>
                            ))}
                            {e.cplVendeurs !== null && <span>CPL <strong className="text-red-600">{e.cplVendeurs.toFixed(0)}€</strong></span>}
                            {e.metaCpl !== null && <span>Meta <strong className="text-blue-600">{e.metaCpl.toFixed(0)}€</strong></span>}
                            {e.googleCpl !== null && <span>Google <strong className="text-orange-600">{e.googleCpl.toFixed(0)}€</strong></span>}
                            {e.margePct !== null && <span>Marge <strong className="text-emerald-600">{e.margePct > 0 ? '+' : ''}{e.margePct.toFixed(0)}%</strong></span>}
                            {e.client.cpl_12m !== null && <span>12m <strong className="text-gray-800">{e.client.cpl_12m.toFixed(0)}€</strong></span>}
                            <span className="text-gray-400">|</span>
                            <span>Dépensé/j <strong className={(configuredDailyBudget > 0 ? configuredDailyBudget : spendPerDay) > budgetPubMaxJour ? 'text-red-600' : 'text-blue-600'}>{spendPerDay.toFixed(1)}€</strong>{configuredDailyBudget > 0 && <>/<strong className="text-gray-800">{configuredDailyBudget.toFixed(1)}€</strong></>}</span>
                            <span>Max/j <strong className="text-gray-800">{budgetPubMaxJour.toFixed(1)}€</strong></span>
                          </div>
                        </div>
                        {isExpanded && renderExpandedEntries(e.client.id_client, 'border-red-100')}
                      </div>
                    )
                  })}
                </div>
              </div>
            )}
          </div>
        )}

        {perfTableJsx}
      </div>
      </>)}

      {/* Modal détail client — plein écran */}
      {detailModal && (
        <div className="fixed inset-0 bg-white z-50 flex flex-col animate-fade-in">
          {/* Top bar */}
          <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200 shrink-0 bg-gradient-to-r from-white to-gray-50">
            <div className="flex items-center gap-4">
              <button onClick={() => { setDetailModal(null); setCplMonthlyData(null) }} className="flex items-center gap-2 text-gray-500 hover:text-gray-800 transition-colors cursor-pointer group">
                <svg className="w-5 h-5 group-hover:-translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
                <span className="text-xs font-medium">Retour</span>
              </button>
              <div className="w-px h-6 bg-gray-200" />
              <h2 className="text-base font-bold text-gray-900">{detailModal.clientName}</h2>
            </div>
            <div className="flex items-center gap-3">
              {detailModal.stats.cpl !== null && (
                <div className="flex items-center gap-1.5 bg-gray-100 rounded-lg px-3 py-1.5">
                  <span className="text-[10px] text-gray-500 uppercase tracking-wide">CPL</span>
                  <strong className={`text-sm ${detailModal.stats.cpl > 45 ? 'text-red-600' : 'text-emerald-600'}`}>{detailModal.stats.cpl.toFixed(0)}€</strong>
                </div>
              )}
              {detailModal.stats.metaCpl !== null && (
                <div className="flex items-center gap-1.5 bg-blue-50 rounded-lg px-3 py-1.5">
                  <span className="text-[10px] text-blue-500 uppercase tracking-wide">Meta</span>
                  <strong className="text-sm text-blue-700">{detailModal.stats.metaCpl.toFixed(0)}€</strong>
                </div>
              )}
              {detailModal.stats.googleCpl !== null && (
                <div className="flex items-center gap-1.5 bg-orange-50 rounded-lg px-3 py-1.5">
                  <span className="text-[10px] text-orange-500 uppercase tracking-wide">Google</span>
                  <strong className="text-sm text-orange-700">{detailModal.stats.googleCpl.toFixed(0)}€</strong>
                </div>
              )}
              {detailModal.stats.margePct !== null && (
                <div className="flex items-center gap-1.5 bg-emerald-50 rounded-lg px-3 py-1.5">
                  <span className="text-[10px] text-emerald-500 uppercase tracking-wide">Marge</span>
                  <strong className="text-sm text-emerald-700">+{detailModal.stats.margePct.toFixed(0)}%</strong>
                </div>
              )}
              <div className="flex items-center gap-1.5 bg-gray-100 rounded-lg px-3 py-1.5">
                <span className="text-[10px] text-gray-500 uppercase tracking-wide">Budget</span>
                <strong className="text-sm text-gray-800">{detailModal.stats.budgetMensuel}€/mois</strong>
              </div>
            </div>
          </div>

          {/* Main content: sidebar + detail */}
          <div className="flex flex-1 overflow-hidden">
            {/* Sidebar — liste des pubs */}
            <div className="w-80 shrink-0 border-r border-gray-200 bg-gray-50 flex flex-col overflow-hidden">
              <div className="px-3 py-2 border-b border-gray-200 text-[11px] font-semibold text-gray-500 uppercase tracking-wide">
                {modalAllItems.length} publicité{modalAllItems.length > 1 ? 's' : ''}
              </div>
              <div className="flex-1 overflow-y-auto">
                {detailModal.loading ? (
                  <div className="flex items-center justify-center py-12">
                    <div className="animate-spin rounded-full h-6 w-6 border-2 border-blue-500 border-t-transparent"></div>
                  </div>
                ) : modalAllItems.length === 0 ? (
                  <p className="text-xs text-gray-400 text-center py-8">Aucune pub trouvée</p>
                ) : (
                  modalAllItems.map(item => (
                    <div
                      key={`${item.source}-${item.id}`}
                      className={`px-3 py-2.5 cursor-pointer border-b border-gray-100 transition-colors ${detailModalSelected === item.id ? 'bg-white border-l-2 border-l-blue-500' : 'hover:bg-white border-l-2 border-l-transparent'}`}
                      onClick={() => setDetailModalSelected(item.id)}
                    >
                      <div className="flex items-center gap-2 mb-1">
                        <span className={`shrink-0 w-1.5 h-1.5 rounded-full ${item.source === 'meta' ? 'bg-blue-500' : 'bg-orange-500'}`}></span>
                        <span className="text-xs font-medium text-gray-900 truncate">{item.name}</span>
                      </div>
                      <div className="flex items-center gap-3 ml-3.5 text-[11px]">
                        <span className="text-gray-500">{item.spend.toFixed(0)}€</span>
                        <span className="text-gray-500">{item.leads} lead{item.leads > 1 ? 's' : ''}</span>
                        {item.cpl !== null ? (
                          <span className={`font-semibold ${item.cpl > 100 ? 'text-red-600' : 'text-emerald-600'}`}>{item.cpl.toFixed(0)}€/lead</span>
                        ) : (
                          <span className="text-gray-300">—</span>
                        )}
                        <span className="ml-auto font-medium text-gray-400">{item.budget.toFixed(0)}€/j</span>
                      </div>
                    </div>
                  ))
                )}

                {/* Historique en bas de la sidebar */}
                {detailModal.data?.history && detailModal.data.history.length > 0 && (
                  <div
                    className={`px-3 py-2.5 cursor-pointer border-b border-gray-100 transition-colors ${detailModalSelected === '__history__' ? 'bg-white border-l-2 border-l-purple-500' : 'hover:bg-white border-l-2 border-l-transparent'}`}
                    onClick={() => setDetailModalSelected('__history__')}
                  >
                    <div className="flex items-center gap-2">
                      <span className="shrink-0 w-1.5 h-1.5 rounded-full bg-purple-500"></span>
                      <span className="text-xs font-medium text-gray-700">Historique ({detailModal.data.history.length})</span>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Right panel — détail de l'item sélectionné */}
            <div className="flex-1 overflow-y-auto p-6">
              {/* Courbe CPL mensuel */}
              {cplMonthlyLoading && (
                <div className="mb-6 bg-gray-50 rounded-xl p-4 border border-gray-200 flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-5 w-5 border-2 border-purple-500 border-t-transparent"></div>
                  <span className="ml-2 text-xs text-gray-400">Chargement CPL mensuel...</span>
                </div>
              )}
              {cplMonthlyData && cplMonthlyData.some(d => d.leads > 0) && (
                <div className="mb-6 bg-gray-50 rounded-xl p-4 border border-gray-200">
                  <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">
                    CPL vendeur mensuel (12 derniers mois)
                  </h3>
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={cplMonthlyData.map(d => ({
                      ...d,
                      label: new Date(d.month + '-01').toLocaleDateString('fr-FR', { month: 'short', year: '2-digit' }),
                      cplDisplay: d.cpl ?? 0
                    }))}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis dataKey="label" tick={{ fontSize: 10 }} stroke="#9ca3af" />
                      <YAxis tick={{ fontSize: 10 }} stroke="#9ca3af" tickFormatter={(v: number) => `${v}€`} />
                      <Tooltip
                        contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px', fontSize: '12px' }}
                        formatter={(value: any, name: any) => {
                          const v = Number(value)
                          if (name === 'CPL') return [v > 0 ? `${v.toFixed(0)}€` : 'Pas de leads', 'CPL']
                          return [v, name]
                        }}
                      />
                      <Bar dataKey="cplDisplay" name="CPL" fill="#8b5cf6" radius={[4, 4, 0, 0]} opacity={0.8} />
                      {cplMonthlyAverage !== null && (
                        <ReferenceLine
                          y={cplMonthlyAverage}
                          stroke="#ef4444"
                          strokeDasharray="5 5"
                          strokeWidth={2}
                          label={{ value: `Moy. ${cplMonthlyAverage}€`, position: 'right', fill: '#ef4444', fontSize: 11 }}
                        />
                      )}
                    </ComposedChart>
                  </ResponsiveContainer>
                  <div className="flex items-center gap-4 mt-2 text-[11px] text-gray-500">
                    <span>Budget: <strong className="text-gray-700">{cplMonthlyData.reduce((s, d) => s + d.budget, 0).toFixed(0)}€</strong></span>
                    <span>Leads vendeur: <strong className="text-gray-700">{cplMonthlyData.reduce((s, d) => s + d.leads, 0).toFixed(0)}</strong></span>
                    <span>Mois avec leads: <strong className="text-gray-700">{cplMonthlyData.filter(d => d.leads > 0).length}/12</strong></span>
                    {cplMonthlyAverage !== null && <span>CPL moyen: <strong className="text-purple-700">{cplMonthlyAverage}€</strong></span>}
                  </div>
                </div>
              )}

              {detailModal.loading ? (
                <div className="flex items-center justify-center py-16">
                  <div className="animate-spin rounded-full h-8 w-8 border-2 border-blue-500 border-t-transparent"></div>
                  <span className="ml-3 text-gray-500">Chargement...</span>
                </div>
              ) : !detailModalSelected || (!modalSelectedItem && detailModalSelected !== '__history__') ? (
                <div className="flex flex-col items-center justify-center h-full text-gray-400">
                  <svg className="w-12 h-12 mb-3 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5M7.188 2.239l.777 2.897M5.136 7.965l-2.898-.777M13.95 4.05l-2.122 2.122m-5.657 5.656l-2.12 2.122" /></svg>
                  <p className="text-sm">Sélectionnez une pub dans la liste</p>
                </div>
              ) : detailModalSelected === '__history__' ? (
                /* Historique */
                <div>
                  <h3 className="text-sm font-bold text-gray-800 mb-4">Historique des modifications</h3>
                  <table className="w-full text-xs">
                    <thead className="bg-gray-50 sticky top-0">
                      <tr className="text-gray-500">
                        <th className="text-left py-2 px-3 font-medium">Date</th>
                        <th className="text-left py-2 px-3 font-medium">Source</th>
                        <th className="text-left py-2 px-3 font-medium">Action</th>
                        <th className="text-left py-2 px-3 font-medium">Objet</th>
                        <th className="text-left py-2 px-3 font-medium">Détails</th>
                        <th className="text-left py-2 px-3 font-medium">Par</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detailModal.data!.history.map((ev, i) => (
                        <tr key={i} className="border-t border-gray-100 hover:bg-gray-50">
                          <td className="py-1.5 px-3 text-gray-500 whitespace-nowrap">
                            {new Date(ev.date).toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: '2-digit' })}{' '}
                            <span className="text-gray-400">{new Date(ev.date).toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' })}</span>
                          </td>
                          <td className="py-1.5 px-3">
                            <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${ev.source === 'meta' ? 'bg-blue-100 text-blue-700' : ev.source === 'google' ? 'bg-orange-100 text-orange-700' : 'bg-purple-100 text-purple-700'}`}>
                              {ev.source === 'meta' ? 'Meta' : ev.source === 'google' ? 'Google' : 'Interne'}
                            </span>
                          </td>
                          <td className="py-1.5 px-3 text-gray-800">{ev.description}</td>
                          <td className="py-1.5 px-3 text-gray-600">{ev.entity_name}</td>
                          <td className="py-1.5 px-3 text-gray-500">{ev.details || '—'}</td>
                          <td className="py-1.5 px-3 text-gray-400">{ev.actor || '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : modalSelectedMeta ? (
                /* Détail Meta adset */
                <div className="space-y-5">
                  {/* Header adset */}
                  <div className="flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-blue-500"></span>
                    <h3 className="text-base font-bold text-gray-900">{modalSelectedMeta.name}</h3>
                    <span className={`text-[10px] px-2 py-0.5 rounded-full font-medium ${modalSelectedMeta.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : modalSelectedMeta.status === 'PAUSED' ? 'bg-yellow-100 text-yellow-700' : 'bg-gray-100 text-gray-500'}`}>{modalSelectedMeta.status}</span>
                  </div>

                  {/* KPI cards */}
                  <div className="grid grid-cols-4 gap-3">
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Budget/jour</p>
                      {editingBudget?.id === modalSelectedMeta.id ? (
                        <div className="flex items-center justify-center gap-1">
                          <input
                            type="text"
                            className="w-20 text-center text-lg font-bold border border-blue-300 rounded px-1 py-0.5 focus:outline-none focus:ring-2 focus:ring-blue-400"
                            defaultValue={editingBudget.value}
                            autoFocus
                            onKeyDown={(ev) => {
                              if (ev.key === 'Enter') handleBudgetSave('meta', modalSelectedMeta.id, (ev.target as HTMLInputElement).value, modalSelectedMeta.name, modalSelectedMeta.daily_budget)
                              if (ev.key === 'Escape') setEditingBudget(null)
                            }}
                            onBlur={(ev) => handleBudgetSave('meta', modalSelectedMeta.id, ev.target.value, modalSelectedMeta.name, modalSelectedMeta.daily_budget)}
                          />
                          <span className="text-lg font-bold text-gray-900">€</span>
                        </div>
                      ) : (
                        <button
                          className="text-lg font-bold text-gray-900 hover:text-blue-600 transition-colors"
                          onClick={() => setEditingBudget({ source: 'meta', id: modalSelectedMeta.id, value: modalSelectedMeta.daily_budget.toFixed(2) })}
                        >
                          {savingBudget === modalSelectedMeta.id ? '...' : `${modalSelectedMeta.daily_budget.toFixed(2)}€`}
                        </button>
                      )}
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Dépensé</p>
                      <p className="text-lg font-bold text-blue-700">{modalSelectedItem?.spend.toFixed(0) || 0}€</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Résultats</p>
                      <p className="text-lg font-bold text-blue-700">{modalSelectedItem?.leads || 0}</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">CPL</p>
                      <p className={`text-lg font-bold ${modalSelectedItem?.cpl !== null && (modalSelectedItem?.cpl || 0) > 100 ? 'text-red-600' : 'text-emerald-600'}`}>
                        {modalSelectedItem?.cpl !== null && modalSelectedItem?.cpl !== undefined ? `${modalSelectedItem.cpl.toFixed(0)}€` : '—'}
                      </p>
                    </div>
                  </div>

                  {/* Ciblage */}
                  {(modalSelectedMeta.targeting.age_min || modalSelectedMeta.targeting.age_max || modalSelectedMeta.targeting.locations.length > 0 || modalSelectedMeta.targeting.interests.length > 0 || modalSelectedMeta.targeting.custom_audiences.length > 0) && (
                    <div>
                      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Ciblage</h4>
                      <div className="bg-blue-50/60 rounded-lg p-3 text-xs text-gray-700 space-y-1">
                        {(modalSelectedMeta.targeting.age_min || modalSelectedMeta.targeting.age_max) && (
                          <p><span className="text-gray-500">Âge :</span> {modalSelectedMeta.targeting.age_min || '?'} — {modalSelectedMeta.targeting.age_max || '?'} ans</p>
                        )}
                        {modalSelectedMeta.targeting.locations.length > 0 && (
                          <p><span className="text-gray-500">Zones :</span> {modalSelectedMeta.targeting.locations.join(', ')}</p>
                        )}
                        {modalSelectedMeta.targeting.interests.length > 0 && (
                          <p><span className="text-gray-500">Intérêts :</span> {modalSelectedMeta.targeting.interests.join(', ')}</p>
                        )}
                        {modalSelectedMeta.targeting.custom_audiences.length > 0 && (
                          <p><span className="text-gray-500">Audiences :</span> {modalSelectedMeta.targeting.custom_audiences.join(', ')}</p>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Publicités */}
                  <div>
                    <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Publicités ({modalSelectedMeta.ads.length})</h4>
                    {modalSelectedMeta.ads.length > 0 ? (
                      <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                        {modalSelectedMeta.ads.map(ad => (
                          <div key={ad.id} className="border border-gray-200 rounded-xl overflow-hidden bg-white shadow-sm">
                            {ad.image_url && (
                              <img src={ad.image_url} alt={ad.name} className="w-full h-48 object-cover" loading="lazy" />
                            )}
                            <div className="p-3 space-y-2">
                              <div className="flex items-center justify-between">
                                <p className="text-xs font-semibold text-gray-900 truncate flex-1">{ad.name}</p>
                                <span className={`text-[10px] px-1.5 py-0.5 rounded-full shrink-0 ml-2 ${ad.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : ad.status === 'PAUSED' ? 'bg-yellow-100 text-yellow-700' : 'bg-gray-100 text-gray-500'}`}>{ad.status}</span>
                              </div>
                              {ad.titles.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Titres</span>
                                  <div className="flex flex-wrap gap-1 mt-0.5">
                                    {ad.titles.map((t, i) => (
                                      <span key={i} className="bg-blue-50 text-blue-800 px-2 py-0.5 rounded-md text-xs">{t}</span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {ad.bodies.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Textes</span>
                                  <div className="mt-0.5 space-y-1">
                                    {ad.bodies.map((b, i) => (
                                      <p key={i} className="text-xs text-gray-700 bg-gray-50 rounded-md px-2 py-1">{b}</p>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {ad.descriptions.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Descriptions</span>
                                  <div className="mt-0.5 space-y-1">
                                    {ad.descriptions.map((d, i) => (
                                      <p key={i} className="text-xs text-gray-600 italic bg-gray-50 rounded-md px-2 py-1">{d}</p>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="text-xs text-gray-400 italic">Aucune publicité trouvée</p>
                    )}
                  </div>
                </div>
              ) : modalSelectedGoogle ? (
                /* Détail Google campaign */
                <div className="space-y-5">
                  {/* Header campaign */}
                  <div className="flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-orange-500"></span>
                    <h3 className="text-base font-bold text-gray-900">{modalSelectedGoogle.name}</h3>
                    <span className="text-[10px] px-2 py-0.5 rounded-full font-medium bg-orange-100 text-orange-700">
                      {modalSelectedGoogle.channel_type === 'PERFORMANCE_MAX' ? 'PMax' : modalSelectedGoogle.channel_type === 'SEARCH' ? 'Search' : modalSelectedGoogle.channel_type}
                    </span>
                  </div>

                  {/* KPI cards */}
                  <div className="grid grid-cols-4 gap-3">
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Budget/jour</p>
                      {editingBudget?.id === modalSelectedGoogle.id ? (
                        <div className="flex items-center justify-center gap-1">
                          <input
                            type="text"
                            className="w-20 text-center text-lg font-bold border border-orange-300 rounded px-1 py-0.5 focus:outline-none focus:ring-2 focus:ring-orange-400"
                            defaultValue={editingBudget.value}
                            autoFocus
                            onKeyDown={(ev) => {
                              if (ev.key === 'Enter') handleBudgetSave('google', modalSelectedGoogle.id, (ev.target as HTMLInputElement).value, modalSelectedGoogle.name, modalSelectedGoogle.daily_budget)
                              if (ev.key === 'Escape') setEditingBudget(null)
                            }}
                            onBlur={(ev) => handleBudgetSave('google', modalSelectedGoogle.id, ev.target.value, modalSelectedGoogle.name, modalSelectedGoogle.daily_budget)}
                          />
                          <span className="text-lg font-bold text-gray-900">€</span>
                        </div>
                      ) : (
                        <button
                          className="text-lg font-bold text-gray-900 hover:text-orange-600 transition-colors"
                          onClick={() => setEditingBudget({ source: 'google', id: modalSelectedGoogle.id, value: modalSelectedGoogle.daily_budget.toFixed(2) })}
                        >
                          {savingBudget === modalSelectedGoogle.id ? '...' : `${modalSelectedGoogle.daily_budget.toFixed(2)}€`}
                        </button>
                      )}
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Dépensé</p>
                      <p className="text-lg font-bold text-orange-700">{modalSelectedItem?.spend.toFixed(0) || 0}€</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Résultats</p>
                      <p className="text-lg font-bold text-orange-700">{modalSelectedItem?.leads || 0}</p>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <p className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">CPL</p>
                      <p className={`text-lg font-bold ${modalSelectedItem?.cpl !== null && (modalSelectedItem?.cpl || 0) > 100 ? 'text-red-600' : 'text-emerald-600'}`}>
                        {modalSelectedItem?.cpl !== null && modalSelectedItem?.cpl !== undefined ? `${modalSelectedItem.cpl.toFixed(0)}€` : '—'}
                      </p>
                    </div>
                  </div>

                  {/* Ciblage géo */}
                  {modalSelectedGoogle.locations.length > 0 && (
                    <div>
                      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Ciblage géographique</h4>
                      <div className="bg-orange-50/60 rounded-lg p-3 text-xs text-gray-700">
                        {modalSelectedGoogle.locations.join(', ')}
                      </div>
                    </div>
                  )}

                  {/* Ad groups (Search) */}
                  {modalSelectedGoogle.ad_groups.length > 0 && (
                    <div>
                      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Groupes d'annonces ({modalSelectedGoogle.ad_groups.length})</h4>
                      <div className="space-y-3">
                        {modalSelectedGoogle.ad_groups.map((ag, i) => (
                          <div key={i} className="border border-gray-200 rounded-xl p-4 bg-white">
                            <p className="text-sm font-medium text-gray-800 mb-3">{ag.name}</p>
                            {ag.ads.map((ad, j) => (
                              <div key={j} className="space-y-2 mb-3 last:mb-0">
                                {ad.headlines.length > 0 && (
                                  <div>
                                    <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Titres</span>
                                    <div className="flex flex-wrap gap-1 mt-0.5">
                                      {ad.headlines.map((h, k) => (
                                        <span key={k} className="bg-orange-50 text-orange-800 px-2 py-0.5 rounded-md text-xs">{h}</span>
                                      ))}
                                    </div>
                                  </div>
                                )}
                                {ad.descriptions.length > 0 && (
                                  <div>
                                    <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Descriptions</span>
                                    <div className="flex flex-wrap gap-1 mt-0.5">
                                      {ad.descriptions.map((d, k) => (
                                        <span key={k} className="bg-gray-100 text-gray-700 px-2 py-0.5 rounded-md text-xs">{d}</span>
                                      ))}
                                    </div>
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Asset groups (PMax) */}
                  {modalSelectedGoogle.asset_groups.length > 0 && (
                    <div>
                      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Groupes d'assets ({modalSelectedGoogle.asset_groups.length})</h4>
                      <div className="space-y-4">
                        {modalSelectedGoogle.asset_groups.map((ag, i) => (
                          <div key={i} className="border border-gray-200 rounded-xl p-4 bg-white">
                            <p className="text-sm font-medium text-gray-800 mb-3">{ag.name}</p>
                            <div className="space-y-2">
                              {ag.headlines.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Titres</span>
                                  <div className="flex flex-wrap gap-1 mt-0.5">
                                    {ag.headlines.map((h, k) => (
                                      <span key={k} className="bg-orange-50 text-orange-800 px-2 py-0.5 rounded-md text-xs">{h}</span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {ag.long_headlines.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Titres longs</span>
                                  <div className="flex flex-wrap gap-1 mt-0.5">
                                    {ag.long_headlines.map((h, k) => (
                                      <span key={k} className="bg-orange-100 text-orange-900 px-2 py-0.5 rounded-md text-xs">{h}</span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {ag.descriptions.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Descriptions</span>
                                  <div className="flex flex-wrap gap-1 mt-0.5">
                                    {ag.descriptions.map((d, k) => (
                                      <span key={k} className="bg-gray-100 text-gray-700 px-2 py-0.5 rounded-md text-xs">{d}</span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {ag.images.length > 0 && (
                                <div>
                                  <span className="text-gray-400 text-[10px] uppercase tracking-wide font-medium">Images</span>
                                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2 mt-1">
                                    {ag.images.map((url, k) => (
                                      <img key={k} src={url} alt="" className="w-full h-36 object-cover rounded-lg border border-gray-200" loading="lazy" />
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {modalSelectedGoogle.ad_groups.length === 0 && modalSelectedGoogle.asset_groups.length === 0 && (
                    <p className="text-sm text-gray-400 italic">Aucune annonce trouvée pour cette campagne</p>
                  )}
                </div>
              ) : null}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default GestionPubPage
