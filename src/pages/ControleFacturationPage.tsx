import { useState, useEffect, useMemo, useCallback } from 'react'
import { RefreshCw, CheckCircle, XCircle, Search, ChevronDown, ChevronUp, ExternalLink, Link2, Trash2, AlertTriangle, Pencil } from 'lucide-react'

// --- Types ---

interface ZohoInvoice {
  invoice_id: string
  invoice_number: string
  date: string
  due_date: string
  status: string
  total: number
  balance: number
  country?: string | null
}

interface ZohoCustomer {
  customer_name: string
  customer_id: string
  gcl_id: string | null
  created_time: string | null
  invoices: ZohoInvoice[]
  total_invoiced: number
  last_invoice_date: string | null
}

interface V2Agency {
  nom: string
  gcl_id: string | null
  end_date: string | null
  start_date: string | null
  normalized: string
}

interface ZohoData {
  customers: Record<string, ZohoCustomer>
  total_invoices: number
  total_contacts: number
  v2_agencies?: V2Agency[]
}

interface Campaign {
  source: 'meta' | 'google'
  name: string
  normalized: string
  status: string
  daily_budget: number
  url?: string
  account_name?: string
  created_time?: string | null
}

interface CampaignsData { meta: Campaign[]; google: Campaign[] }

interface ClientGroup {
  name: string
  normalized: string
  campaigns: Campaign[]
  metaCount: number
  googleCount: number
  totalDailyBudget: number
  zohoName: string | null
  matchMethod: 'gcl' | 'name' | 'manual' | null
  recentInvoice: ZohoInvoice | null
  hasRecentInvoice: boolean
}

interface ActiveClient {
  nom: string
  normalized: string
  gcl_id: string | null
  budget_mensuel: number
  tarifs: { code_postal: string; tarif: number }[]
  start_date: string | null
  end_date: string | null
}

interface ActiveClientsData { clients: ActiveClient[]; total: number }

interface BudgetRow {
  client: ActiveClient
  zohoName: string | null
  matchMethod: 'gcl' | 'name' | 'manual' | null
  recentInvoice: ZohoInvoice | null
  invoiceAmount: number
  budgetMatch: 'ok' | 'mismatch' | 'no_invoice' | 'no_match'
  diff: number // invoice - budget
  pctDiff: number // % écart
}

type CampSortField = 'status' | 'name' | 'budget' | 'zoho' | 'invoice'
type BudgetSortField = 'status' | 'name' | 'budget' | 'zoho' | 'invoice' | 'diff' | 'pctDiff'

// --- Helpers ---

function normalize(name: string): string {
  return name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]/g, '')
}

function formatDate(d: string): string {
  return new Date(d).toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' })
}

function formatMoney(n: number): string {
  return new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR' }).format(n)
}

const NOISE_WORDS = new Set(['copie', 'copy', 'avenant', 'new', 'pmax', 'leads', 'performance', 'max', 'smart', 'campagne', 'adgroup', 'ensemble', 'exp', 'immo', 'immobilier', 'immobiliere', 'agence', 'groupe', 'group', 'conseil', 'proprietes', 'privees', 'reseaux', 'reseau', 'safti', 'iad', 'orpi', 'century', 'laforet', 'guy', 'hoquet', 'stephane', 'plaza', 'era', 'keller', 'williams', 'solvimo', 'capifrance'])

// --- Zoho matching helper (shared between tabs) ---
function findZohoMatch(
  norm: string,
  originalName: string | null,
  gclId: string | null,
  zohoByNorm: Map<string, { name: string; customer: ZohoCustomer }>,
  zohoByGcl: Map<string, { name: string; customer: ZohoCustomer }>,
  overrides: Record<string, string>
): { zohoName: string | null; customer: ZohoCustomer | null; method: 'gcl' | 'name' | 'manual' | null } {
  // Override manuel
  if (overrides[norm]) {
    const m = zohoByNorm.get(normalize(overrides[norm])) || [...zohoByNorm.values()].find(v => v.name === overrides[norm])
    if (m) return { zohoName: m.name, customer: m.customer, method: 'manual' }
  }
  // GCL ID
  if (gclId) {
    const m = zohoByGcl.get(gclId)
    if (m) return { zohoName: m.name, customer: m.customer, method: 'gcl' }
  }
  // Nom exact
  const exact = zohoByNorm.get(norm)
  if (exact) return { zohoName: exact.name, customer: exact.customer, method: 'name' }
  // Nom partiel
  if (norm.length >= 3) {
    for (const [zn, m] of zohoByNorm) {
      if (zn.length >= 3 && (zn.includes(norm) || norm.includes(zn)))
        return { zohoName: m.name, customer: m.customer, method: 'name' }
    }
  }
  // Mot-clé unique
  if (originalName) {
    const words = originalName.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .split(/[\s\-_.,;:()[\]]+/).map(w => w.replace(/[^a-z]/g, '')).filter(w => w.length >= 3 && !NOISE_WORDS.has(w))
    for (const word of words) {
      const matches: { name: string; customer: ZohoCustomer }[] = []
      for (const [zn, m] of zohoByNorm) { if (zn.includes(word)) matches.push(m) }
      if (matches.length === 1) return { zohoName: matches[0].name, customer: matches[0].customer, method: 'name' }
    }
  }
  return { zohoName: null, customer: null, method: null }
}

// --- Main Component ---

function ControleFacturationPage() {
  const [tab, setTab] = useState<'campaigns' | 'budgets'>('campaigns')
  const [zohoData, setZohoData] = useState<ZohoData | null>(null)
  const [campaignsData, setCampaignsData] = useState<CampaignsData | null>(null)
  const [activeClientsData, setActiveClientsData] = useState<ActiveClientsData | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [showOnlyProblems, setShowOnlyProblems] = useState(false)
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())
  const [overrides, setOverrides] = useState<Record<string, string>>({})
  const [matchingRow, setMatchingRow] = useState<string | null>(null)
  const [campSort, setCampSort] = useState<{ field: CampSortField; dir: 'asc' | 'desc' }>({ field: 'status', dir: 'asc' })
  const [budgetSort, setBudgetSort] = useState<{ field: BudgetSortField; dir: 'asc' | 'desc' }>({ field: 'status', dir: 'asc' })

  const API_URL = import.meta.env.VITE_API_URL || '/api'

  const fetchOverrides = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/invoice-matching-overrides`)
      if (r.ok) setOverrides(await r.json())
    } catch { /* ignore */ }
  }, [API_URL])

  const fetchData = async (refresh = false) => {
    if (refresh) setRefreshing(true)
    else setLoading(true)
    setError(null)
    try {
      const qs = refresh ? '?refresh=1' : ''
      const [zRes, cRes, aRes] = await Promise.all([
        fetch(`${API_URL}/controle/zoho-invoices${qs}`),
        fetch(`${API_URL}/controle/active-campaigns${qs}`),
        fetch(`${API_URL}/controle/active-clients${qs}`),
      ])
      await fetchOverrides()
      if (!zRes.ok) throw new Error(`Zoho ${zRes.status}`)
      if (!cRes.ok) throw new Error(`Campaigns ${cRes.status}`)
      setZohoData(await zRes.json())
      setCampaignsData(await cRes.json())
      if (aRes.ok) setActiveClientsData(await aRes.json())
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  const saveOverride = async (key: string, zohoName: string) => {
    await fetch(`${API_URL}/invoice-matching-overrides`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ campaign_normalized: key, zoho_customer_name: zohoName })
    })
    setOverrides(prev => ({ ...prev, [key]: zohoName }))
    setMatchingRow(null)
  }

  const deleteOverride = async (key: string) => {
    await fetch(`${API_URL}/invoice-matching-overrides`, {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ campaign_normalized: key })
    })
    setOverrides(prev => { const n = { ...prev }; delete n[key]; return n })
  }

  useEffect(() => { fetchData() }, [])

  // Zoho indexes (shared)
  const { zohoByNorm, zohoByGcl } = useMemo(() => {
    const byNorm = new Map<string, { name: string; customer: ZohoCustomer }>()
    const byGcl = new Map<string, { name: string; customer: ZohoCustomer }>()
    if (zohoData) {
      for (const [name, customer] of Object.entries(zohoData.customers)) {
        const n = normalize(name)
        if (n) byNorm.set(n, { name, customer })
        if (customer.gcl_id) byGcl.set(customer.gcl_id, { name, customer })
      }
    }
    return { zohoByNorm: byNorm, zohoByGcl: byGcl }
  }, [zohoData])

  const since31d = useMemo(() => {
    const d = new Date(); d.setDate(d.getDate() - 31)
    return d.toISOString().slice(0, 10)
  }, [])

  // ==================== TAB 1: Campaigns ====================
  const groups = useMemo((): ClientGroup[] => {
    if (!campaignsData) return []
    const active = [...campaignsData.meta, ...campaignsData.google].filter(c => c.status === 'ACTIVE' || c.status === 'ENABLED')
    const byNorm = new Map<string, Campaign[]>()
    for (const c of active) { if (c.normalized) { if (!byNorm.has(c.normalized)) byNorm.set(c.normalized, []); byNorm.get(c.normalized)!.push(c) } }

    const v2ByNorm = new Map<string, V2Agency>()
    const v2ByGcl = new Map<string, V2Agency>()
    if (zohoData?.v2_agencies) {
      for (const a of zohoData.v2_agencies) { if (a.normalized) v2ByNorm.set(a.normalized, a); if (a.gcl_id) v2ByGcl.set(a.gcl_id, a) }
    }

    const result: ClientGroup[] = []
    for (const [norm, camps] of byNorm) {
      // Resolve GCL from V2
      const v2 = v2ByNorm.get(norm) || (() => { if (norm.length < 3) return null; for (const [v2n, a] of v2ByNorm) { if (v2n.length >= 3 && (v2n.includes(norm) || norm.includes(v2n))) return a } return null })()

      // Exclure les clients V2 créés il y a moins de 30 jours
      if (v2?.start_date && v2.start_date > since31d) continue
      // Exclure si pas de V2 et toutes les campagnes créées il y a moins de 30 jours
      if (!v2) {
        const allRecent = camps.every(c => {
          if (!c.created_time) return false
          const created = c.created_time.slice(0, 10)
          return created > since31d
        })
        if (allRecent) continue
      }

      const gclId = v2?.gcl_id || null
      const { zohoName, customer: zohoCustomer, method: matchMethod } = findZohoMatch(norm, camps[0].name, gclId, zohoByNorm, zohoByGcl, overrides)
      let recentInvoice: ZohoInvoice | null = null
      if (zohoCustomer) {
        const recent = zohoCustomer.invoices.filter(inv => inv.date >= since31d).sort((a, b) => b.date.localeCompare(a.date))
        recentInvoice = recent[0] || null
      }
      result.push({
        name: camps[0].name, normalized: norm, campaigns: camps,
        metaCount: camps.filter(c => c.source === 'meta').length,
        googleCount: camps.filter(c => c.source === 'google').length,
        totalDailyBudget: camps.reduce((s, c) => s + c.daily_budget, 0),
        zohoName, matchMethod, recentInvoice, hasRecentInvoice: !!recentInvoice
      })
    }
    return result
  }, [campaignsData, zohoData, since31d, overrides, zohoByNorm, zohoByGcl])

  // ==================== TAB 2: Budgets ====================
  const budgetRows = useMemo((): BudgetRow[] => {
    if (!activeClientsData || !zohoData) return []
    const result: BudgetRow[] = []
    for (const client of activeClientsData.clients) {
      const { zohoName, customer, method } = findZohoMatch(client.normalized, client.nom, client.gcl_id, zohoByNorm, zohoByGcl, overrides)
      let recentInvoice: ZohoInvoice | null = null
      if (customer) {
        const recent = customer.invoices.filter(inv => inv.date >= since31d).sort((a, b) => b.date.localeCompare(a.date))
        recentInvoice = recent[0] || null
      }
      // Zoho total est TTC pour la France, HT pour les autres pays → convertir en HT si France
      const isFrance = !recentInvoice?.country || recentInvoice.country.toLowerCase() === 'france'
      const invoiceAmountHT = recentInvoice ? Math.round((isFrance ? recentInvoice.total / 1.20 : recentInvoice.total) * 100) / 100 : 0
      const invoiceAmount = invoiceAmountHT
      const diff = invoiceAmount - client.budget_mensuel
      let budgetMatch: BudgetRow['budgetMatch'] = 'ok'
      if (!customer) budgetMatch = 'no_match'
      else if (!recentInvoice) budgetMatch = 'no_invoice'
      else if (Math.abs(diff) > 1) budgetMatch = 'mismatch' // tolérance 1€
      const pctDiff = client.budget_mensuel > 0 && recentInvoice ? Math.round((diff / client.budget_mensuel) * 100) : 0
      result.push({ client, zohoName, matchMethod: method, recentInvoice, invoiceAmount, budgetMatch, diff, pctDiff })
    }
    result.sort((a, b) => {
      const order = { no_match: 0, no_invoice: 1, mismatch: 2, ok: 3 }
      return order[a.budgetMatch] - order[b.budgetMatch] || a.client.nom.localeCompare(b.client.nom, 'fr')
    })
    return result
  }, [activeClientsData, zohoData, since31d, overrides, zohoByNorm, zohoByGcl])

  // Filtered
  const filtered = useMemo(() => {
    let rows = groups
    if (showOnlyProblems) rows = rows.filter(r => !r.hasRecentInvoice)
    if (search) { const s = normalize(search); rows = rows.filter(r => r.normalized.includes(s) || (r.zohoName && normalize(r.zohoName).includes(s))) }
    const { field, dir } = campSort
    rows = [...rows].sort((a, b) => {
      let cmp = 0
      if (field === 'status') {
        // Problèmes d'abord, triés par budget décroissant
        cmp = (a.hasRecentInvoice ? 1 : 0) - (b.hasRecentInvoice ? 1 : 0)
        if (cmp === 0 && !a.hasRecentInvoice) cmp = b.totalDailyBudget - a.totalDailyBudget // plus gros budget en premier
      }
      else if (field === 'name') cmp = a.name.localeCompare(b.name, 'fr')
      else if (field === 'budget') cmp = a.totalDailyBudget - b.totalDailyBudget
      else if (field === 'zoho') cmp = (a.zohoName || '').localeCompare(b.zohoName || '', 'fr')
      else if (field === 'invoice') {
        const aHas = a.recentInvoice ? 0 : 1
        const bHas = b.recentInvoice ? 0 : 1
        if (aHas !== bHas) return aHas - bHas
        const toHT = (inv: ZohoInvoice) => { const fr = !inv.country || inv.country.toLowerCase() === 'france'; return fr ? inv.total / 1.20 : inv.total }
        cmp = toHT(a.recentInvoice!) - toHT(b.recentInvoice!)
      }
      const result = dir === 'asc' ? cmp : -cmp
      return result !== 0 ? result : a.name.localeCompare(b.name, 'fr')
    })
    return rows
  }, [groups, showOnlyProblems, search, campSort])

  const filteredBudget = useMemo(() => {
    let rows = budgetRows
    if (showOnlyProblems) rows = rows.filter(r => r.budgetMatch !== 'ok')
    if (search) { const s = normalize(search); rows = rows.filter(r => r.client.normalized.includes(s) || (r.zohoName && normalize(r.zohoName).includes(s))) }
    const { field, dir } = budgetSort
    const statusOrder = { no_match: 0, no_invoice: 1, mismatch: 2, ok: 3 }
    rows = [...rows].sort((a, b) => {
      let cmp = 0
      if (field === 'status') cmp = statusOrder[a.budgetMatch] - statusOrder[b.budgetMatch]
      else if (field === 'name') cmp = a.client.nom.localeCompare(b.client.nom, 'fr')
      else if (field === 'budget') cmp = a.client.budget_mensuel - b.client.budget_mensuel
      else if (field === 'zoho') cmp = (a.zohoName || '').localeCompare(b.zohoName || '', 'fr')
      else if (field === 'invoice' || field === 'diff' || field === 'pctDiff') {
        // Lignes sans facture/match toujours en fin, quel que soit le sens du tri
        const aOrder = a.budgetMatch === 'no_match' ? 2 : a.budgetMatch === 'no_invoice' ? 1 : 0
        const bOrder = b.budgetMatch === 'no_match' ? 2 : b.budgetMatch === 'no_invoice' ? 1 : 0
        if (aOrder !== bOrder) return aOrder - bOrder
        if (field === 'invoice') cmp = a.invoiceAmount - b.invoiceAmount
        else if (field === 'diff') cmp = a.diff - b.diff
        else cmp = a.pctDiff - b.pctDiff
      }
      const result = dir === 'asc' ? cmp : -cmp
      return result !== 0 ? result : a.client.nom.localeCompare(b.client.nom, 'fr')
    })
    return rows
  }, [budgetRows, showOnlyProblems, search, budgetSort])

  // Stats tab 1
  const problems1 = groups.filter(g => !g.hasRecentInvoice)
  const problemsNoMatch = problems1.filter(g => !g.zohoName)
  const problemsNoInvoice = problems1.filter(g => g.zohoName)
  const problemBudgetDay = problems1.reduce((s, g) => s + g.totalDailyBudget, 0)

  // Stats tab 2
  const budgetStats = useMemo(() => ({
    total: budgetRows.length,
    ok: budgetRows.filter(r => r.budgetMatch === 'ok').length,
    mismatch: budgetRows.filter(r => r.budgetMatch === 'mismatch').length,
    noInvoice: budgetRows.filter(r => r.budgetMatch === 'no_invoice').length,
    noMatch: budgetRows.filter(r => r.budgetMatch === 'no_match').length,
    missingBudget: budgetRows.filter(r => r.budgetMatch !== 'ok').reduce((s, r) => s + r.client.budget_mensuel, 0)
  }), [budgetRows])

  if (loading) {
    return <div className="flex items-center justify-center h-96"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto" /><p className="text-gray-600 ml-4">Chargement...</p></div>
  }
  if (error) {
    return <div className="bg-red-50 border border-red-200 rounded-xl p-6"><p className="text-red-700 font-medium">{error}</p><button onClick={() => fetchData()} className="mt-3 px-4 py-2 bg-red-600 text-white rounded-lg text-sm">Reessayer</button></div>
  }

  return (
    <div className="space-y-5">
      {/* Tabs */}
      <div className="flex items-center gap-1 bg-white rounded-xl p-1 shadow-sm border border-gray-200 w-fit">
        <button onClick={() => { setTab('campaigns'); setShowOnlyProblems(false); setSearch('') }}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${tab === 'campaigns' ? 'bg-blue-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}>
          Campagnes actives
          {problems1.length > 0 && <span className="ml-2 px-1.5 py-0.5 rounded-full bg-red-500 text-white text-[10px]">{problems1.length}</span>}
        </button>
        <button onClick={() => { setTab('budgets'); setShowOnlyProblems(false); setSearch('') }}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${tab === 'budgets' ? 'bg-blue-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}>
          Budget clients BDD
          {(budgetStats.mismatch + budgetStats.noInvoice + budgetStats.noMatch) > 0 && (
            <span className="ml-2 px-1.5 py-0.5 rounded-full bg-red-500 text-white text-[10px]">{budgetStats.mismatch + budgetStats.noInvoice + budgetStats.noMatch}</span>
          )}
        </button>
      </div>

      {/* ============ TAB 1: Campagnes ============ */}
      {tab === 'campaigns' && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="bg-white rounded-xl p-5 shadow-sm border border-gray-200 text-center">
              <div className="text-3xl font-bold text-gray-900">{groups.length}</div>
              <div className="text-sm text-gray-500 mt-1">Clients avec campagnes actives</div>
            </div>
            <div className={`bg-white rounded-xl p-5 shadow-sm border cursor-pointer transition-colors ${showOnlyProblems ? 'border-red-400 ring-2 ring-red-200' : 'border-red-200 hover:bg-red-50'}`}
              onClick={() => setShowOnlyProblems(v => !v)}>
              <div className="text-3xl font-bold text-red-600">{problems1.length}</div>
              <div className="text-sm text-red-500 mt-1">Sans facture depuis 31j</div>
              <div className="mt-2 pt-2 border-t border-red-100 space-y-0.5">
                {problemsNoInvoice.length > 0 && <div className="text-[11px] text-red-600"><span className="font-semibold">{problemsNoInvoice.length}</span> matchés sans facture</div>}
                {problemsNoMatch.length > 0 && <div className="text-[11px] text-gray-500"><span className="font-semibold">{problemsNoMatch.length}</span> sans match Zoho</div>}
              </div>
            </div>
            <div className="bg-white rounded-xl p-5 shadow-sm border border-red-200 text-center">
              <div className="text-3xl font-bold text-red-700">{formatMoney(problemBudgetDay * 30)}</div>
              <div className="text-sm text-red-500 mt-1">Budget /mois non facturé</div>
              <div className="mt-2 pt-2 border-t border-red-100 text-xs text-red-400">{formatMoney(problemBudgetDay)}/jour</div>
            </div>
            <div className="bg-white rounded-xl p-5 shadow-sm border border-green-200 text-center">
              <div className="text-3xl font-bold text-green-600">{groups.length - problems1.length}</div>
              <div className="text-sm text-green-500 mt-1">OK (facture récente)</div>
            </div>
          </div>

          <Toolbar search={search} setSearch={setSearch} refreshing={refreshing} onRefresh={() => fetchData(true)} />

          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200 text-gray-500 text-xs uppercase tracking-wider">
                  <SortHeader field="status" label="" current={campSort} onSort={setCampSort} className="w-10" />
                  <SortHeader field="name" label="Client (campagne)" current={campSort} onSort={setCampSort} />
                  <th className="px-5 py-3 text-center">Source</th>
                  <SortHeader field="budget" label="Budget /jour" current={campSort} onSort={setCampSort} align="right" />
                  <th className="px-5 py-3 text-right">~Budget /mois</th>
                  <SortHeader field="zoho" label="Client Zoho" current={campSort} onSort={setCampSort} />
                  <th className="px-5 py-3 text-left">Facture (31j)</th>
                  <SortHeader field="invoice" label="Montant HT" current={campSort} onSort={setCampSort} align="right" />
                </tr>
              </thead>
              <tbody>
                {filtered.map(g => {
                  const expanded = expandedRows.has(g.normalized)
                  return (
                    <tr key={g.normalized} className={`border-b border-gray-100 last:border-0 transition-colors ${g.hasRecentInvoice ? 'hover:bg-gray-50' : g.zohoName ? 'bg-red-50/40' : 'bg-gray-50/40'}`}>
                      <td className="px-5 py-3.5">{g.hasRecentInvoice ? <CheckCircle className="w-5 h-5 text-green-500" /> : g.zohoName ? <XCircle className="w-5 h-5 text-red-400" /> : <span className="w-5 h-5 rounded-full border-2 border-gray-300 block" />}</td>
                      <td className="px-5 py-3.5">
                        <button onClick={() => setExpandedRows(prev => { const n = new Set(prev); n.has(g.normalized) ? n.delete(g.normalized) : n.add(g.normalized); return n })} className="flex items-center gap-1.5 text-left group">
                          {expanded ? <ChevronUp className="w-3.5 h-3.5 text-gray-400" /> : <ChevronDown className="w-3.5 h-3.5 text-gray-400" />}
                          <span className="font-medium text-gray-900 group-hover:text-blue-600 transition-colors">{g.name}</span>
                          {g.campaigns.length > 1 && <span className="text-[10px] text-gray-400 ml-1">({g.campaigns.length})</span>}
                        </button>
                        {expanded && <CampaignDetails campaigns={g.campaigns} />}
                      </td>
                      <td className="px-5 py-3.5 text-center"><SourceBadges campaigns={g.campaigns} metaCount={g.metaCount} googleCount={g.googleCount} /></td>
                      <td className="px-5 py-3.5 text-right font-semibold text-gray-900">{g.totalDailyBudget > 0 ? formatMoney(g.totalDailyBudget) : '-'}</td>
                      <td className="px-5 py-3.5 text-right text-gray-500">{g.totalDailyBudget > 0 ? formatMoney(g.totalDailyBudget * 30) : '-'}</td>
                      <td className="px-5 py-3.5">
                        <ZohoMatchCell zohoName={g.zohoName} matchMethod={g.matchMethod} normalized={g.normalized}
                          matchingRow={matchingRow} setMatchingRow={setMatchingRow} zohoCustomers={zohoData ? Object.keys(zohoData.customers).sort((a, b) => a.localeCompare(b, 'fr')) : []}
                          onSave={saveOverride} onDelete={deleteOverride} />
                      </td>
                      <td className="px-5 py-3.5">{g.recentInvoice ? <div><span className="text-gray-700">{formatDate(g.recentInvoice.date)}</span><InvoiceStatusBadge status={g.recentInvoice.status} /></div> : <span className="text-red-400 font-medium">Aucune</span>}</td>
                      <td className="px-5 py-3.5 text-right">{g.recentInvoice ? (() => {
                        const isFR = !g.recentInvoice.country || g.recentInvoice.country.toLowerCase() === 'france'
                        const ht = isFR ? Math.round((g.recentInvoice.total / 1.20) * 100) / 100 : g.recentInvoice.total
                        return <span className="font-semibold text-gray-900">{formatMoney(ht)}<span className="text-[10px] text-gray-400 ml-0.5">HT</span></span>
                      })() : <span className="text-red-300">-</span>}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            {filtered.length === 0 && <div className="text-center py-16">{showOnlyProblems ? <div><CheckCircle className="w-10 h-10 text-green-400 mx-auto mb-2" /><p className="text-green-600 font-medium">Tous les clients actifs ont une facture recente</p></div> : <p className="text-gray-400">Aucun resultat</p>}</div>}
          </div>
        </>
      )}

      {/* ============ TAB 2: Budget BDD ============ */}
      {tab === 'budgets' && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            <div className="bg-white rounded-xl p-4 shadow-sm border border-gray-200 text-center">
              <div className="text-2xl font-bold text-gray-900">{budgetStats.total}</div>
              <div className="text-xs text-gray-500 mt-1">Clients actifs (&gt;30j)</div>
            </div>
            <div className="bg-white rounded-xl p-4 shadow-sm border border-green-200 text-center">
              <div className="text-2xl font-bold text-green-600">{budgetStats.ok}</div>
              <div className="text-xs text-green-500 mt-1">Montant OK</div>
            </div>
            <div className={`bg-white rounded-xl p-4 shadow-sm border cursor-pointer transition-colors ${showOnlyProblems ? 'border-orange-400 ring-2 ring-orange-200' : 'border-orange-200 hover:bg-orange-50'}`}
              onClick={() => setShowOnlyProblems(v => !v)}>
              <div className="text-2xl font-bold text-orange-600">{budgetStats.mismatch}</div>
              <div className="text-xs text-orange-500 mt-1">Montant different</div>
            </div>
            <div className="bg-white rounded-xl p-4 shadow-sm border border-red-200 text-center">
              <div className="text-2xl font-bold text-red-600">{budgetStats.noInvoice}</div>
              <div className="text-xs text-red-500 mt-1">Pas de facture</div>
            </div>
            <div className="bg-white rounded-xl p-4 shadow-sm border border-gray-200 text-center">
              <div className="text-2xl font-bold text-gray-400">{budgetStats.noMatch}</div>
              <div className="text-xs text-gray-400 mt-1">Pas de match Zoho</div>
              {budgetStats.missingBudget > 0 && <div className="mt-1 pt-1 border-t border-gray-100 text-[10px] text-red-500 font-semibold">{formatMoney(budgetStats.missingBudget)}/mois manquants</div>}
            </div>
          </div>

          <Toolbar search={search} setSearch={setSearch} refreshing={refreshing} onRefresh={() => fetchData(true)} />

          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200 text-gray-500 text-xs uppercase tracking-wider">
                  <SortHeader field="status" label="" current={budgetSort} onSort={setBudgetSort} className="w-10" />
                  <SortHeader field="name" label="Client BDD" current={budgetSort} onSort={setBudgetSort} align="left" />
                  <SortHeader field="budget" label="Budget BDD / mois" current={budgetSort} onSort={setBudgetSort} align="right" />
                  <SortHeader field="zoho" label="Client Zoho" current={budgetSort} onSort={setBudgetSort} align="left" />
                  <th className="px-5 py-3 text-left">Facture (31j)</th>
                  <SortHeader field="invoice" label="Montant facture HT" current={budgetSort} onSort={setBudgetSort} align="right" />
                  <SortHeader field="diff" label="Ecart" current={budgetSort} onSort={setBudgetSort} align="right" />
                  <SortHeader field="pctDiff" label="% Ecart" current={budgetSort} onSort={setBudgetSort} align="right" />
                </tr>
              </thead>
              <tbody>
                {filteredBudget.map(r => (
                  <tr key={r.client.normalized} className={`border-b border-gray-100 last:border-0 transition-colors ${
                    r.budgetMatch === 'ok' ? 'hover:bg-gray-50' :
                    r.budgetMatch === 'mismatch' ? 'bg-orange-50/40' :
                    'bg-red-50/40'
                  }`}>
                    <td className="px-5 py-3.5">
                      {r.budgetMatch === 'ok' && <CheckCircle className="w-5 h-5 text-green-500" />}
                      {r.budgetMatch === 'mismatch' && <AlertTriangle className="w-5 h-5 text-orange-500" />}
                      {r.budgetMatch === 'no_invoice' && <XCircle className="w-5 h-5 text-red-400" />}
                      {r.budgetMatch === 'no_match' && <span className="w-5 h-5 rounded-full border-2 border-gray-300 block" />}
                    </td>
                    <td className="px-5 py-3.5">
                      <div className="font-medium text-gray-900">{r.client.nom}</div>
                      {r.client.start_date && <div className="text-[10px] text-gray-400">Depuis {formatDate(r.client.start_date)}</div>}
                    </td>
                    <td className="px-5 py-3.5 text-right font-semibold text-gray-900">{formatMoney(r.client.budget_mensuel)}</td>
                    <td className="px-5 py-3.5">
                      <ZohoMatchCell zohoName={r.zohoName} matchMethod={r.matchMethod} normalized={r.client.normalized}
                        matchingRow={matchingRow} setMatchingRow={setMatchingRow} zohoCustomers={zohoData ? Object.keys(zohoData.customers).sort((a, b) => a.localeCompare(b, 'fr')) : []}
                        onSave={saveOverride} onDelete={deleteOverride} />
                    </td>
                    <td className="px-5 py-3.5">{r.recentInvoice ? <div><span className="text-gray-700">{formatDate(r.recentInvoice.date)}</span><InvoiceStatusBadge status={r.recentInvoice.status} /></div> : <span className="text-red-400 font-medium">{r.zohoName ? 'Aucune' : '-'}</span>}</td>
                    <td className="px-5 py-3.5 text-right">{r.recentInvoice ? <span className="font-semibold text-gray-900">{formatMoney(r.invoiceAmount)}</span> : <span className="text-red-300">-</span>}</td>
                    <td className="px-5 py-3.5 text-right">
                      {r.budgetMatch === 'ok' && <span className="text-green-600 font-medium">-</span>}
                      {r.budgetMatch === 'mismatch' && <span className={`font-semibold ${r.diff > 0 ? 'text-green-600' : 'text-red-600'}`}>{r.diff > 0 ? '+' : ''}{formatMoney(r.diff)}</span>}
                      {(r.budgetMatch === 'no_invoice' || r.budgetMatch === 'no_match') && <span className="text-red-300">-</span>}
                    </td>
                    <td className="px-5 py-3.5 text-right">
                      {r.budgetMatch === 'ok' && <span className="text-green-600 font-medium">-</span>}
                      {r.budgetMatch === 'mismatch' && <span className={`font-semibold ${r.pctDiff > 0 ? 'text-green-600' : 'text-red-600'}`}>{r.pctDiff > 0 ? '+' : ''}{r.pctDiff}%</span>}
                      {(r.budgetMatch === 'no_invoice' || r.budgetMatch === 'no_match') && <span className="text-red-300">-</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {filteredBudget.length === 0 && <div className="text-center py-16">{showOnlyProblems ? <div><CheckCircle className="w-10 h-10 text-green-400 mx-auto mb-2" /><p className="text-green-600 font-medium">Tous les budgets correspondent</p></div> : <p className="text-gray-400">Aucun resultat</p>}</div>}
          </div>
        </>
      )}
    </div>
  )
}

// --- Sub-components ---

function SortHeader<T extends string>({ field, label, current, onSort, align = 'left', className = '' }: {
  field: T; label: string
  current: { field: T; dir: 'asc' | 'desc' }
  onSort: (v: { field: T; dir: 'asc' | 'desc' }) => void
  align?: 'left' | 'right'; className?: string
}) {
  const active = current.field === field
  return (
    <th className={`px-5 py-3 text-${align} cursor-pointer hover:text-gray-900 select-none ${className}`}
      onClick={() => onSort({ field, dir: active && current.dir === 'asc' ? 'desc' : 'asc' })}>
      <div className={`flex items-center gap-1 ${align === 'right' ? 'justify-end' : ''}`}>
        {label}
        {active && (current.dir === 'asc' ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />)}
      </div>
    </th>
  )
}

function Toolbar({ search, setSearch, refreshing, onRefresh }: { search: string; setSearch: (s: string) => void; refreshing: boolean; onRefresh: () => void }) {
  return (
    <div className="flex items-center gap-3">
      <div className="relative flex-1">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
        <input type="text" value={search} onChange={e => setSearch(e.target.value)} placeholder="Rechercher un client..."
          className="w-full pl-10 pr-4 py-2.5 bg-white border border-gray-300 rounded-xl text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent" />
      </div>
      <button onClick={onRefresh} disabled={refreshing} className="flex items-center gap-2 px-4 py-2.5 bg-blue-600 text-white rounded-xl text-sm hover:bg-blue-700 transition-colors disabled:opacity-50">
        <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />{refreshing ? 'Actualisation...' : 'Actualiser'}
      </button>
    </div>
  )
}

function InvoiceStatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = { paid: 'bg-green-100 text-green-600', sent: 'bg-blue-100 text-blue-600', overdue: 'bg-red-100 text-red-600' }
  return <span className={`ml-2 text-[10px] px-1.5 py-0.5 rounded font-medium ${colors[status] || 'bg-gray-100 text-gray-500'}`}>{status}</span>
}

function CampaignDetails({ campaigns }: { campaigns: Campaign[] }) {
  return (
    <div className="mt-2 ml-5 space-y-1">
      {campaigns.map((c, i) => (
        <div key={i} className="flex items-center gap-2 text-xs text-gray-500">
          <span className={`px-1.5 py-0.5 rounded font-semibold text-[10px] ${c.source === 'meta' ? 'bg-blue-100 text-blue-700' : 'bg-amber-100 text-amber-700'}`}>{c.source === 'meta' ? 'Meta' : 'Google'}</span>
          {c.account_name && <span className="text-gray-400 text-[10px]">{c.account_name}</span>}
          {c.url ? <a href={c.url} target="_blank" rel="noopener noreferrer" className="truncate text-blue-600 hover:underline flex items-center gap-1">{c.name}<ExternalLink className="w-3 h-3 flex-shrink-0" /></a> : <span className="truncate">{c.name}</span>}
          {c.daily_budget > 0 && <span className="ml-auto text-gray-400">{formatMoney(c.daily_budget)}/j</span>}
        </div>
      ))}
    </div>
  )
}

function SourceBadges({ campaigns, metaCount, googleCount }: { campaigns: Campaign[]; metaCount: number; googleCount: number }) {
  return (
    <div className="flex items-center justify-center gap-1.5">
      {metaCount > 0 && (() => {
        const c = campaigns.find(c => c.source === 'meta' && c.url)
        const badge = <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 text-[11px] font-semibold">Meta{metaCount > 1 ? ` x${metaCount}` : ''}{c && <ExternalLink className="w-2.5 h-2.5" />}</span>
        return c ? <a href={c.url} target="_blank" rel="noopener noreferrer" className="hover:opacity-80 transition-opacity">{badge}</a> : badge
      })()}
      {googleCount > 0 && (() => {
        const c = campaigns.find(c => c.source === 'google' && c.url)
        const badge = <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-amber-100 text-amber-700 text-[11px] font-semibold">Google{googleCount > 1 ? ` x${googleCount}` : ''}{c && <ExternalLink className="w-2.5 h-2.5" />}</span>
        return c ? <a href={c.url} target="_blank" rel="noopener noreferrer" className="hover:opacity-80 transition-opacity">{badge}</a> : badge
      })()}
    </div>
  )
}

function ZohoMatchCell({ zohoName, matchMethod, normalized, matchingRow, setMatchingRow, zohoCustomers, onSave, onDelete }: {
  zohoName: string | null; matchMethod: string | null; normalized: string; matchingRow: string | null
  setMatchingRow: (v: string | null) => void; zohoCustomers: string[]
  onSave: (key: string, name: string) => void; onDelete: (key: string) => void
}) {
  // Mode édition
  if (matchingRow === normalized) {
    return <MatchSelector zohoCustomers={zohoCustomers} onSelect={name => onSave(normalized, name)} onCancel={() => setMatchingRow(null)} />
  }
  if (zohoName) {
    return (
      <div className="flex items-center gap-1.5 group/match">
        <span className="text-gray-700">{zohoName}</span>
        <span className={`text-[9px] px-1 py-0.5 rounded font-medium ${matchMethod === 'gcl' ? 'bg-purple-100 text-purple-600' : matchMethod === 'manual' ? 'bg-indigo-100 text-indigo-600' : 'bg-gray-100 text-gray-400'}`}>
          {matchMethod === 'gcl' ? 'GCL' : matchMethod === 'manual' ? 'manuel' : 'nom'}
        </span>
        <button onClick={e => { e.stopPropagation(); setMatchingRow(normalized) }}
          className="text-gray-300 hover:text-blue-500 transition-colors opacity-0 group-hover/match:opacity-100" title="Changer le matching">
          <Pencil className="w-3 h-3" />
        </button>
        {matchMethod === 'manual' && (
          <button onClick={e => { e.stopPropagation(); onDelete(normalized) }}
            className="text-gray-300 hover:text-red-500 transition-colors opacity-0 group-hover/match:opacity-100" title="Supprimer l'override">
            <Trash2 className="w-3 h-3" />
          </button>
        )}
      </div>
    )
  }
  return <button onClick={e => { e.stopPropagation(); setMatchingRow(normalized) }} className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium transition-colors"><Link2 className="w-3 h-3" /> Matcher</button>
}

function MatchSelector({ zohoCustomers, onSelect, onCancel }: { zohoCustomers: string[]; onSelect: (name: string) => void; onCancel: () => void }) {
  const [q, setQ] = useState('')
  const filtered = useMemo(() => {
    if (!q) return zohoCustomers.slice(0, 8)
    const s = q.toLowerCase()
    return zohoCustomers.filter(n => n.toLowerCase().includes(s)).slice(0, 8)
  }, [zohoCustomers, q])
  return (
    <div className="relative" onClick={e => e.stopPropagation()}>
      <input type="text" value={q} onChange={e => setQ(e.target.value)} placeholder="Chercher client Zoho..." autoFocus
        className="w-full px-2 py-1 border border-blue-300 rounded text-xs focus:ring-1 focus:ring-blue-500 focus:outline-none"
        onKeyDown={e => { if (e.key === 'Escape') onCancel() }} />
      <div className="absolute top-full left-0 right-0 mt-0.5 bg-white border border-gray-200 rounded shadow-lg z-50 max-h-48 overflow-y-auto">
        {filtered.map(name => <button key={name} onClick={() => onSelect(name)} className="w-full text-left px-2 py-1.5 text-xs hover:bg-blue-50 text-gray-700 truncate transition-colors">{name}</button>)}
        {filtered.length === 0 && <div className="px-2 py-1.5 text-xs text-gray-400">Aucun resultat</div>}
      </div>
      <button onClick={onCancel} className="absolute -top-1 -right-1 w-4 h-4 bg-gray-200 rounded-full flex items-center justify-center text-gray-500 hover:bg-red-200 hover:text-red-600 text-[10px]">×</button>
    </div>
  )
}

export default ControleFacturationPage
