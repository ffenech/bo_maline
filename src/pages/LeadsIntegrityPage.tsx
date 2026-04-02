import React, { useState } from 'react'
import { AlertTriangle, RefreshCw, Check, X, Loader2, ChevronDown, ChevronUp, Filter } from 'lucide-react'

// ======== Types ========

interface EstimateurLevel {
  etape3: number
  etape4: number
  etape3_vendeurs: number
  etape3_non_vendeurs: number
  etape4_vendeurs: number
  etape4_non_vendeurs: number
}

interface MissingInV3 {
  id: number
  uuid_id: string
  adresse: string
  pourcentage_vente: number
  date_acquisition: string
  is_vendeur: boolean
  hors_zone: boolean
  cp_bien: string
  cp_agence: string
}

interface DifferentDateInV3 {
  id: number
  uuid_id: string
  adresse: string
  pourcentage_vente: number
  date_acquisition: string
  is_vendeur: boolean
  v3_created_date: string
}

interface MissingInV2 {
  id_property: string
  address: string
  sale_project: string
  created_date: string
  origin: string
  demo: boolean
}

interface LogNotInV2 {
  uniqueId: string
  etape: number
  vente: number
  is_vendeur: boolean
  adresse: string
  idAgence: string
}

interface FunnelData {
  date: string
  estimateur: { fr: EstimateurLevel; es: EstimateurLevel }
  logs: { etape3: number; etape4: number; etape3_vendeurs: number; etape3_non_vendeurs: number; etape4_vendeurs: number; etape4_non_vendeurs: number }
  v2: { total: number; vendeurs: number; non_vendeurs: number; hors_zone_importes: number }
  v3: { total: number }
  missing_in_v3: MissingInV3[]
  missing_in_v3_stats: { total: number; vendeurs: number; non_vendeurs: number; hors_zone: number; en_zone: number }
  different_date_in_v3: DifferentDateInV3[]
  missing_in_v2: MissingInV2[]
  logs_not_in_v2: LogNotInV2[]
  logs_not_in_v2_stats: { total: number; vendeurs: number; non_vendeurs: number; etape3: number; etape4: number }
}

interface RetryResult {
  id: number
  success: boolean
  output: string
}

// ======== Composants utilitaires ========

function Badge({ children, color }: { children: React.ReactNode; color: 'red' | 'green' | 'yellow' | 'orange' | 'gray' | 'blue' | 'purple' }) {
  const colors = {
    red: 'bg-red-100 text-red-800',
    green: 'bg-green-100 text-green-800',
    yellow: 'bg-yellow-100 text-yellow-800',
    orange: 'bg-orange-100 text-orange-800',
    gray: 'bg-gray-100 text-gray-600',
    blue: 'bg-blue-100 text-blue-800',
    purple: 'bg-purple-100 text-purple-800'
  }
  return <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${colors[color]}`}>{children}</span>
}


// ======== Composant principal ========

function LeadsIntegrityPage() {
  const [date, setDate] = useState(() => {
    const d = new Date()
    d.setDate(d.getDate() - 1)
    return d.toISOString().split('T')[0]
  })

  const formatDateFr = (d: string) => {
    const parts = d.split('-')
    if (parts.length === 3) return `${parts[2]}/${parts[1]}/${parts[0]}`
    return d
  }

  const [data, setData] = useState<FunnelData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [retrying, setRetrying] = useState<Set<number>>(new Set())
  const [retryResults, setRetryResults] = useState<Map<number, RetryResult>>(new Map())
  const [retryingAll, setRetryingAll] = useState(false)
  // Import logs
  const [importingLog, setImportingLog] = useState<Set<string>>(new Set())
  const [importLogResults, setImportLogResults] = useState<Map<string, { success: boolean; output: string }>>(new Map())
  const [importingAll, setImportingAll] = useState(false)
  const [importAllResult, setImportAllResult] = useState<{ success: boolean; total: number; found: number; missed: number; importOutput: string } | null>(null)

  // Collapsible sections
  const [showLogsNotInV2, setShowLogsNotInV2] = useState(true)
  const [showMissingV3, setShowMissingV3] = useState(true)
  const [showDiffDate, setShowDiffDate] = useState(false)
  const [showMissingV2, setShowMissingV2] = useState(false)
  const [filterLogsVendeur, setFilterLogsVendeur] = useState<'all' | 'vendeur' | 'non_vendeur'>('all')
  const [filterLogsEtape, setFilterLogsEtape] = useState<'all' | '3' | '4'>('all')
  // Filters
  const [filterVendeur, setFilterVendeur] = useState<'all' | 'vendeur' | 'non_vendeur'>('all')
  const [filterHorsZone, setFilterHorsZone] = useState<'all' | 'en_zone' | 'hors_zone'>('all')

  const fetchIntegrity = async () => {
    setLoading(true)
    setError(null)
    setRetryResults(new Map())
    try {
      const res = await fetch(`/api/leads/integrity-funnel?date=${date}`)
      if (!res.ok) throw new Error(`Erreur ${res.status}`)
      const json = await res.json()
      setData(json)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  const retryOne = async (id: number) => {
    setRetrying(prev => new Set(prev).add(id))
    try {
      const res = await fetch('/api/leads/integrity-retry', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id })
      })
      const json = await res.json()
      setRetryResults(prev => new Map(prev).set(id, { id, success: json.success, output: json.output || json.error }))
    } catch (e: any) {
      setRetryResults(prev => new Map(prev).set(id, { id, success: false, output: e.message }))
    } finally {
      setRetrying(prev => { const next = new Set(prev); next.delete(id); return next })
    }
  }

  const retryBatch = async (ids: number[]) => {
    if (ids.length === 0) return
    setRetryingAll(true)
    try {
      const res = await fetch('/api/leads/integrity-retry-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids })
      })
      const json = await res.json()
      if (json.results) {
        const newResults = new Map(retryResults)
        json.results.forEach((r: RetryResult) => newResults.set(r.id, r))
        setRetryResults(newResults)
      }
    } catch (e: any) {
      setError(e.message)
    } finally {
      setRetryingAll(false)
    }
  }

  const importLog = async (uniqueId: string) => {
    if (!data) return
    setImportingLog(prev => new Set(prev).add(uniqueId))
    try {
      const res = await fetch('/api/leads/integrity-import-log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ uniqueId, date: data.date })
      })
      const json = await res.json()
      setImportLogResults(prev => new Map(prev).set(uniqueId, { success: json.success, output: json.output || json.error || json.details }))
    } catch (e: any) {
      setImportLogResults(prev => new Map(prev).set(uniqueId, { success: false, output: e.message }))
    } finally {
      setImportingLog(prev => { const next = new Set(prev); next.delete(uniqueId); return next })
    }
  }

  const importAllLogs = async () => {
    if (!data || !data.logs_not_in_v2 || data.logs_not_in_v2.length === 0) return
    const filtered = data.logs_not_in_v2
      .filter(r => {
        if (filterLogsVendeur === 'vendeur' && !r.is_vendeur) return false
        if (filterLogsVendeur === 'non_vendeur' && r.is_vendeur) return false
        if (filterLogsEtape === '3' && r.etape !== 3) return false
        if (filterLogsEtape === '4' && r.etape !== 4) return false
        return true
      })
      .filter(r => !importLogResults.has(r.uniqueId))
    if (filtered.length === 0) return
    if (!confirm(`Importer ${filtered.length} leads non importés ? Cette action va copier les lignes JSON dans recette_immo et lancer import_from_ri.php.`)) return

    setImportingAll(true)
    setImportAllResult(null)
    try {
      const res = await fetch('/api/leads/integrity-import-log-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ uniqueIds: filtered.map(r => r.uniqueId), date: data.date })
      })
      const json = await res.json()
      setImportAllResult(json)
      // Marquer chaque lead comme importé
      if (json.results) {
        const newResults = new Map(importLogResults)
        json.results.forEach((r: any) => {
          newResults.set(r.uniqueId, { success: r.found, output: r.found ? 'Importé via batch' : 'Non trouvé dans les logs' })
        })
        setImportLogResults(newResults)
      }
    } catch (e: any) {
      setImportAllResult({ success: false, total: filtered.length, found: 0, missed: 0, importOutput: e.message })
    } finally {
      setImportingAll(false)
    }
  }

  // Filtered missing in V3
  const filteredMissingV3 = data?.missing_in_v3.filter(r => {
    if (filterVendeur === 'vendeur' && !r.is_vendeur) return false
    if (filterVendeur === 'non_vendeur' && r.is_vendeur) return false
    if (filterHorsZone === 'en_zone' && r.hors_zone) return false
    if (filterHorsZone === 'hors_zone' && !r.hors_zone) return false
    return true
  }) ?? []

  // Calcul funnel
  const estimTotal3 = data ? data.estimateur.fr.etape3 + data.estimateur.es.etape3 : 0
  const estimTotal4 = data ? data.estimateur.fr.etape4 + data.estimateur.es.etape4 : 0
  // Logs : etape3 = exclusifs (jamais passés en 4), etape4 = complétés. Total = somme des deux
  const logsTotal = data ? data.logs.etape3 + data.logs.etape4 : 0
  const logsTotalVendeurs = data ? data.logs.etape3_vendeurs + data.logs.etape4_vendeurs : 0
  const logsTotalNonVendeurs = data ? data.logs.etape3_non_vendeurs + data.logs.etape4_non_vendeurs : 0

  return (
    <div className="space-y-6">
      {/* Header + Date picker */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Audit d'intégrité du pipeline</h2>
            <p className="text-sm text-gray-500 mt-1">Estimateur → Logs get_price → MySQL V2 → PostgreSQL V3</p>
          </div>
          <div className="flex items-center gap-3">
            <input
              type="date"
              value={date}
              onChange={e => setDate(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
            <span className="text-sm text-gray-500 font-medium">{formatDateFr(date)}</span>
            <button
              onClick={fetchIntegrity}
              disabled={loading}
              className="bg-blue-600 text-white px-5 py-2 rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Analyser
            </button>
          </div>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">{error}</div>
      )}

      {data && (
        <>
          {/* ======== FUNNEL EN 5 BLOCS ======== */}
          <div className="space-y-1">
            <h3 className="text-base font-semibold text-gray-800 mb-3">Pipeline du {formatDateFr(data.date)}</h3>

            {/* Bloc 1 & 2 : Estimateurs FR et ES côte à côte */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Estimateur FR */}
              <div className="bg-white rounded-lg shadow border-l-4 border-indigo-500 p-5">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="text-sm font-bold text-indigo-700 uppercase tracking-wide">Estimateur New FR</h4>
                  <span className="text-2xl font-bold text-indigo-600">{data.estimateur.fr.etape3 + data.estimateur.fr.etape4}</span>
                </div>
                <div className="grid grid-cols-2 gap-3 text-sm">
                  <div className="bg-indigo-50 rounded-lg p-3">
                    <div className="text-xs text-indigo-500 font-medium">Etape 3 (coordonnées)</div>
                    <div className="text-xl font-bold text-indigo-700">{data.estimateur.fr.etape3 + data.estimateur.fr.etape4}</div>
                    <div className="text-xs text-gray-500 mt-1">V: {data.estimateur.fr.etape3_vendeurs + data.estimateur.fr.etape4_vendeurs} | NV: {data.estimateur.fr.etape3_non_vendeurs + data.estimateur.fr.etape4_non_vendeurs}</div>
                  </div>
                  <div className="bg-indigo-50 rounded-lg p-3">
                    <div className="text-xs text-indigo-500 font-medium">Etape 4 (complétées)</div>
                    <div className="text-xl font-bold text-indigo-700">{data.estimateur.fr.etape4}</div>
                    <div className="text-xs text-gray-500 mt-1">V: {data.estimateur.fr.etape4_vendeurs} | NV: {data.estimateur.fr.etape4_non_vendeurs}</div>
                    {(data.estimateur.fr.etape3 + data.estimateur.fr.etape4) > 0 && (
                      <div className="text-xs font-medium mt-1" style={{ color: data.estimateur.fr.etape4 / (data.estimateur.fr.etape3 + data.estimateur.fr.etape4) >= 0.7 ? '#16a34a' : '#dc2626' }}>
                        {((data.estimateur.fr.etape4 / (data.estimateur.fr.etape3 + data.estimateur.fr.etape4)) * 100).toFixed(1)}% conversion
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Estimateur ES */}
              <div className="bg-white rounded-lg shadow border-l-4 border-purple-500 p-5">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="text-sm font-bold text-purple-700 uppercase tracking-wide">Estimateur New ES</h4>
                  <span className="text-2xl font-bold text-purple-600">{data.estimateur.es.etape3 + data.estimateur.es.etape4}</span>
                </div>
                <div className="grid grid-cols-2 gap-3 text-sm">
                  <div className="bg-purple-50 rounded-lg p-3">
                    <div className="text-xs text-purple-500 font-medium">Etape 3 (coordonnées)</div>
                    <div className="text-xl font-bold text-purple-700">{data.estimateur.es.etape3 + data.estimateur.es.etape4}</div>
                    <div className="text-xs text-gray-500 mt-1">V: {data.estimateur.es.etape3_vendeurs + data.estimateur.es.etape4_vendeurs} | NV: {data.estimateur.es.etape3_non_vendeurs + data.estimateur.es.etape4_non_vendeurs}</div>
                  </div>
                  <div className="bg-purple-50 rounded-lg p-3">
                    <div className="text-xs text-purple-500 font-medium">Etape 4 (complétées)</div>
                    <div className="text-xl font-bold text-purple-700">{data.estimateur.es.etape4}</div>
                    <div className="text-xs text-gray-500 mt-1">V: {data.estimateur.es.etape4_vendeurs} | NV: {data.estimateur.es.etape4_non_vendeurs}</div>
                    {(data.estimateur.es.etape3 + data.estimateur.es.etape4) > 0 && (
                      <div className="text-xs font-medium mt-1" style={{ color: data.estimateur.es.etape4 / (data.estimateur.es.etape3 + data.estimateur.es.etape4) >= 0.7 ? '#16a34a' : '#dc2626' }}>
                        {((data.estimateur.es.etape4 / (data.estimateur.es.etape3 + data.estimateur.es.etape4)) * 100).toFixed(1)}% conversion
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Flèche */}
            <div className="flex justify-center py-1">
              <div className="flex flex-col items-center text-gray-400">
                <div className="w-0.5 h-4 bg-gray-300" />
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" /></svg>
              </div>
            </div>

            {/* Bloc 3 : Get_Price (Logs) */}
            <div className="bg-white rounded-lg shadow border-l-4 border-blue-500 p-5">
              <div className="flex items-center justify-between mb-3">
                <h4 className="text-sm font-bold text-blue-700 uppercase tracking-wide">Get_Price (Logs serveur)</h4>
                <span className="text-2xl font-bold text-blue-600">{logsTotal} <span className="text-sm font-normal text-gray-400">leads uniques étape 3+</span></span>
              </div>
              <div className="grid grid-cols-3 gap-3 text-sm">
                <div className="bg-blue-50 rounded-lg p-3">
                  <div className="text-xs text-blue-500 font-medium">Total étape 3+</div>
                  <div className="text-xl font-bold text-blue-700">{logsTotal}</div>
                  <div className="text-xs text-gray-500 mt-1">V: {logsTotalVendeurs} | NV: {logsTotalNonVendeurs}</div>
                </div>
                <div className="bg-blue-50 rounded-lg p-3">
                  <div className="text-xs text-blue-500 font-medium">Complétées (étape 4)</div>
                  <div className="text-xl font-bold text-blue-700">{data.logs.etape4}</div>
                  <div className="text-xs text-gray-500 mt-1">V: {data.logs.etape4_vendeurs} | NV: {data.logs.etape4_non_vendeurs}</div>
                  {logsTotal > 0 && (
                    <div className="text-xs font-medium mt-1 text-green-600">
                      {((data.logs.etape4 / logsTotal) * 100).toFixed(1)}% conversion
                    </div>
                  )}
                </div>
                <div className="bg-gray-50 rounded-lg p-3">
                  <div className="text-xs text-gray-500 font-medium">Abandons (restés ét. 3)</div>
                  <div className="text-xl font-bold text-gray-500">{data.logs.etape3}</div>
                  <div className="text-xs text-gray-400 mt-1">V: {data.logs.etape3_vendeurs} | NV: {data.logs.etape3_non_vendeurs}</div>
                </div>
              </div>
              {/* Écart estimateur vs logs */}
              {estimTotal4 > 0 && logsTotal !== (estimTotal3 + estimTotal4) && (
                <div className="mt-3 text-xs text-gray-500 bg-gray-50 rounded p-2">
                  Ecart Estimateur vs Logs : {logsTotal} logs vs {estimTotal3 + estimTotal4} estimateur ({logsTotal - (estimTotal3 + estimTotal4) > 0 ? '+' : ''}{logsTotal - (estimTotal3 + estimTotal4)})
                </div>
              )}
            </div>

            {/* Flèche */}
            <div className="flex justify-center py-1">
              <div className="flex flex-col items-center text-gray-400">
                <div className="w-0.5 h-4 bg-gray-300" />
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" /></svg>
              </div>
            </div>

            {/* Bloc 4 : Maline V2 */}
            <div className="bg-white rounded-lg shadow border-l-4 border-amber-500 p-5">
              <div className="flex items-center justify-between mb-3">
                <h4 className="text-sm font-bold text-amber-700 uppercase tracking-wide">Maline V2 (MySQL)</h4>
                <div className="text-right">
                  <span className="text-2xl font-bold text-amber-600">{data.v2.total}</span>
                  {data.logs.etape4 > 0 && (
                    <span className={`ml-2 text-sm font-medium ${data.v2.total >= data.logs.etape4 ? 'text-green-600' : 'text-red-600'}`}>
                      {((data.v2.total / data.logs.etape4) * 100).toFixed(1)}% des complétés
                    </span>
                  )}
                </div>
              </div>
              <div className="grid grid-cols-3 gap-3 text-sm">
                <div className="bg-amber-50 rounded-lg p-3">
                  <div className="text-xs text-amber-600 font-medium">Vendeurs</div>
                  <div className="text-xl font-bold text-amber-700">{data.v2.vendeurs}</div>
                  <div className="text-xs text-gray-400 mt-1">pv &ge; 35</div>
                </div>
                <div className="bg-amber-50 rounded-lg p-3">
                  <div className="text-xs text-amber-600 font-medium">Non-vendeurs</div>
                  <div className="text-xl font-bold text-amber-700">{data.v2.non_vendeurs}</div>
                  <div className="text-xs text-gray-400 mt-1">pv &lt; 35</div>
                </div>
                {data.v2.hors_zone_importes > 0 && (
                  <div className="bg-orange-50 rounded-lg p-3">
                    <div className="text-xs text-orange-600 font-medium">Hors zone (importés)</div>
                    <div className="text-xl font-bold text-orange-600">{data.v2.hors_zone_importes}</div>
                  </div>
                )}
              </div>
              {data.v2.total > data.logs.etape4 && data.logs.etape4 > 0 && (
                <div className="mt-3 text-xs text-amber-700 bg-amber-50 rounded p-2">
                  {data.v2.total - data.logs.etape4} leads V2 de plus que les logs complétés (mises à jour d'anciens biens ou imports manuels)
                </div>
              )}
            </div>

            {/* Flèche */}
            <div className="flex justify-center py-1">
              <div className="flex flex-col items-center text-gray-400">
                <div className="w-0.5 h-4 bg-gray-300" />
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" /></svg>
              </div>
            </div>

            {/* Bloc 5 : Maline V3 */}
            <div className="bg-white rounded-lg shadow border-l-4 border-green-500 p-5">
              <div className="flex items-center justify-between mb-3">
                <h4 className="text-sm font-bold text-green-700 uppercase tracking-wide">Maline V3 (PostgreSQL)</h4>
                <div className="text-right">
                  <span className="text-2xl font-bold text-green-600">{data.v3.total}</span>
                  {data.v2.total > 0 && (
                    <span className={`ml-2 text-sm font-medium ${data.v3.total >= data.v2.total ? 'text-green-600' : 'text-red-600'}`}>
                      {((data.v3.total / data.v2.total) * 100).toFixed(1)}% de V2
                    </span>
                  )}
                </div>
              </div>
              {data.missing_in_v3_stats.total > 0 ? (
                <div className="grid grid-cols-3 gap-3 text-sm">
                  <div className="bg-red-50 rounded-lg p-3">
                    <div className="text-xs text-red-600 font-medium">Manquants (absents V3)</div>
                    <div className="text-xl font-bold text-red-600">{data.missing_in_v3_stats.total}</div>
                    <div className="text-xs text-gray-500 mt-1">{data.missing_in_v3_stats.vendeurs} V | {data.missing_in_v3_stats.non_vendeurs} NV</div>
                  </div>
                  <div className="bg-yellow-50 rounded-lg p-3">
                    <div className="text-xs text-yellow-600 font-medium">Date différente</div>
                    <div className="text-xl font-bold text-yellow-600">{data.different_date_in_v3.length}</div>
                    <div className="text-xs text-gray-500 mt-1">Présents V3 autre date</div>
                  </div>
                  <div className="bg-blue-50 rounded-lg p-3">
                    <div className="text-xs text-blue-600 font-medium">En V3 mais pas V2</div>
                    <div className="text-xl font-bold text-blue-600">{data.missing_in_v2.length}</div>
                    <div className="text-xs text-gray-500 mt-1">Créations directes V3</div>
                  </div>
                </div>
              ) : (
                <div className="bg-green-50 rounded-lg p-3 text-center">
                  <Check className="w-5 h-5 text-green-600 mx-auto mb-1" />
                  <div className="text-sm font-medium text-green-700">Aucune perte de données V2 → V3</div>
                  {data.different_date_in_v3.length > 0 && (
                    <div className="text-xs text-gray-500 mt-1">{data.different_date_in_v3.length} leads V2 présents en V3 à une autre date</div>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* ======== LOGS NON IMPORTÉS EN V2 ======== */}
          {data.logs_not_in_v2 && data.logs_not_in_v2.length > 0 && (
            <div className="bg-white rounded-lg shadow">
              <button
                onClick={() => setShowLogsNotInV2(!showLogsNotInV2)}
                className="w-full p-4 flex items-center justify-between hover:bg-gray-50"
              >
                <div className="flex items-center gap-3">
                  <AlertTriangle className="w-5 h-5 text-orange-500" />
                  <span className="font-semibold text-gray-800">
                    Leads non importés en V2 ({data.logs_not_in_v2_stats.total})
                  </span>
                  <Badge color="orange">{data.logs_not_in_v2_stats.vendeurs} vendeurs</Badge>
                  <Badge color="gray">{data.logs_not_in_v2_stats.non_vendeurs} non-vendeurs</Badge>
                  <Badge color="blue">ét.3: {data.logs_not_in_v2_stats.etape3}</Badge>
                  <Badge color="purple">ét.4: {data.logs_not_in_v2_stats.etape4}</Badge>
                </div>
                {showLogsNotInV2 ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
              </button>
              {showLogsNotInV2 && (
                <div className="px-4 pb-4">
                  <div className="flex gap-2 mb-3">
                    <select value={filterLogsVendeur} onChange={e => setFilterLogsVendeur(e.target.value as any)} className="text-xs border rounded px-2 py-1">
                      <option value="all">Tous</option>
                      <option value="vendeur">Vendeurs</option>
                      <option value="non_vendeur">Non-vendeurs</option>
                    </select>
                    <select value={filterLogsEtape} onChange={e => setFilterLogsEtape(e.target.value as any)} className="text-xs border rounded px-2 py-1">
                      <option value="all">Toutes étapes</option>
                      <option value="3">Étape 3 seule</option>
                      <option value="4">Étape 4 (complété)</option>
                    </select>
                    <button
                      onClick={importAllLogs}
                      disabled={importingAll}
                      className="bg-orange-500 text-white px-3 py-1 rounded text-xs hover:bg-orange-600 disabled:opacity-50 flex items-center gap-1"
                    >
                      {importingAll ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
                      Importer tout ({data.logs_not_in_v2
                        .filter(r => {
                          if (filterLogsVendeur === 'vendeur' && !r.is_vendeur) return false
                          if (filterLogsVendeur === 'non_vendeur' && r.is_vendeur) return false
                          if (filterLogsEtape === '3' && r.etape !== 3) return false
                          if (filterLogsEtape === '4' && r.etape !== 4) return false
                          return true
                        })
                        .filter(r => !importLogResults.has(r.uniqueId)).length})
                    </button>
                  </div>
                  {importAllResult && (
                    <div className="mb-3">
                      <div className={`text-sm font-medium mb-1 ${importAllResult.success ? 'text-green-600' : 'text-red-600'}`}>
                        {importAllResult.success
                          ? `Import terminé : ${importAllResult.found}/${importAllResult.total} fichiers créés`
                          : 'Erreur lors de l\'import batch'}
                      </div>
                      {importAllResult.importOutput && (
                        <pre className="bg-gray-900 text-green-400 p-3 rounded text-xs font-mono overflow-x-auto whitespace-pre-wrap max-h-64 overflow-y-auto">
                          {importAllResult.importOutput}
                        </pre>
                      )}
                    </div>
                  )}
                  <div className="overflow-x-auto max-h-96 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-3 py-2 text-left">UniqueId</th>
                          <th className="px-3 py-2 text-left">Étape</th>
                          <th className="px-3 py-2 text-left">Vente</th>
                          <th className="px-3 py-2 text-left">Type</th>
                          <th className="px-3 py-2 text-left">Adresse</th>
                          <th className="px-3 py-2 text-left">Agence</th>
                          <th className="px-3 py-2 text-left">Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.logs_not_in_v2
                          .filter(r => {
                            if (filterLogsVendeur === 'vendeur' && !r.is_vendeur) return false
                            if (filterLogsVendeur === 'non_vendeur' && r.is_vendeur) return false
                            if (filterLogsEtape === '3' && r.etape !== 3) return false
                            if (filterLogsEtape === '4' && r.etape !== 4) return false
                            return true
                          })
                          .map((r, i) => {
                            const result = importLogResults.get(r.uniqueId)
                            const isImporting = importingLog.has(r.uniqueId)
                            return (
                              <React.Fragment key={i}>
                                <tr className={`border-t ${r.is_vendeur ? 'bg-orange-50' : ''} ${result?.success ? 'opacity-50' : ''}`}>
                                  <td className="px-3 py-1.5 font-mono">{r.uniqueId}</td>
                                  <td className="px-3 py-1.5">
                                    <Badge color={r.etape === 4 ? 'purple' : 'blue'}>{r.etape}</Badge>
                                  </td>
                                  <td className="px-3 py-1.5">{r.vente}</td>
                                  <td className="px-3 py-1.5">
                                    <Badge color={r.is_vendeur ? 'orange' : 'gray'}>{r.is_vendeur ? 'Vendeur' : 'Non-V'}</Badge>
                                  </td>
                                  <td className="px-3 py-1.5">{r.adresse}</td>
                                  <td className="px-3 py-1.5 font-mono text-gray-400">{r.idAgence}</td>
                                  <td className="px-3 py-1.5">
                                    {result ? (
                                      <Badge color={result.success ? 'green' : 'red'}>{result.success ? 'OK' : 'Erreur'}</Badge>
                                    ) : (
                                      <button
                                        onClick={() => importLog(r.uniqueId)}
                                        disabled={isImporting}
                                        className="bg-blue-500 text-white px-2 py-0.5 rounded text-xs hover:bg-blue-600 disabled:opacity-50 flex items-center gap-1"
                                      >
                                        {isImporting ? <Loader2 className="w-3 h-3 animate-spin" /> : null}
                                        Importer
                                      </button>
                                    )}
                                  </td>
                                </tr>
                                {result && (
                                  <tr>
                                    <td colSpan={7} className="px-3 py-2">
                                      <pre className="bg-gray-900 text-green-400 p-3 rounded text-xs font-mono overflow-x-auto whitespace-pre-wrap max-h-48 overflow-y-auto">
                                        {result.output}
                                      </pre>
                                    </td>
                                  </tr>
                                )}
                              </React.Fragment>
                            )
                          })
                        }
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* ======== KPI CARDS PERTES ======== */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-red-500">
              <div className="text-sm text-gray-500">Manquants en V3</div>
              <div className="text-3xl font-bold text-red-600">{data.missing_in_v3_stats.total}</div>
              <div className="text-xs text-gray-400 mt-1">
                {data.missing_in_v3_stats.vendeurs} vendeurs, {data.missing_in_v3_stats.non_vendeurs} non-vendeurs
              </div>
              <div className="text-xs mt-1">
                <Badge color="orange">{data.missing_in_v3_stats.hors_zone} hors zone</Badge>{' '}
                <Badge color="red">{data.missing_in_v3_stats.en_zone} en zone</Badge>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-yellow-500">
              <div className="text-sm text-gray-500">Date differente V3</div>
              <div className="text-3xl font-bold text-yellow-600">{data.different_date_in_v3.length}</div>
              <div className="text-xs text-gray-400 mt-1">Existent en V3, autre date</div>
            </div>

            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-orange-500">
              <div className="text-sm text-gray-500">Manquants en V2</div>
              <div className="text-3xl font-bold text-orange-600">{data.missing_in_v2.length}</div>
              <div className="text-xs text-gray-400 mt-1">Presents en V3, absents en V2</div>
            </div>

            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-purple-500">
              <div className="text-sm text-gray-500">Hors zone importes</div>
              <div className="text-3xl font-bold text-purple-600">{data.v2.hors_zone_importes}</div>
              <div className="text-xs text-gray-400 mt-1">Biens V2 hors zone agence</div>
            </div>
          </div>

          {/* ======== DÉTAIL ESTIMATEUR ======== */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="px-6 py-4 border-b border-gray-200 bg-indigo-50">
              <h3 className="text-base font-semibold text-indigo-800">Detail Estimateur</h3>
            </div>
            <div className="p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs text-gray-500 uppercase">
                    <th className="px-3 py-2">Source</th>
                    <th className="px-3 py-2 text-center">Etape 3</th>
                    <th className="px-3 py-2 text-center">Vendeurs</th>
                    <th className="px-3 py-2 text-center">Non-vendeurs</th>
                    <th className="px-3 py-2 text-center">Etape 4</th>
                    <th className="px-3 py-2 text-center">Vendeurs</th>
                    <th className="px-3 py-2 text-center">Non-vendeurs</th>
                    <th className="px-3 py-2 text-center">Taux 3→4</th>
                  </tr>
                </thead>
                <tbody>
                  {([
                    ['FR', data.estimateur.fr],
                    ['ES', data.estimateur.es],
                    ['Logs', data.logs]
                  ] as [string, EstimateurLevel | typeof data.logs][]).map(([label, d]) => (
                    <tr key={label} className="border-t">
                      <td className="px-3 py-2 font-medium">{label}</td>
                      <td className="px-3 py-2 text-center font-bold">{d.etape3}</td>
                      <td className="px-3 py-2 text-center text-red-600">{d.etape3_vendeurs}</td>
                      <td className="px-3 py-2 text-center text-gray-500">{d.etape3_non_vendeurs}</td>
                      <td className="px-3 py-2 text-center font-bold">{d.etape4}</td>
                      <td className="px-3 py-2 text-center text-red-600">{d.etape4_vendeurs}</td>
                      <td className="px-3 py-2 text-center text-gray-500">{d.etape4_non_vendeurs}</td>
                      <td className="px-3 py-2 text-center">
                        {d.etape3 > 0 ? <span className={d.etape4 / d.etape3 < 0.9 ? 'text-red-600 font-bold' : 'text-green-600'}>{((d.etape4 / d.etape3) * 100).toFixed(1)}%</span> : '-'}
                      </td>
                    </tr>
                  ))}
                  <tr className="border-t-2 border-gray-300 bg-gray-50 font-bold">
                    <td className="px-3 py-2">Total</td>
                    <td className="px-3 py-2 text-center">{estimTotal3}</td>
                    <td className="px-3 py-2 text-center text-red-600">{data.estimateur.fr.etape3_vendeurs + data.estimateur.es.etape3_vendeurs}</td>
                    <td className="px-3 py-2 text-center text-gray-500">{data.estimateur.fr.etape3_non_vendeurs + data.estimateur.es.etape3_non_vendeurs}</td>
                    <td className="px-3 py-2 text-center">{estimTotal4}</td>
                    <td className="px-3 py-2 text-center text-red-600">{data.estimateur.fr.etape4_vendeurs + data.estimateur.es.etape4_vendeurs}</td>
                    <td className="px-3 py-2 text-center text-gray-500">{data.estimateur.fr.etape4_non_vendeurs + data.estimateur.es.etape4_non_vendeurs}</td>
                    <td className="px-3 py-2 text-center">
                      {estimTotal3 > 0 ? ((estimTotal4 / estimTotal3) * 100).toFixed(1) + '%' : '-'}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* ======== TABLEAU MANQUANTS V3 ======== */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div
              className="px-6 py-4 border-b border-gray-200 flex items-center justify-between cursor-pointer bg-red-50 hover:bg-red-100 transition"
              onClick={() => setShowMissingV3(!showMissingV3)}
            >
              <h3 className="text-base font-semibold flex items-center gap-2 text-red-800">
                <AlertTriangle className="w-5 h-5 text-red-500" />
                Manquants en V3 ({data.missing_in_v3.length})
                {data.missing_in_v3_stats.en_zone > 0 && (
                  <Badge color="red">{data.missing_in_v3_stats.en_zone} en zone = vrais manquants</Badge>
                )}
              </h3>
              <div className="flex items-center gap-3">
                {showMissingV3 && data.missing_in_v3.length > 0 && (
                  <>
                    <button
                      onClick={e => { e.stopPropagation(); retryBatch(filteredMissingV3.filter(r => r.is_vendeur).map(r => r.id)) }}
                      disabled={retryingAll}
                      className="bg-red-600 text-white px-3 py-1.5 rounded text-xs font-medium hover:bg-red-700 disabled:opacity-50 flex items-center gap-1"
                    >
                      {retryingAll ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
                      Importer vendeurs
                    </button>
                    <button
                      onClick={e => { e.stopPropagation(); retryBatch(filteredMissingV3.map(r => r.id)) }}
                      disabled={retryingAll}
                      className="bg-gray-600 text-white px-3 py-1.5 rounded text-xs font-medium hover:bg-gray-700 disabled:opacity-50 flex items-center gap-1"
                    >
                      Tout importer ({filteredMissingV3.length})
                    </button>
                  </>
                )}
                {showMissingV3 ? <ChevronUp className="w-5 h-5 text-gray-400" /> : <ChevronDown className="w-5 h-5 text-gray-400" />}
              </div>
            </div>

            {showMissingV3 && data.missing_in_v3.length > 0 && (
              <>
                {/* Filtres */}
                <div className="px-6 py-3 bg-gray-50 border-b flex items-center gap-4">
                  <Filter className="w-4 h-4 text-gray-400" />
                  <select value={filterVendeur} onChange={e => setFilterVendeur(e.target.value as any)} className="text-sm border rounded px-2 py-1">
                    <option value="all">Tous</option>
                    <option value="vendeur">Vendeurs uniquement</option>
                    <option value="non_vendeur">Non-vendeurs uniquement</option>
                  </select>
                  <select value={filterHorsZone} onChange={e => setFilterHorsZone(e.target.value as any)} className="text-sm border rounded px-2 py-1">
                    <option value="all">Tous</option>
                    <option value="en_zone">En zone uniquement</option>
                    <option value="hors_zone">Hors zone uniquement</option>
                  </select>
                  <span className="text-xs text-gray-500">{filteredMissingV3.length} resultats</span>
                </div>

                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Zone</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID V2</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">% Vente</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Adresse</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">CP Bien</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Action</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {filteredMissingV3.map(row => {
                        const result = retryResults.get(row.id)
                        const isRetrying = retrying.has(row.id)
                        return (
                          <tr key={row.id} className={row.hors_zone ? 'bg-orange-50' : row.is_vendeur ? 'bg-red-50' : ''}>
                            <td className="px-3 py-2 text-sm">
                              <Badge color={row.is_vendeur ? 'red' : 'gray'}>{row.is_vendeur ? 'VENDEUR' : 'non-vend'}</Badge>
                            </td>
                            <td className="px-3 py-2 text-sm">
                              <Badge color={row.hors_zone ? 'orange' : 'green'}>{row.hors_zone ? 'HORS ZONE' : 'En zone'}</Badge>
                            </td>
                            <td className="px-3 py-2 text-sm font-mono">{row.id}</td>
                            <td className="px-3 py-2 text-sm">{row.pourcentage_vente}</td>
                            <td className="px-3 py-2 text-sm whitespace-nowrap">{row.date_acquisition}</td>
                            <td className="px-3 py-2 text-sm max-w-xs truncate" title={row.adresse}>{row.adresse}</td>
                            <td className="px-3 py-2 text-sm font-mono">{row.cp_bien || '-'}</td>
                            <td className="px-3 py-2 text-sm">
                              {result ? (
                                <span className={`flex items-center gap-1 text-xs ${result.success ? 'text-green-600' : 'text-red-600'}`}>
                                  {result.success ? <Check className="w-3 h-3" /> : <X className="w-3 h-3" />}
                                  {result.success ? 'OK' : 'Erreur'}
                                </span>
                              ) : (
                                <button
                                  onClick={() => retryOne(row.id)}
                                  disabled={isRetrying}
                                  className="text-blue-600 hover:text-blue-800 text-xs font-medium flex items-center gap-1 disabled:opacity-50"
                                >
                                  {isRetrying ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
                                  Importer
                                </button>
                              )}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            {showMissingV3 && data.missing_in_v3.length === 0 && (
              <div className="p-6 text-center text-green-600">
                <Check className="w-6 h-6 mx-auto mb-1" />
                Aucun lead manquant en V3
              </div>
            )}
          </div>

          {/* ======== TABLEAU DATE DIFFÉRENTE ======== */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div
              className="px-6 py-4 border-b border-gray-200 flex items-center justify-between cursor-pointer bg-yellow-50 hover:bg-yellow-100 transition"
              onClick={() => setShowDiffDate(!showDiffDate)}
            >
              <h3 className="text-base font-semibold flex items-center gap-2 text-yellow-800">
                <AlertTriangle className="w-5 h-5 text-yellow-500" />
                Date differente en V3 ({data.different_date_in_v3.length})
              </h3>
              {showDiffDate ? <ChevronUp className="w-5 h-5 text-gray-400" /> : <ChevronDown className="w-5 h-5 text-gray-400" />}
            </div>

            {showDiffDate && data.different_date_in_v3.length > 0 && (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID V2</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">% Vente</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date V2</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date V3</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Adresse</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {data.different_date_in_v3.map(row => (
                      <tr key={row.id} className="bg-yellow-50">
                        <td className="px-3 py-2 text-sm"><Badge color={row.is_vendeur ? 'red' : 'gray'}>{row.is_vendeur ? 'VENDEUR' : 'non-vend'}</Badge></td>
                        <td className="px-3 py-2 text-sm font-mono">{row.id}</td>
                        <td className="px-3 py-2 text-sm">{row.pourcentage_vente}</td>
                        <td className="px-3 py-2 text-sm whitespace-nowrap">{row.date_acquisition}</td>
                        <td className="px-3 py-2 text-sm whitespace-nowrap">{row.v3_created_date ? new Date(row.v3_created_date).toISOString().substring(0, 10) : '-'}</td>
                        <td className="px-3 py-2 text-sm">{row.adresse}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* ======== TABLEAU MANQUANTS V2 ======== */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div
              className="px-6 py-4 border-b border-gray-200 flex items-center justify-between cursor-pointer bg-orange-50 hover:bg-orange-100 transition"
              onClick={() => setShowMissingV2(!showMissingV2)}
            >
              <h3 className="text-base font-semibold flex items-center gap-2 text-orange-800">
                <AlertTriangle className="w-5 h-5 text-orange-500" />
                Manquants en V2 ({data.missing_in_v2.length})
              </h3>
              {showMissingV2 ? <ChevronUp className="w-5 h-5 text-gray-400" /> : <ChevronDown className="w-5 h-5 text-gray-400" />}
            </div>

            {showMissingV2 && data.missing_in_v2.length > 0 && (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Projet vente</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Demo</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Origin</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Adresse</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {data.missing_in_v2.map((row, i) => (
                      <tr key={i} className="bg-orange-50">
                        <td className="px-3 py-2 text-sm">{row.sale_project || '-'}</td>
                        <td className="px-3 py-2 text-sm">{row.demo ? <Badge color="yellow">Demo</Badge> : 'Non'}</td>
                        <td className="px-3 py-2 text-sm">{row.origin || '-'}</td>
                        <td className="px-3 py-2 text-sm whitespace-nowrap">{new Date(row.created_date).toISOString().substring(0, 19)}</td>
                        <td className="px-3 py-2 text-sm">{row.address}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Message succès si aucune perte */}
          {data.missing_in_v3.length === 0 && data.missing_in_v2.length === 0 && data.different_date_in_v3.length === 0 && (
            <div className="bg-green-50 border border-green-200 rounded-lg p-6 text-center text-green-700">
              <Check className="w-8 h-8 mx-auto mb-2" />
              <span className="text-lg font-medium">Aucune perte de donnees detectee pour le {formatDateFr(data.date)}</span>
            </div>
          )}
        </>
      )}
    </div>
  )
}

export default LeadsIntegrityPage
