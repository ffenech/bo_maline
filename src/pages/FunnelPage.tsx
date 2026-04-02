import { useState, useEffect, useMemo } from 'react'
import { cachedFetch } from '../lib/fetchCache'

interface FunnelStep {
  path: string
  label: string
  pageViews: number
  users: number
}

interface FunnelData {
  steps: FunnelStep[]
  altSteps: FunnelStep[]
  period: { startDate: string; endDate: string }
}

type Period = '7d' | '30d' | '90d'

function getPeriodDates(period: Period): { startDate: string; endDate: string } {
  const end = new Date()
  const fmt = (d: Date) => d.toISOString().slice(0, 10)
  const start = new Date(end)
  switch (period) {
    case '7d': start.setDate(start.getDate() - 7); break
    case '30d': start.setDate(start.getDate() - 30); break
    case '90d': start.setDate(start.getDate() - 90); break
  }
  return { startDate: fmt(start), endDate: 'today' }
}

function FunnelBar({ step, maxUsers, prevUsers, index }: {
  step: FunnelStep
  maxUsers: number
  prevUsers: number | null
  index: number
}) {
  // Barre par rapport a l'etape precedente (100% si premiere etape)
  const pct = Math.min(100, prevUsers !== null && prevUsers > 0 ? (step.users / prevUsers) * 100 : 100)
  const dropPct = prevUsers !== null && prevUsers > 0
    ? ((prevUsers - step.users) / prevUsers * 100)
    : null
  const totalDropPct = maxUsers > 0 ? (step.users / maxUsers * 100) : 0

  // Use a single blue palette that darkens as funnel narrows
  const lightness = Math.max(30, 55 - index * 1.5)
  const barColor = `hsl(220, 70%, ${lightness}%)`

  return (
    <div className="flex items-center gap-3 group">
      <div className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white shrink-0"
        style={{ backgroundColor: barColor }}>
        {index + 1}
      </div>

      <div className="w-48 shrink-0">
        <div className="text-sm font-medium text-gray-800 truncate">{step.label}</div>
        <div className="text-xs text-gray-400 truncate">{step.path}</div>
      </div>

      <div className="flex-1 flex items-center gap-2">
        <div className="flex-1 h-7 rounded bg-gray-100">
          <div
            className="h-full rounded transition-all duration-700 ease-out"
            style={{ width: `${Math.max(pct, 1)}%`, backgroundColor: barColor }}
          />
        </div>
        <span className="text-sm font-semibold text-gray-800 w-16 text-right tabular-nums shrink-0">
          {step.users.toLocaleString('fr-FR')}
        </span>
      </div>

      <div className="w-20 text-right shrink-0">
        <div className="text-sm font-semibold text-gray-700">{totalDropPct.toFixed(1)}%</div>
        <div className="text-xs text-gray-400">du total</div>
      </div>

      <div className="w-24 text-right shrink-0">
        {dropPct !== null ? (
          <>
            <div className={`text-sm font-semibold ${dropPct > 50 ? 'text-red-600' : dropPct > 30 ? 'text-orange-500' : dropPct > 15 ? 'text-yellow-600' : 'text-green-600'}`}>
              -{dropPct.toFixed(1)}%
            </div>
            <div className="text-xs text-gray-400">vs préc.</div>
          </>
        ) : (
          <div className="text-xs text-gray-400">-</div>
        )}
      </div>
    </div>
  )
}

export default function FunnelPage() {
  const [funnelFr, setFunnelFr] = useState<FunnelData | null>(null)
  const [funnelEs, setFunnelEs] = useState<FunnelData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState<Period>('30d')
  const [activeTab, setActiveTab] = useState<'fr' | 'es' | 'compare'>('compare')

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true)
      setError(null)
      try {
        const { startDate, endDate } = getPeriodDates(period)
        const [frRes, esRes] = await Promise.all([
          cachedFetch<FunnelData>(`/api/ga4/conversion-funnel?country=fr&startDate=${startDate}&endDate=${endDate}`),
          cachedFetch<FunnelData>(`/api/ga4/conversion-funnel?country=es&startDate=${startDate}&endDate=${endDate}`),
        ])
        setFunnelFr(frRes)
        setFunnelEs(esRes)
      } catch (e: any) {
        setError(e.message || 'Erreur de chargement')
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [period])

  const frSteps = useMemo(() => funnelFr?.steps.filter(s => s.users > 0) || [], [funnelFr])
  const esSteps = useMemo(() => funnelEs?.steps.filter(s => s.users > 0) || [], [funnelEs])
  const frAlt = useMemo(() => funnelFr?.altSteps || [], [funnelFr])
  const esAlt = useMemo(() => funnelEs?.altSteps || [], [funnelEs])

  const getSummary = (steps: FunnelStep[], altSteps: FunnelStep[]) => {
    if (steps.length < 2) return null
    const homepage = steps.find(s => s.path === '/')
    const typologie = steps.find(s => s.path === '/prix-m2/estimation-typologie')
    const coordStep = steps.find(s => s.path === '/prix-m2/estimation-coordonnees')
    const phoneStep = steps.find(s => s.path === '/prix-m2/estimation-telephone')
    const smsStep = steps.find(s => s.path === '/prix-m2/verification-sms')
    const confirmStep = steps.find(s => s.path === '/prix-m2/confirmation-estimation')
    const askAddr = altSteps.find(s => s.path === '/ask-address')
    const totalVisitors = homepage?.users || 0
    const formStarted = typologie?.users || 0
    const confirmed = confirmStep?.users || 0
    return {
      totalVisitors,
      formStarted,
      formStartRate: totalVisitors > 0 ? (formStarted / totalVisitors * 100) : 0,
      askAddressFallback: askAddr?.users || 0,
      coordonnees: coordStep?.users || 0,
      telephone: phoneStep?.users || 0,
      smsVerif: smsStep?.users || 0,
      confirmed,
      conversionRate: totalVisitors > 0 ? (confirmed / totalVisitors * 100) : 0,
      formCompletionRate: formStarted > 0 ? (confirmed / formStarted * 100) : 0,
    }
  }

  const frSummary = getSummary(frSteps, frAlt)
  const esSummary = getSummary(esSteps, esAlt)

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-500">Chargement des donnees GA4...</span>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
        <p className="text-red-600 font-medium">Erreur : {error}</p>
        <button onClick={() => setPeriod(period)} className="mt-3 px-4 py-2 bg-red-100 text-red-700 rounded hover:bg-red-200">
          Reessayer
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header controls */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex gap-2">
          {(['compare', 'fr', 'es'] as const).map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                activeTab === tab
                  ? 'bg-blue-600 text-white shadow'
                  : 'bg-white text-gray-600 hover:bg-gray-100 border'
              }`}
            >
              {tab === 'compare' ? 'Comparaison FR / ES' : tab === 'fr' ? 'France' : 'Espagne'}
            </button>
          ))}
        </div>

        <div className="flex gap-2">
          {(['7d', '30d', '90d'] as const).map(p => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                period === p
                  ? 'bg-gray-800 text-white'
                  : 'bg-white text-gray-600 hover:bg-gray-100 border'
              }`}
            >
              {p === '7d' ? '7 jours' : p === '30d' ? '30 jours' : '90 jours'}
            </button>
          ))}
        </div>
      </div>

      {/* Summary cards */}
      {activeTab === 'compare' && frSummary && esSummary && (
        <div className="grid grid-cols-2 gap-4">
          <SummaryCard title="France" emoji="FR" summary={frSummary} color="blue" />
          <SummaryCard title="Espagne" emoji="ES" summary={esSummary} color="orange" />
        </div>
      )}

      {activeTab === 'fr' && frSummary && (
        <SummaryCard title="France" emoji="FR" summary={frSummary} color="blue" />
      )}

      {activeTab === 'es' && esSummary && (
        <SummaryCard title="Espagne" emoji="ES" summary={esSummary} color="orange" />
      )}

      {/* Funnel visualization */}
      {activeTab === 'compare' ? (
        <div className="grid grid-cols-2 gap-6">
          <FunnelPanel title="France" emoji="FR" steps={frSteps} altSteps={frAlt} />
          <FunnelPanel title="Espagne" emoji="ES" steps={esSteps} altSteps={esAlt} />
        </div>
      ) : activeTab === 'fr' ? (
        <FunnelPanel title="France" emoji="FR" steps={frSteps} altSteps={frAlt} />
      ) : (
        <FunnelPanel title="Espagne" emoji="ES" steps={esSteps} altSteps={esAlt} />
      )}
    </div>
  )
}

function SummaryCard({ title, emoji, summary, color }: {
  title: string
  emoji: string
  summary: {
    totalVisitors: number
    formStarted: number
    formStartRate: number
    askAddressFallback: number
    coordonnees: number
    telephone: number
    smsVerif: number
    confirmed: number
    conversionRate: number
    formCompletionRate: number
  }
  color: 'blue' | 'orange'
}) {
  const bg = color === 'blue' ? 'bg-blue-50 border-blue-200' : 'bg-orange-50 border-orange-200'
  const accent = color === 'blue' ? 'text-blue-700' : 'text-orange-700'
  const statBg = color === 'blue' ? 'bg-blue-100' : 'bg-orange-100'

  return (
    <div className={`${bg} border rounded-xl p-5`}>
      <h3 className={`text-lg font-bold ${accent} mb-3`}>{emoji} {title}</h3>
      <div className="grid grid-cols-3 gap-3">
        <StatBox label="Visiteurs HP" value={summary.totalVisitors.toLocaleString('fr-FR')} bg={statBg} />
        <StatBox label="Demarrent le form" value={`${summary.formStarted.toLocaleString('fr-FR')} (${summary.formStartRate.toFixed(1)}%)`} bg={statBg} />
        <StatBox label="Fallback adresse" value={summary.askAddressFallback.toLocaleString('fr-FR')} bg={statBg} sub="n'ont pas trouve leur adresse" />
        <StatBox label="Coordonnees" value={summary.coordonnees.toLocaleString('fr-FR')} bg={statBg} />
        <StatBox label="Telephone" value={summary.telephone.toLocaleString('fr-FR')} bg={statBg} />
        <StatBox label="Confirmations" value={summary.confirmed.toLocaleString('fr-FR')} bg={statBg} />
      </div>
      <div className="mt-3 flex gap-4">
        <div className={`${statBg} rounded-lg px-3 py-2 flex-1 text-center`}>
          <div className={`text-xl font-bold ${accent}`}>{summary.conversionRate.toFixed(2)}%</div>
          <div className="text-xs text-gray-500">Taux global (visiteur &rarr; confirmation)</div>
        </div>
        <div className={`${statBg} rounded-lg px-3 py-2 flex-1 text-center`}>
          <div className={`text-xl font-bold ${accent}`}>{summary.formCompletionRate.toFixed(1)}%</div>
          <div className="text-xs text-gray-500">Taux formulaire (typologie &rarr; confirmation)</div>
        </div>
      </div>
    </div>
  )
}

function StatBox({ label, value, bg, sub }: { label: string; value: string; bg: string; sub?: string }) {
  return (
    <div className={`${bg} rounded-lg px-3 py-2 text-center`}>
      <div className="text-sm font-bold text-gray-800">{value}</div>
      <div className="text-xs text-gray-500">{label}</div>
      {sub && <div className="text-[10px] text-gray-400 mt-0.5">{sub}</div>}
    </div>
  )
}

function FunnelPanel({ title, emoji, steps, altSteps }: {
  title: string
  emoji: string
  steps: FunnelStep[]
  altSteps: FunnelStep[]
}) {
  const maxUsers = steps.length > 0 ? Math.max(...steps.map(s => s.users)) : 0

  return (
    <div className="bg-white rounded-xl shadow-sm border p-6">
      <h3 className="text-lg font-bold text-gray-800 mb-4">{emoji} {title} - Tunnel de conversion</h3>

      {/* Info box about the flow */}
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 mb-4 text-xs text-gray-600">
        <strong>Parcours :</strong> HP &rarr; saisie adresse &rarr; si adresse trouvee, direct vers Typologie.
        Sinon, passage par la page "Adresse (fallback)" puis "Validation".
        {altSteps.length > 0 && altSteps[0].users > 0 && (
          <span className="ml-1 text-orange-600 font-medium">
            ({altSteps[0].users.toLocaleString('fr-FR')} users sont passes par le fallback adresse)
          </span>
        )}
      </div>

      {steps.length === 0 ? (
        <p className="text-gray-400 text-center py-8">Aucune donnee</p>
      ) : (
        <div className="space-y-2">
          {steps.map((step, i) => (
            <FunnelBar
              key={step.path}
              step={step}
              maxUsers={maxUsers}
              prevUsers={i > 0 ? steps[i - 1].users : null}
              index={i}
            />
          ))}
        </div>
      )}
    </div>
  )
}
