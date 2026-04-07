import { useState, useEffect, useMemo, lazy, Suspense } from 'react'
import { TrendingUp, Calendar, BarChart3 } from 'lucide-react'
import { cachedFetch } from '../lib/fetchCache'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'

const LeadsV1Page = lazy(() => import('./LeadsV1Page'))
const LeadsIntegrityPage = lazy(() => import('./LeadsIntegrityPage'))

interface DailyLead {
  date: string
  total_leads: number
}

interface DailyPhoneStats {
  date: string
  leads_with_phone: number
  leads_with_validated_phone: number
}

interface VisitorData {
  [date: string]: number
}

interface RemarksData {
  [date: string]: string
}

type TabType = 'all' | 'fr' | 'es' | 'v1' | 'integrity'

function LeadsPage() {
  const [activeTab, setActiveTab] = useState<TabType>('all')

  // Données pour tous les clients
  const [dailyLeads, setDailyLeads] = useState<DailyLead[]>([])
  const [dailyPhoneStats, setDailyPhoneStats] = useState<DailyPhoneStats[]>([])

  // Données pour les clients espagnols
  const [dailyLeadsEs, setDailyLeadsEs] = useState<DailyLead[]>([])
  const [dailyPhoneStatsEs, setDailyPhoneStatsEs] = useState<DailyPhoneStats[]>([])

  const [visitors, setVisitors] = useState<VisitorData>({})
  const [visitorsEs, setVisitorsEs] = useState<VisitorData>({})
  const [remarks, setRemarks] = useState<RemarksData>({})
  const [clientCountFr, setClientCountFr] = useState<number>(0)
  const [clientCountEs, setClientCountEs] = useState<number>(0)
  const [loading, setLoading] = useState(true)
  const [editingCell, setEditingCell] = useState<string | null>(null)

  // Données France = tous - Espagne
  const dailyLeadsFr = useMemo(() => {
    const esMap = new Map(dailyLeadsEs.map(d => [d.date.split('T')[0], d.total_leads]))
    return dailyLeads.map(d => ({
      date: d.date,
      total_leads: d.total_leads - (esMap.get(d.date.split('T')[0]) || 0)
    }))
  }, [dailyLeads, dailyLeadsEs])

  const dailyPhoneStatsFr = useMemo(() => {
    const esMap = new Map(dailyPhoneStatsEs.map(d => [d.date.split('T')[0], d]))
    return dailyPhoneStats.map(d => {
      const es = esMap.get(d.date.split('T')[0])
      return {
        date: d.date,
        leads_with_phone: d.leads_with_phone - (es?.leads_with_phone || 0),
        leads_with_validated_phone: d.leads_with_validated_phone - (es?.leads_with_validated_phone || 0)
      }
    })
  }, [dailyPhoneStats, dailyPhoneStatsEs])

  // Visiteurs FR = tous - ES
  const visitorsFr = useMemo(() => {
    const result: VisitorData = {}
    for (const [date, count] of Object.entries(visitors)) {
      result[date] = count - (visitorsEs[date] || 0)
    }
    return result
  }, [visitors, visitorsEs])

  // Sélectionner les données selon l'onglet actif
  const currentDailyLeads = activeTab === 'es' ? dailyLeadsEs : activeTab === 'fr' ? dailyLeadsFr : dailyLeads
  const currentDailyPhoneStats = activeTab === 'es' ? dailyPhoneStatsEs : activeTab === 'fr' ? dailyPhoneStatsFr : dailyPhoneStats
  const currentVisitors = activeTab === 'es' ? visitorsEs : activeTab === 'fr' ? visitorsFr : visitors

  useEffect(() => {
    const fetchData = async () => {
      const apiUrl = import.meta.env.VITE_API_URL || '/api'
      const todayStr = new Date().toISOString().split('T')[0]

      // Fonction de filtrage des dates (à partir du 14 septembre 2024)
      const filterByDate = <T extends { date: string }>(data: T[]): T[] => {
        const startDate = '2024-09-14'
        return data.filter((item) => {
          const dateStr = item.date.split('T')[0]
          if (dateStr < startDate) return false
          if (dateStr > todayStr) return false
          return true
        })
      }

      try {
        // Lancer toutes les requêtes en parallèle avec cache
        const [
          leadsData,
          phoneData,
          leadsEsData,
          phoneEsData,
          visitorsData,
          visitorsEsData,
          remarksData,
          clientLocalesData
        ] = await Promise.all([
          cachedFetch<DailyLead[]>(`${apiUrl}/leads/v2-daily`),
          cachedFetch<DailyPhoneStats[]>(`${apiUrl}/leads/v2-daily-phone`),
          cachedFetch<DailyLead[]>(`${apiUrl}/leads/v2-daily-es`),
          cachedFetch<DailyPhoneStats[]>(`${apiUrl}/leads/v2-daily-phone-es`),
          cachedFetch<VisitorData>(`${apiUrl}/ga4/daily-visitors-v2`),
          cachedFetch<any[]>(`${apiUrl}/ga4/daily-visitors-es`),
          cachedFetch<RemarksData>(`${apiUrl}/remarks`),
          cachedFetch<any>(`${apiUrl}/pub-stats?period=30d`)
        ])

        // Traiter les leads (tous)
        setDailyLeads(filterByDate(leadsData || []).reverse())

        // Traiter les statistiques téléphone (tous)
        setDailyPhoneStats(filterByDate(phoneData || []).reverse())

        // Traiter les leads espagnols
        setDailyLeadsEs(filterByDate(leadsEsData || []).reverse())

        // Traiter les statistiques téléphone espagnols
        setDailyPhoneStatsEs(filterByDate(phoneEsData || []).reverse())

        // Traiter les visiteurs GA4 (tous)
        const visitorsMap: VisitorData = {}
        if (Array.isArray(visitorsData)) {
          visitorsData.forEach((item: { date: string; visitors: number }) => {
            visitorsMap[item.date] = item.visitors
          })
        }

        // Traiter les visiteurs GA4 ES
        const visitorsEsMap: VisitorData = {}
        if (Array.isArray(visitorsEsData)) {
          visitorsEsData.forEach((item: { date: string; visitors: number }) => {
            visitorsEsMap[item.date] = item.visitors
          })
        }

        // Récupérer les visiteurs temps réel pour aujourd'hui (cache court 30s)
        try {
          const todayData = await cachedFetch<{ v2: number; v1: number; es: number; date: string }>(
            `${apiUrl}/ga4/today-visitors`, undefined, 30 * 1000
          )
          if (todayData?.date) {
            // Écraser les valeurs du jour avec les données temps réel (plus fraîches)
            if (todayData.v2 > 0) visitorsMap[todayData.date] = todayData.v2
            if (todayData.es > 0) visitorsEsMap[todayData.date] = todayData.es
          }
        } catch (e) {
          console.warn('⚠️ Visiteurs temps réel non disponibles:', e)
        }

        setVisitors(visitorsMap)
        setVisitorsEs(visitorsEsMap)

        // Traiter les remarques
        setRemarks(remarksData || {})

        // Compter les clients actifs par locale
        const activeClients = clientLocalesData?.clients || []
        setClientCountFr(activeClients.filter((c: any) => (c.locale || 'fr_FR') === 'fr_FR').length)
        setClientCountEs(activeClients.filter((c: any) => c.locale === 'es_ES').length)
      } catch (error) {
        console.error('Erreur:', error)
        // Fallback sur localStorage pour les remarques
        const savedRemarks = localStorage.getItem('leads-remarks')
        if (savedRemarks) {
          setRemarks(JSON.parse(savedRemarks))
        }
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [])

  const formatDateShort = (dateString: string) => {
    if (!dateString) return '-'
    return new Date(dateString).toLocaleDateString('fr-FR', {
      day: '2-digit',
      month: 'short'
    })
  }


  const handleRemarkChange = async (date: string, value: string) => {
    const updatedRemarks = {
      ...remarks,
      [date]: value
    }

    try {
      // Essayer de sauvegarder dans l'API
      const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/remarks`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ date, remark: value }),
      })

      if (response.ok) {
        // Mettre à jour l'état local
        setRemarks(updatedRemarks)
        console.log('Remarque sauvegardée en BDD:', date, value)
      } else {
        // Fallback sur localStorage si la table n'existe pas
        setRemarks(updatedRemarks)
        localStorage.setItem('leads-remarks', JSON.stringify(updatedRemarks))
        console.log('Remarque sauvegardée dans localStorage (table BDD non créée):', date, value)
      }
    } catch (error) {
      // Fallback sur localStorage en cas d'erreur
      setRemarks(updatedRemarks)
      localStorage.setItem('leads-remarks', JSON.stringify(updatedRemarks))
      console.log('Remarque sauvegardée dans localStorage (erreur BDD):', date, value)
    }
    setEditingCell(null)
  }

  const totalLeads = useMemo(() => {
    return currentDailyLeads.reduce((sum, day) => sum + day.total_leads, 0)
  }, [currentDailyLeads])

  const averageLeads = useMemo(() => {
    if (currentDailyLeads.length === 0) return 0
    return Math.round(totalLeads / currentDailyLeads.length)
  }, [currentDailyLeads, totalLeads])

  // Seuil minimum de visiteurs pour exclure les jours où GA4 n'avait pas assez de données
  const MIN_VISITORS = 50

  // Calculer le taux de conversion moyen et l'écart type
  const conversionStats = useMemo(() => {
    const rates: number[] = []
    currentDailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      if (visitorsCount >= MIN_VISITORS) {
        const rate = (day.total_leads / visitorsCount) * 100
        rates.push(rate)
      }
    })

    if (rates.length === 0) return { average: 0, stdDev: 0 }

    const average = rates.reduce((sum, rate) => sum + rate, 0) / rates.length
    const variance = rates.reduce((sum, rate) => sum + Math.pow(rate - average, 2), 0) / rates.length
    const stdDev = Math.sqrt(variance)

    return { average, stdDev }
  }, [currentDailyLeads, visitors])

  // Calculer le taux de conversion sur les 10 derniers jours
  const conversionStatsLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    const rates: number[] = []
    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      if (visitorsCount >= MIN_VISITORS) {
        const rate = (day.total_leads / visitorsCount) * 100
        rates.push(rate)
      }
    })

    if (rates.length === 0) return { average: 0, stdDev: 0 }

    const average = rates.reduce((sum, rate) => sum + rate, 0) / rates.length
    const variance = rates.reduce((sum, rate) => sum + Math.pow(rate - average, 2), 0) / rates.length
    const stdDev = Math.sqrt(variance)

    return { average, stdDev }
  }, [currentDailyLeads, visitors])

  // Calculer le % tel sur les 10 derniers jours
  const phonePercentLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    let totalLeadsCount = 0
    let totalWithPhone = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalLeadsCount += day.total_leads
      totalWithPhone += phoneStat?.leads_with_phone || 0
    })

    return totalLeadsCount > 0 ? (totalWithPhone / totalLeadsCount) * 100 : 0
  }, [currentDailyLeads, currentDailyPhoneStats])

  // Taux conversion vendeur avec tel (10 derniers jours)
  const conversionPhoneLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    let totalWithPhone = 0
    let totalVisitors = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalWithPhone += phoneStat?.leads_with_phone || 0
      totalVisitors += visitorsCount
    })

    return totalVisitors > 0 ? (totalWithPhone / totalVisitors) * 100 : 0
  }, [currentDailyLeads, currentDailyPhoneStats, visitors])

  // Taux conversion vendeur avec tel validés (10 derniers jours)
  const conversionValidatedPhoneLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    let totalValidated = 0
    let totalVisitors = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalValidated += phoneStat?.leads_with_validated_phone || 0
      totalVisitors += visitorsCount
    })

    return totalVisitors > 0 ? (totalValidated / totalVisitors) * 100 : 0
  }, [currentDailyLeads, currentDailyPhoneStats, visitors])

  // Commits impactants du site estimateur (estimerlogement.fr)
  const siteCommits: { date: string; impact: 'positive' | 'negative' | 'neutral'; summary: string }[] = [
    // Août-Sep 2025 — Stabilisation initiale
    { date: '2025-08-31', impact: 'positive', summary: 'Fix boucle infinie page typologie — déblocage étape 1' },
    { date: '2025-09-05', impact: 'positive', summary: 'Loading states boutons + remplacement alert() par Toasts' },
    { date: '2025-09-07', impact: 'positive', summary: 'Fix bouton étage RDC bloqué (floor=0 falsy) + distance non-éligible autorisée' },
    { date: '2025-09-09', impact: 'positive', summary: 'Intégration complète workflow estimation (insert, distance, prix) — nombreux flux cassés corrigés' },
    { date: '2025-09-10', impact: 'positive', summary: 'Suppression timeout 15s prix + cache résultat prix — moins de drop-off' },
    { date: '2025-09-11', impact: 'positive', summary: 'Fix boutons typologie mobile (overflow iPhone 12)' },
    // Oct 2025
    { date: '2025-10-21', impact: 'neutral', summary: 'Simplification UI page tel — CTA avec flèche, placeholder modifié' },
    { date: '2025-10-23', impact: 'negative', summary: 'Validation adresse BAN stricte — risque blocage si adresse imparfaite' },
    // Nov 2025
    { date: '2025-11-27', impact: 'positive', summary: 'Autocomplétion DOM-TOM (Réunion, Guadeloupe, Martinique)' },
    // Déc 2025
    { date: '2025-12-18', impact: 'positive', summary: 'Fix texte espagnol sur page FR + calendrier RDV + affichage prix amélioré' },
    { date: '2025-12-24', impact: 'positive', summary: 'Soumission tel non-bloquante + formatage +33 — moins d\'utilisateurs bloqués' },
    { date: '2025-12-29', impact: 'positive', summary: 'Fallback BAN si erreur API adresse — utilisateurs ne restent plus bloqués' },
    // Jan 2026
    { date: '2026-01-06', impact: 'positive', summary: 'Formulaire fallback + autocomplétion BAN si Google Maps HS' },
    { date: '2026-01-19', impact: 'negative', summary: 'Bouton tel plus petit (plus full-width) — cible réduite sur mobile' },
    { date: '2026-01-21', impact: 'neutral', summary: 'Refonte layout pages estimation + loading 600→800ms' },
    { date: '2026-01-23', impact: 'neutral', summary: 'Migration GTM vers Stape (1st party) — risque si mal configuré' },
    // Fév 2026
    { date: '2026-02-09', impact: 'neutral', summary: 'Chat bubbles dynamiques depuis API (risque si API lente)' },
    { date: '2026-02-10', impact: 'neutral', summary: 'Session tracking + analytics reset — changement de mesure' },
    { date: '2026-02-16', impact: 'positive', summary: 'Fix format tel SMS (0033→+33 E.164) — corrige échecs livraison SMS' },
    { date: '2026-02-20', impact: 'negative', summary: 'Champ obligatoire "nb pièces" ajouté + ordre étapes modifié — friction accrue' },
    { date: '2026-02-27', impact: 'positive', summary: 'SMS relance 5min au lieu de 30min — rattrape utilisateurs engagés' },
    // Mar 2026
    { date: '2026-03-06', impact: 'positive', summary: 'Facebook Advanced Matching — meilleure qualité trafic pub' },
    { date: '2026-03-13', impact: 'negative', summary: 'Codes vérification 3→5 chiffres — friction au moment critique' },
    { date: '2026-03-15', impact: 'positive', summary: 'Nettoyage tracking dupliqué — bundle plus léger, pages plus rapides' },
    { date: '2026-03-19', impact: 'positive', summary: 'Refactor init agent — chargement plus fiable via liens SMS' },
  ]

  // Données hebdomadaires pour les courbes de taux de conversion
  const weeklyConversionData = useMemo(() => {
    if (!currentDailyLeads.length) return []
    // Exclure aujourd'hui (journée incomplète qui fausserait la semaine en cours)
    const now = new Date()
    const todayKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
    // Grouper par semaine ISO (lundi → dimanche)
    const weekMap = new Map<string, { visitors: number; leads: number; validatedPhone: number }>()
    currentDailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      if (dateKey === todayKey) return
      const d = new Date(dateKey + 'T00:00:00')
      // Trouver le lundi de la semaine
      const dayOfWeek = d.getDay()
      const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek
      const monday = new Date(d)
      monday.setDate(d.getDate() + mondayOffset)
      const weekKey = monday.toISOString().split('T')[0]

      const visitorsCount = currentVisitors[dateKey] || 0
      const phoneStat = currentDailyPhoneStats.find(s => s.date.split('T')[0] === dateKey)
      const validatedPhone = phoneStat?.leads_with_validated_phone || 0

      const existing = weekMap.get(weekKey) || { visitors: 0, leads: 0, validatedPhone: 0 }
      existing.visitors += visitorsCount
      existing.leads += day.total_leads
      existing.validatedPhone += validatedPhone
      weekMap.set(weekKey, existing)
    })

    // Ne garder que les 20 dernières semaines avec assez de visiteurs (min 200/semaine)
    const MIN_WEEKLY_VISITORS = 200
    return Array.from(weekMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .filter(([weekStart, data]) => data.visitors >= MIN_WEEKLY_VISITORS && weekStart >= '2025-10-05')
      .map(([weekStart, data]) => {
        const d = new Date(weekStart + 'T00:00:00')
        const endOfWeek = new Date(d)
        endOfWeek.setDate(d.getDate() + 6)
        const label = `${d.getDate()}/${d.getMonth() + 1}`
        // Trouver les commits de cette semaine
        const weekCommits = siteCommits.filter(c => c.date >= weekStart && c.date <= endOfWeek.toISOString().split('T')[0])
        return {
          week: label,
          weekStart,
          tauxConversionLead: data.visitors > 0 ? parseFloat(((data.leads / data.visitors) * 100).toFixed(2)) : 0,
          tauxConversionTelValide: data.visitors > 0 ? parseFloat(((data.validatedPhone / data.visitors) * 100).toFixed(2)) : 0,
          commits: weekCommits,
        }
      })
  }, [currentDailyLeads, currentDailyPhoneStats, currentVisitors])

  // Fonction pour détecter le type d'anomalie (> 1 écart-type)
  const getAnomalyType = (rate: number): 'normal' | 'low' | 'high' => {
    const { average, stdDev } = conversionStats
    const diff = rate - average
    if (Math.abs(diff) <= stdDev) return 'normal'
    if (diff < 0) return 'low' // En dessous de la moyenne - alerte rouge
    return 'high' // Au dessus de la moyenne - positif vert
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="text-gray-600 mt-4">Chargement des leads...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-full overflow-hidden">
      {/* Onglets */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveTab('all')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'all'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Tous les clients ({clientCountFr + clientCountEs})
          </button>
          <button
            onClick={() => setActiveTab('fr')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'fr'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            France ({clientCountFr})
          </button>
          <button
            onClick={() => setActiveTab('es')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'es'
                ? 'border-orange-500 text-orange-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Espagne ({clientCountEs})
          </button>
          <button
            onClick={() => setActiveTab('v1')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'v1'
                ? 'border-amber-500 text-amber-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Ancien esti
          </button>
          <button
            onClick={() => setActiveTab('integrity')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'integrity'
                ? 'border-red-500 text-red-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Intégrité V2/V3
          </button>
        </nav>
      </div>

      {activeTab === 'v1' ? (
        <Suspense fallback={
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-amber-600 mx-auto"></div>
              <p className="text-gray-600 mt-4">Chargement...</p>
            </div>
          </div>
        }>
          <LeadsV1Page />
        </Suspense>
      ) : activeTab === 'integrity' ? (
        <Suspense fallback={
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-red-600 mx-auto"></div>
              <p className="text-gray-600 mt-4">Chargement...</p>
            </div>
          </div>
        }>
          <LeadsIntegrityPage />
        </Suspense>
      ) : (
      <>
      {/* Statistiques */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-blue-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-blue-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Total Leads étape 3 vendeurs</p>
              <p className="text-xl font-bold text-gray-900">{totalLeads.toLocaleString('fr-FR')}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-green-100 rounded-lg">
              <BarChart3 className="w-5 h-5 text-green-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Moyenne leads étape 3 vendeur / jour</p>
              <p className="text-xl font-bold text-gray-900">{averageLeads}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-purple-100 rounded-lg">
              <Calendar className="w-5 h-5 text-purple-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Jours suivis</p>
              <p className="text-xl font-bold text-gray-900">{currentDailyLeads.length}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-orange-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-orange-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion étape 3 vendeurs</p>
              <p className="text-xl font-bold text-gray-900">{conversionStats.average.toFixed(2)}%</p>
              <p className="text-xs text-gray-500 mt-0.5">±{conversionStats.stdDev.toFixed(2)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-cyan-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-cyan-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion étape 3 (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{conversionStatsLast10Days.average.toFixed(2)}%</p>
              <p className="text-xs text-gray-500 mt-0.5">±{conversionStatsLast10Days.stdDev.toFixed(2)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-pink-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-pink-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">% tel (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{phonePercentLast10Days.toFixed(1)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-teal-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-teal-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion vendeur avec tel (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{conversionPhoneLast10Days.toFixed(2)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-indigo-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-indigo-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion vendeur avec tel validés (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{conversionValidatedPhoneLast10Days.toFixed(2)}%</p>
            </div>
          </div>
        </div>
      </div>

      {/* Tableau */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900">Détail journalier {activeTab === 'es' ? '- Espagne' : activeTab === 'fr' ? '- France' : ''}</h3>
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
                {currentDailyLeads.map((day, index) => (
                  <th key={index} className="px-2 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[80px]">
                    {formatDateShort(day.date)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {currentDailyLeads.length === 0 ? (
                <tr>
                  <td colSpan={currentDailyLeads.length + 1} className="px-6 py-8 text-center text-gray-500">
                    Aucun lead trouvé {activeTab === 'es' ? 'pour les clients espagnols' : activeTab === 'fr' ? 'pour les clients français' : 'pour les comptes Estimateur V2'} depuis le 1er septembre
                  </td>
                </tr>
              ) : (
                <>
                  {/* Ligne Nombre visiteurs */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Nombre visiteurs
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      // Normaliser la date de la BDD (ISO) en format YYYY-MM-DD
                      const dateKey = day.date.split('T')[0]
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            {currentVisitors[dateKey] || 0}
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Leads vendeurs */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Leads vendeurs
                    </td>
                    {currentDailyLeads.map((day, index) => (
                      <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                          {day.total_leads}
                        </span>
                      </td>
                    ))}
                  </tr>
                  {/* Ligne Taux conversion lead vendeur */}
                  <tr className="hover:bg-gray-50 transition-colors bg-purple-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-purple-50 z-10">
                      Taux conversion lead vendeur
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const visitorsCount = currentVisitors[dateKey] || 0
                      const leadsCount = day.total_leads
                      const rate = visitorsCount > 0
                        ? (leadsCount / visitorsCount) * 100
                        : 0
                      const conversionRate = rate.toFixed(2)
                      const anomalyType = getAnomalyType(rate)

                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center relative">
                          {anomalyType === 'low' && (
                            <div className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full animate-pulse z-20"></div>
                          )}
                          {anomalyType === 'high' && (
                            <div className="absolute top-1 right-1 w-2 h-2 bg-green-500 rounded-full animate-pulse z-20"></div>
                          )}
                          <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-bold ${
                            anomalyType === 'low'
                              ? 'bg-red-100 text-red-800 ring-1 ring-red-400 shadow-lg'
                              : anomalyType === 'high'
                              ? 'bg-green-100 text-green-800 ring-1 ring-green-400 shadow-lg'
                              : 'bg-purple-100 text-purple-800'
                          }`}>
                            {conversionRate}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Leads vendeurs avec tel */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Leads vendeurs avec tel
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-teal-100 text-teal-800">
                            {phoneStat?.leads_with_phone || 0}
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne % leads vendeurs avec tel */}
                  <tr className="hover:bg-gray-50 transition-colors bg-teal-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-teal-50 z-10">
                      % leads vendeurs avec tel
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      const leadsWithPhone = phoneStat?.leads_with_phone || 0
                      const totalLeadsDay = day.total_leads
                      const percentage = totalLeadsDay > 0
                        ? (leadsWithPhone / totalLeadsDay) * 100
                        : 0
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-teal-100 text-teal-800">
                            {percentage.toFixed(1)}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Taux conversion avec tel */}
                  <tr className="hover:bg-gray-50 transition-colors bg-teal-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-teal-50 z-10">
                      Taux conversion avec tel
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const visitorsCount = currentVisitors[dateKey] || 0
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      const leadsWithPhone = phoneStat?.leads_with_phone || 0
                      const rate = visitorsCount > 0
                        ? (leadsWithPhone / visitorsCount) * 100
                        : 0
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-bold bg-teal-100 text-teal-800">
                            {rate.toFixed(2)}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Leads vendeurs avec tel validés */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Leads vendeurs avec tel validés
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800">
                            {phoneStat?.leads_with_validated_phone || 0}
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne % leads vendeurs avec tel validés */}
                  <tr className="hover:bg-gray-50 transition-colors bg-indigo-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-indigo-50 z-10">
                      % leads vendeurs avec tel validés
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      const validatedPhone = phoneStat?.leads_with_validated_phone || 0
                      const totalLeadsDay = day.total_leads
                      const percentage = totalLeadsDay > 0
                        ? (validatedPhone / totalLeadsDay) * 100
                        : 0
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800">
                            {percentage.toFixed(1)}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Remarques */}
                  <tr className="hover:bg-gray-50 transition-colors bg-yellow-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-yellow-50 z-10">
                      Remarque
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      return (
                        <td key={index} className="px-2 py-3 text-sm text-center relative">
                          {editingCell === `remark-${dateKey}` ? (
                            <>
                              {/* Backdrop invisible pour fermer en cliquant à l'extérieur */}
                              <div
                                className="fixed inset-0 z-40"
                                onClick={(e) => {
                                  e.stopPropagation()
                                  // Récupérer la valeur du textarea avant de fermer
                                  const textarea = document.querySelector(`textarea[data-date="${dateKey}"]`) as HTMLTextAreaElement
                                  if (textarea) {
                                    handleRemarkChange(dateKey, textarea.value)
                                  } else {
                                    setEditingCell(null)
                                  }
                                }}
                              />
                              <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 z-50 mb-2">
                                <div className="bg-white border-2 border-yellow-400 rounded-lg shadow-2xl p-3 w-64">
                                  <textarea
                                    data-date={dateKey}
                                    className="w-full px-3 py-2 text-sm border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-yellow-500 resize-none"
                                    defaultValue={remarks[dateKey] || ''}
                                    autoFocus
                                    rows={4}
                                    placeholder="Ajouter une remarque pour ce jour..."
                                    onClick={(e) => e.stopPropagation()}
                                    onKeyDown={(e) => {
                                      if (e.key === 'Enter' && e.ctrlKey) {
                                        handleRemarkChange(dateKey, e.currentTarget.value)
                                      } else if (e.key === 'Escape') {
                                        setEditingCell(null)
                                      }
                                    }}
                                  />
                                  <div className="mt-2 flex items-center justify-between">
                                    <div className="text-xs text-gray-500">
                                      Ctrl+Enter pour sauvegarder, Échap pour annuler
                                    </div>
                                    {remarks[dateKey] && (
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          handleRemarkChange(dateKey, '')
                                        }}
                                        className="text-xs text-red-600 hover:text-red-800 font-medium px-2 py-1 hover:bg-red-50 rounded transition-colors"
                                      >
                                        Supprimer
                                      </button>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </>
                          ) : null}
                          <div
                            className="cursor-pointer hover:bg-yellow-100 px-2 py-1 rounded min-h-[24px] text-xs group relative"
                            onClick={() => setEditingCell(`remark-${dateKey}`)}
                          >
                            {remarks[dateKey] ? (
                              <>
                                {/* Badge avec remarque */}
                                <div className="flex items-center justify-center">
                                  <span className="inline-flex items-center px-2 py-1 rounded-full bg-blue-500 text-white font-semibold ring-2 ring-blue-600 shadow-md">
                                    Note
                                  </span>
                                </div>
                                {/* Tooltip au survol */}
                                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 whitespace-normal w-64 max-w-xs">
                                  <div className="font-semibold mb-1 text-blue-300">Note du {formatDateShort(day.date)}</div>
                                  <div className="text-gray-100">{remarks[dateKey]}</div>
                                  {/* Flèche */}
                                  <div className="absolute top-full left-1/2 transform -translate-x-1/2 -mt-1">
                                    <div className="border-8 border-transparent border-t-gray-900"></div>
                                  </div>
                                </div>
                              </>
                            ) : (
                              <span className="text-gray-400 hover:text-gray-600">+</span>
                            )}
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                </>
              )}
            </tbody>
          </table>
            </div>
          </div>
        </div>
      </div>

      {/* Courbes hebdomadaires taux de conversion */}
      {weeklyConversionData.length > 1 && (() => {
        const impactColor = (impact: string) => impact === 'positive' ? '#16a34a' : impact === 'negative' ? '#dc2626' : '#9ca3af'
        const impactLabel = (impact: string) => impact === 'positive' ? '↑' : impact === 'negative' ? '↓' : '•'

        // Custom dot qui marque les semaines avec commits
        const CommitDot = (props: any) => {
          const { cx, cy, payload } = props
          if (!payload?.commits?.length) return <circle cx={cx} cy={cy} r={3} fill={props.stroke} />
          const mainImpact = payload.commits.find((c: any) => c.impact !== 'neutral')?.impact || 'neutral'
          return (
            <g>
              <circle cx={cx} cy={cy} r={6} fill={impactColor(mainImpact)} stroke="#fff" strokeWidth={2} />
              <text x={cx} y={cy - 12} textAnchor="middle" fontSize={10} fontWeight="bold" fill={impactColor(mainImpact)}>
                {impactLabel(mainImpact)}
              </text>
            </g>
          )
        }

        // Custom tooltip avec détail des commits
        const CommitTooltip = ({ active, payload, dataKey }: any) => {
          if (!active || !payload?.[0]) return null
          const data = payload[0].payload
          const value = data[dataKey]
          const commits = data.commits || []
          return (
            <div className="bg-white border border-gray-300 rounded-lg shadow-lg p-3 max-w-xs">
              <p className="text-sm font-semibold text-gray-900 mb-1">Semaine du {data.week} — {value}%</p>
              {commits.length > 0 && (
                <div className="border-t border-gray-200 pt-2 mt-1 space-y-1.5">
                  <p className="text-xs font-semibold text-gray-600">Déploiements estimateur :</p>
                  {commits.map((c: any, i: number) => {
                    const [y, m, d] = c.date.split('-')
                    return (
                    <div key={i} className="flex items-start gap-1.5">
                      <span className="text-xs mt-0.5" style={{ color: impactColor(c.impact) }}>
                        {c.impact === 'positive' ? '▲' : c.impact === 'negative' ? '▼' : '●'}
                      </span>
                      <p className="text-xs text-gray-700"><span className="font-semibold text-gray-500">{d}/{m}/{y}</span> {c.summary}</p>
                    </div>
                    )
                  })}
                </div>
              )}
            </div>
          )
        }

        // Semaines avec commits pour les ReferenceLine
        const weeksWithCommits = weeklyConversionData.filter(w => w.commits.length > 0)

        return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-4">Taux conversion vendeur avec tel validés (hebdo)</h3>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={weeklyConversionData} margin={{ top: 20 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="week" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v}%`} />
                <Tooltip content={<CommitTooltip dataKey="tauxConversionTelValide" />} />
                {weeksWithCommits.map(w => {
                  const mainImpact = w.commits.find(c => c.impact !== 'neutral')?.impact || 'neutral'
                  return <ReferenceLine key={w.week} x={w.week} stroke={impactColor(mainImpact)} strokeDasharray="3 3" strokeOpacity={0.5} />
                })}
                <Line type="monotone" dataKey="tauxConversionTelValide" stroke="#6366f1" strokeWidth={2} dot={<CommitDot stroke="#6366f1" />} activeDot={{ r: 5 }} name="Taux conv. tel validés" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-4">Taux conversion lead vendeur (hebdo)</h3>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={weeklyConversionData} margin={{ top: 20 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="week" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v}%`} />
                <Tooltip content={<CommitTooltip dataKey="tauxConversionLead" />} />
                {weeksWithCommits.map(w => {
                  const mainImpact = w.commits.find(c => c.impact !== 'neutral')?.impact || 'neutral'
                  return <ReferenceLine key={w.week} x={w.week} stroke={impactColor(mainImpact)} strokeDasharray="3 3" strokeOpacity={0.5} />
                })}
                <Line type="monotone" dataKey="tauxConversionLead" stroke="#a855f7" strokeWidth={2} dot={<CommitDot stroke="#a855f7" />} activeDot={{ r: 5 }} name="Taux conv. lead vendeur" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          {/* Légende des annotations */}
          <div className="lg:col-span-2 flex items-center gap-6 text-xs text-gray-500 px-2">
            <span className="flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-full bg-green-600"></span> Commit positif pour la conversion</span>
            <span className="flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-full bg-red-600"></span> Commit négatif pour la conversion</span>
            <span className="text-gray-400">Survoler un point pour voir le détail des déploiements</span>
          </div>
        </div>
        )
      })()}

      {/* Légende */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-sm font-semibold text-gray-900 mb-4">Légende & Détection d'anomalies</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-purple-100 text-purple-800">
                {conversionStats.average.toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-gray-900">Taux normal</p>
              <p className="text-xs text-gray-500">Valeur dans la plage habituelle (moyenne +/- 1 écart-type)</p>
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1 relative">
              <div className="absolute -top-1 -right-1 w-3 h-3 bg-red-500 rounded-full animate-pulse"></div>
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-red-100 text-red-800 ring-2 ring-red-400">
                {Math.max(0, conversionStats.average - conversionStats.stdDev).toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-red-900">Anomalie basse</p>
              <p className="text-xs text-gray-500">Taux anormalement bas - Nécessite une vérification</p>
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1 relative">
              <div className="absolute -top-1 -right-1 w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-green-100 text-green-800 ring-2 ring-green-400">
                {(conversionStats.average + conversionStats.stdDev).toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-green-900">Anomalie haute</p>
              <p className="text-xs text-gray-500">Taux anormalement haut - Performance exceptionnelle</p>
            </div>
          </div>
        </div>
        <div className="mt-4 pt-4 border-t border-gray-200 text-xs text-gray-500">
          <p>Données visiteurs depuis Google Analytics 4 (activeUsers)</p>
          <p>Leads vendeurs depuis PostgreSQL - Comptes Estimateur V2 uniquement</p>
        </div>
      </div>
      </>
      )}

    </div>
  )
}

export default LeadsPage
