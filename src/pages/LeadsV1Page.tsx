import { useState, useEffect, useMemo } from 'react'
import { TrendingUp, Calendar, BarChart3 } from 'lucide-react'
import { cachedFetch } from '../lib/fetchCache'

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

function LeadsV1Page() {
  const [dailyLeads, setDailyLeads] = useState<DailyLead[]>([])
  const [dailyPhoneStats, setDailyPhoneStats] = useState<DailyPhoneStats[]>([])
  const [visitors, setVisitors] = useState<VisitorData>({})
  const [remarks, setRemarks] = useState<RemarksData>({})
  const [loading, setLoading] = useState(true)
  const [editingCell, setEditingCell] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      const apiUrl = import.meta.env.VITE_API_URL || '/api'
      const todayStr = new Date().toISOString().split('T')[0]

      // Fonction de filtrage des dates (à partir du 14 septembre 2024)
      const filterAndSort = <T extends { date: string }>(data: T[]): T[] => {
        const startDate = '2024-09-14'
        return data
          .filter((item) => {
            const dateStr = item.date.split('T')[0]
            if (dateStr < startDate) return false
            if (dateStr > todayStr) return false
            return true
          })
          .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())
      }

      try {
        // Lancer toutes les requêtes en parallèle avec cache
        const [leadsData, phoneData, visitorsData, remarksData] = await Promise.all([
          cachedFetch<DailyLead[]>(`${apiUrl}/leads/v1-daily`),
          cachedFetch<DailyPhoneStats[]>(`${apiUrl}/leads/v1-daily-phone`),
          cachedFetch<{ date: string; visitors: number }[]>(`${apiUrl}/ga4/daily-visitors-v1`),
          cachedFetch<RemarksData>(`${apiUrl}/remarks`)
        ])

        // Traiter les leads
        setDailyLeads(filterAndSort(leadsData || []))

        // Traiter les statistiques téléphone
        setDailyPhoneStats(filterAndSort(phoneData || []))

        // Traiter les visiteurs GA4
        const visitorsMap: VisitorData = {}
        if (Array.isArray(visitorsData)) {
          visitorsData.forEach((item) => {
            visitorsMap[item.date] = item.visitors
          })
        }
        setVisitors(visitorsMap)

        // Traiter les remarques
        setRemarks(remarksData || {})
      } catch (error) {
        console.error('Erreur:', error)
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
        console.log('✅ Remarque sauvegardée en BDD:', date, value)
      } else {
        // Fallback sur localStorage si la table n'existe pas
        setRemarks(updatedRemarks)
        localStorage.setItem('leads-remarks', JSON.stringify(updatedRemarks))
        console.log('💾 Remarque sauvegardée dans localStorage (table BDD non créée):', date, value)
      }
    } catch (error) {
      // Fallback sur localStorage en cas d'erreur
      setRemarks(updatedRemarks)
      localStorage.setItem('leads-remarks', JSON.stringify(updatedRemarks))
      console.log('💾 Remarque sauvegardée dans localStorage (erreur BDD):', date, value)
    }
    setEditingCell(null)
  }

  const totalLeads = useMemo(() => {
    return dailyLeads.reduce((sum, day) => sum + Number(day.total_leads), 0)
  }, [dailyLeads])

  const averageLeads = useMemo(() => {
    if (dailyLeads.length === 0) return 0
    return Math.round(totalLeads / dailyLeads.length)
  }, [dailyLeads, totalLeads])

  // Seuil minimum de visiteurs pour exclure les jours où GA4 n'avait pas assez de données
  const MIN_VISITORS = 50

  // Calculer le taux de conversion moyen et l'écart type
  const conversionStats = useMemo(() => {
    const rates: number[] = []
    dailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = visitors[dateKey] || 0
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
  }, [dailyLeads, visitors])

  // Calculer le taux de conversion sur les 10 derniers jours
  const conversionStatsLast10Days = useMemo(() => {
    const last10Days = dailyLeads.slice(0, 10)
    const rates: number[] = []
    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = visitors[dateKey] || 0
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
  }, [dailyLeads, visitors])

  // Calculer le % tel sur les 10 derniers jours
  const phonePercentLast10Days = useMemo(() => {
    const last10Days = dailyLeads.slice(0, 10)
    let totalLeadsCount = 0
    let totalWithPhone = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalLeadsCount += Number(day.total_leads)
      totalWithPhone += phoneStat?.leads_with_phone || 0
    })

    return totalLeadsCount > 0 ? (totalWithPhone / totalLeadsCount) * 100 : 0
  }, [dailyLeads, dailyPhoneStats])

  // Taux conversion vendeur avec tel (10 derniers jours)
  const conversionPhoneLast10Days = useMemo(() => {
    const last10Days = dailyLeads.slice(0, 10)
    let totalWithPhone = 0
    let totalVisitors = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = visitors[dateKey] || 0
      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalWithPhone += phoneStat?.leads_with_phone || 0
      totalVisitors += visitorsCount
    })

    return totalVisitors > 0 ? (totalWithPhone / totalVisitors) * 100 : 0
  }, [dailyLeads, dailyPhoneStats, visitors])

  // Taux conversion vendeur avec tel validés (10 derniers jours)
  const conversionValidatedPhoneLast10Days = useMemo(() => {
    const last10Days = dailyLeads.slice(0, 10)
    let totalValidated = 0
    let totalVisitors = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = visitors[dateKey] || 0
      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalValidated += phoneStat?.leads_with_validated_phone || 0
      totalVisitors += visitorsCount
    })

    return totalVisitors > 0 ? (totalValidated / totalVisitors) * 100 : 0
  }, [dailyLeads, dailyPhoneStats, visitors])

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
      {/* En-tête */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">Leads Vendeur Estimateur V1</h2>
            <p className="text-sm text-gray-500 mt-1">Évolution journalière du 14 septembre à aujourd'hui</p>
          </div>
        </div>
      </div>

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
              <p className="text-xl font-bold text-gray-900">{dailyLeads.length}</p>
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
          <h3 className="text-lg font-semibold text-gray-900">Détail journalier</h3>
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
                {dailyLeads.map((day, index) => (
                  <th key={index} className="px-2 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[80px]">
                    {formatDateShort(day.date)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {dailyLeads.length === 0 ? (
                <tr>
                  <td colSpan={dailyLeads.length + 1} className="px-6 py-8 text-center text-gray-500">
                    Aucun lead trouvé pour les comptes Estimateur V2 depuis le 1er septembre
                  </td>
                </tr>
              ) : (
                <>
                  {/* Ligne Nombre visiteurs */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Nombre visiteurs
                    </td>
                    {dailyLeads.map((day, index) => {
                      // Normaliser la date de la BDD (ISO) en format YYYY-MM-DD
                      const dateKey = day.date.split('T')[0]
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            {visitors[dateKey] || 0}
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
                    {dailyLeads.map((day, index) => (
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
                    {dailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const visitorsCount = visitors[dateKey] || 0
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
                    {dailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
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
                    {dailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
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
                    {dailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const visitorsCount = visitors[dateKey] || 0
                      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
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
                    {dailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
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
                    {dailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
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
                    {dailyLeads.map((day, index) => {
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
                                        🗑️ Supprimer
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
                                    💬 Note
                                  </span>
                                </div>
                                {/* Tooltip au survol */}
                                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 whitespace-normal w-64 max-w-xs">
                                  <div className="font-semibold mb-1 text-blue-300">📝 Note du {formatDateShort(day.date)}</div>
                                  <div className="text-gray-100">{remarks[dateKey]}</div>
                                  {/* Flèche */}
                                  <div className="absolute top-full left-1/2 transform -translate-x-1/2 -mt-1">
                                    <div className="border-8 border-transparent border-t-gray-900"></div>
                                  </div>
                                </div>
                              </>
                            ) : (
                              <span className="text-gray-400 hover:text-gray-600">📝</span>
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

      {/* Légende */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-sm font-semibold text-gray-900 mb-4">📊 Légende & Détection d'anomalies</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-purple-100 text-purple-800">
                {conversionStats.average.toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-gray-900">Taux normal</p>
              <p className="text-xs text-gray-500">Valeur dans la plage habituelle (moyenne ± 1 écart-type)</p>
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
              <p className="text-sm font-medium text-red-900">⚠️ Anomalie basse</p>
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
              <p className="text-sm font-medium text-green-900">✅ Anomalie haute</p>
              <p className="text-xs text-gray-500">Taux anormalement haut - Performance exceptionnelle</p>
            </div>
          </div>
        </div>
        <div className="mt-4 pt-4 border-t border-gray-200 text-xs text-gray-500">
          <p>✓ Données visiteurs depuis Google Analytics 4 (activeUsers)</p>
          <p>✓ Leads vendeurs depuis PostgreSQL - Comptes Estimateur V1 uniquement</p>
          <p>✓ Période : du 14 septembre {new Date().getFullYear()} à aujourd'hui ({dailyLeads.length} jours)</p>
        </div>
      </div>

    </div>
  )
}

export default LeadsV1Page
