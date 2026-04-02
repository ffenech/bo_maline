import { useState, useEffect, useMemo } from 'react'
import { TrendingUp, Calendar, BarChart3, Building2, X, Search, Check, CheckCircle2 } from 'lucide-react'
import { cachedFetch, invalidateClientCache } from '../lib/fetchCache'

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

interface Client {
  id: string
  name: string
  estimateur_version: 'V1' | 'V2'
}

function LeadsTestABPage() {
  const [dailyLeads, setDailyLeads] = useState<DailyLead[]>([])
  const [dailyPhoneStats, setDailyPhoneStats] = useState<DailyPhoneStats[]>([])
  const [visitors, setVisitors] = useState<VisitorData>({})
  const [remarks, setRemarks] = useState<RemarksData>({})
  const [loading, setLoading] = useState(true)
  const [editingCell, setEditingCell] = useState<string | null>(null)
  const [clients, setClients] = useState<Client[]>([])
  const [selectedClients, setSelectedClients] = useState<Set<string>>(new Set())
  const [tempSelectedClients, setTempSelectedClients] = useState<Set<string>>(new Set())
  const [loadingClients, setLoadingClients] = useState(true)
  const [showClientSelector, setShowClientSelector] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')

  // Charger les clients
  useEffect(() => {
    const fetchClients = async () => {
      try {
        // Charger les clients disponibles avec cache
        const data = await cachedFetch<Client[]>(`${import.meta.env.VITE_API_URL || '/api'}/agencies`)
        // Filtrer uniquement les clients V2
        const v2Clients = (data || []).filter((client: Client) => client.estimateur_version === 'V2')
        setClients(v2Clients)

        // Charger les clients sélectionnés depuis la base de données
        try {
          const savedIds = await cachedFetch<string[]>(`${import.meta.env.VITE_API_URL || '/api'}/leads-test-ab/selected-clients`)
          const validIds = (savedIds || []).filter((id: string) => v2Clients.some((c: Client) => c.id === id))
          const savedSet = new Set<string>(validIds)
          setSelectedClients(savedSet)
          setTempSelectedClients(savedSet)
        } catch (error) {
          console.error('Erreur lors du chargement des clients sélectionnés:', error)
          // Par défaut, aucun client n'est sélectionné
          setSelectedClients(new Set<string>())
          setTempSelectedClients(new Set<string>())
        }
      } catch (error) {
        console.error('Erreur:', error)
      } finally {
        setLoadingClients(false)
      }
    }
    fetchClients()
  }, [])

  useEffect(() => {
    const fetchData = async () => {
      if (selectedClients.size === 0) {
        setLoading(false)
        return
      }

      try {
        // Récupérer les leads avec filtrage par clients
        const clientIds = Array.from(selectedClients)
        const leadsResponse = await fetch(
          `${import.meta.env.VITE_API_URL || '/api'}/leads/v2-daily-filtered?agencies=${clientIds.join(',')}`
        )
        if (!leadsResponse.ok) {
          throw new Error('Erreur lors de la récupération des leads')
        }
        const leadsData = await leadsResponse.json()
        // Filtrer uniquement les leads à partir du 14 septembre (inclus)
        const filteredLeads = leadsData.filter((lead: DailyLead) => {
          const dateStr = lead.date.split('T')[0] // YYYY-MM-DD
          const todayStr = new Date().toISOString().split('T')[0]
          const [, month, day] = dateStr.split('-').map(Number)
          // Garder uniquement à partir du 14 septembre
          if (month < 9) return false // Avant septembre
          if (month === 9 && day < 14) return false // Septembre mais avant le 14 (exclure 1-13)
          // Exclure toute date dans le futur par sécurité
          if (dateStr > todayStr) return false
          return true // Garder le 14 septembre et après
        })
        // Inverser l'ordre pour avoir le jour le plus récent en premier
        setDailyLeads(filteredLeads.reverse())

        // Récupérer les statistiques téléphone avec filtrage
        const phoneResponse = await fetch(
          `${import.meta.env.VITE_API_URL || '/api'}/leads/v2-daily-phone-filtered?agencies=${clientIds.join(',')}`
        )
        if (phoneResponse.ok) {
          const phoneData = await phoneResponse.json()
          // Filtrer et inverser de la même manière
          const filteredPhoneData = phoneData.filter((stat: DailyPhoneStats) => {
            const dateStr = stat.date.split('T')[0]
            const todayStr = new Date().toISOString().split('T')[0]
            const [, month, day] = dateStr.split('-').map(Number)
            if (month < 9) return false
            if (month === 9 && day < 14) return false
            if (dateStr > todayStr) return false
            return true
          })
          setDailyPhoneStats(filteredPhoneData.reverse())
        }

        // Récupérer les visiteurs GA4 filtrés par les agences des clients sélectionnés
        if (clientIds.length > 0) {
          const visitorsResponse = await fetch(
            `${import.meta.env.VITE_API_URL || '/api'}/ga4/daily-visitors-v2-filtered?clients=${clientIds.join(',')}`
          )
          if (visitorsResponse.ok) {
            const visitorsData = await visitorsResponse.json()
            const visitorsMap: VisitorData = {}
            if (Array.isArray(visitorsData)) {
              visitorsData.forEach((item: { date: string; visitors: number }) => {
                // GA4 date format: YYYY-MM-DD
                visitorsMap[item.date] = item.visitors
              })
            }
            setVisitors(visitorsMap)
            console.log('Visiteurs GA4 filtrés par agences des clients sélectionnés:', visitorsMap)
          } else {
            const errorText = await visitorsResponse.text()
            console.error('Erreur lors de la récupération des visiteurs filtrés:', visitorsResponse.status, errorText)
            // Si erreur, ne pas utiliser de fallback - laisser vide
            setVisitors({})
          }
        } else {
          // Aucun client sélectionné = aucun visiteur
          setVisitors({})
          console.log('Aucun client sélectionné, visiteurs vides')
        }

        // Récupérer les remarques depuis l'API avec cache
        try {
          const remarksData = await cachedFetch<RemarksData>(`${import.meta.env.VITE_API_URL || '/api'}/remarks`)
          setRemarks(remarksData || {})
          console.log('Remarques chargées depuis BDD:', remarksData)
        } catch (error) {
          // Fallback sur localStorage en cas d'erreur
          const savedRemarks = localStorage.getItem('leads-remarks')
          if (savedRemarks) {
            setRemarks(JSON.parse(savedRemarks))
            console.log('Remarques chargées depuis localStorage (erreur BDD)')
          }
        }
      } catch (error) {
        console.error('Erreur:', error)
      } finally {
        setLoading(false)
      }
    }

    if (!loadingClients) {
      fetchData()
    }
  }, [selectedClients, loadingClients])

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

  const toggleClient = (clientId: string) => {
    const newSelected = new Set(tempSelectedClients)
    if (newSelected.has(clientId)) {
      newSelected.delete(clientId)
    } else {
      newSelected.add(clientId)
    }
    setTempSelectedClients(newSelected)
  }

  const selectAllClients = () => {
    setTempSelectedClients(new Set(clients.map(c => c.id)))
  }

  const deselectAllClients = () => {
    setTempSelectedClients(new Set())
  }

  const applyFilter = async () => {
    const newSelection = new Set(tempSelectedClients)
    setSelectedClients(newSelection)
    setShowClientSelector(false)
    
    // Sauvegarder dans la base de données
    try {
      const clientIds = Array.from(newSelection)
      const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/leads-test-ab/selected-clients`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ clientIds }),
      })

      if (response.ok) {
        invalidateClientCache('selected-clients')
      } else {
        console.error('Erreur lors de la sauvegarde des clients sélectionnés')
      }
    } catch (error) {
      console.error('Erreur lors de la sauvegarde des clients sélectionnés:', error)
    }
  }

  const cancelFilter = () => {
    setTempSelectedClients(new Set(selectedClients))
    setShowClientSelector(false)
  }

  // Sauvegarder automatiquement quand la sélection change (hors modal)
  useEffect(() => {
    if (!loadingClients) {
      const saveSelection = async () => {
        try {
          const clientIds = Array.from(selectedClients)
          const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/leads-test-ab/selected-clients`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({ clientIds }),
          })

          if (response.ok) {
            invalidateClientCache('selected-clients')
          } else {
            console.error('Erreur lors de la sauvegarde automatique des clients sélectionnés')
          }
        } catch (error) {
          console.error('Erreur lors de la sauvegarde automatique:', error)
        }
      }

      // Délai pour éviter trop de requêtes
      const timeoutId = setTimeout(saveSelection, 500)
      return () => clearTimeout(timeoutId)
    }
  }, [selectedClients, loadingClients])

  const totalLeads = useMemo(() => {
    return dailyLeads.reduce((sum, day) => sum + day.total_leads, 0)
  }, [dailyLeads])

  const averageLeads = useMemo(() => {
    if (dailyLeads.length === 0) return 0
    return Math.round(totalLeads / dailyLeads.length)
  }, [dailyLeads, totalLeads])

  // Statistiques étape 4 (téléphone validé)
  const totalLeadsValidatedPhone = useMemo(() => {
    let total = 0
    dailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      total += phoneStat?.leads_with_validated_phone || 0
    })
    return total
  }, [dailyLeads, dailyPhoneStats])

  const averageLeadsValidatedPhone = useMemo(() => {
    if (dailyLeads.length === 0) return 0
    return Math.round(totalLeadsValidatedPhone / dailyLeads.length)
  }, [dailyLeads, totalLeadsValidatedPhone])

  const conversionStatsValidatedPhone = useMemo(() => {
    const rates: number[] = []
    dailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = visitors[dateKey] || 0
      const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      const validatedPhoneCount = phoneStat?.leads_with_validated_phone || 0
      if (visitorsCount > 0) {
        const rate = (validatedPhoneCount / visitorsCount) * 100
        rates.push(rate)
      }
    })

    if (rates.length === 0) return { average: 0, stdDev: 0 }

    const average = rates.reduce((sum, rate) => sum + rate, 0) / rates.length
    const variance = rates.reduce((sum, rate) => sum + Math.pow(rate - average, 2), 0) / rates.length
    const stdDev = Math.sqrt(variance)

    return { average, stdDev }
  }, [dailyLeads, dailyPhoneStats, visitors])

  // Calculer le taux de conversion moyen et l'écart type
  const conversionStats = useMemo(() => {
    const rates: number[] = []
    dailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = visitors[dateKey] || 0
      if (visitorsCount > 0) {
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
      if (visitorsCount > 0) {
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
      totalLeadsCount += day.total_leads
      totalWithPhone += phoneStat?.leads_with_phone || 0
    })

    return totalLeadsCount > 0 ? (totalWithPhone / totalLeadsCount) * 100 : 0
  }, [dailyLeads, dailyPhoneStats])

  // Fonction pour détecter le type d'anomalie (> 1 écart-type)
  const getAnomalyType = (rate: number): 'normal' | 'low' | 'high' => {
    const { average, stdDev } = conversionStats
    const diff = rate - average
    if (Math.abs(diff) <= stdDev) return 'normal'
    if (diff < 0) return 'low' // En dessous de la moyenne - alerte rouge
    return 'high' // Au dessus de la moyenne - positif vert
  }

  // Fonction pour détecter le type d'anomalie pour l'étape 4 (téléphone validé)
  const getAnomalyTypeValidatedPhone = (rate: number): 'normal' | 'low' | 'high' => {
    const { average, stdDev } = conversionStatsValidatedPhone
    const diff = rate - average
    if (Math.abs(diff) <= stdDev) return 'normal'
    if (diff < 0) return 'low'
    return 'high'
  }

  if (loading || loadingClients) {
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
      {/* En-tête avec sélecteur d'agences */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1">
            <h2 className="text-2xl font-bold text-gray-900">Leads Vendeur Estimateur V2 - Test A/B</h2>
            <p className="text-sm text-gray-500 mt-1">Évolution journalière depuis le 14 septembre - Filtrage par clients</p>
            <div className="mt-3 flex items-center gap-2 flex-wrap">
              <span className="inline-flex items-center gap-2 px-3 py-1.5 bg-blue-50 text-blue-700 rounded-lg text-sm font-medium border border-blue-200">
                <Building2 className="w-4 h-4" />
                {selectedClients.size > 0 
                  ? `${selectedClients.size} client${selectedClients.size > 1 ? 's' : ''} sélectionné${selectedClients.size > 1 ? 's' : ''}`
                  : 'Aucun client sélectionné'}
              </span>
              {selectedClients.size > 0 && selectedClients.size < clients.length && (
                <span className="text-xs text-gray-500">
                  ({clients.length - selectedClients.size} client{clients.length - selectedClients.size > 1 ? 's' : ''} exclu{clients.length - selectedClients.size > 1 ? 's' : ''})
                </span>
              )}
            </div>
          </div>
          <div className="relative">
            <button
              onClick={() => {
                setTempSelectedClients(new Set(selectedClients))
                setShowClientSelector(!showClientSelector)
              }}
              className="flex items-center gap-2 px-4 py-2.5 bg-gradient-to-r from-blue-600 to-cyan-600 text-white rounded-lg hover:from-blue-700 hover:to-cyan-700 transition-all shadow-md hover:shadow-lg"
            >
              <Building2 className="w-5 h-5" />
              <span className="font-medium">Filtrer par clients</span>
            </button>
            
            {showClientSelector && (
              <>
                {/* Backdrop avec animation */}
                <div
                  className="fixed inset-0 z-50 bg-black bg-opacity-50 backdrop-blur-sm transition-opacity duration-300"
                  onClick={cancelFilter}
                />
                
                {/* Modale centrée avec animation */}
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none">
                  <div 
                    className="bg-white rounded-2xl shadow-2xl border border-gray-200 w-full max-w-2xl max-h-[85vh] flex flex-col pointer-events-auto transform transition-all duration-300 scale-100"
                    onClick={(e) => e.stopPropagation()}
                  >
                    {/* En-tête avec gradient */}
                    <div className="relative p-6 border-b border-gray-200 bg-gradient-to-br from-blue-600 via-blue-500 to-cyan-500 rounded-t-2xl">
                      <div className="flex items-start justify-between mb-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-3 mb-2">
                            <div className="w-12 h-12 rounded-xl bg-white/20 backdrop-blur-sm flex items-center justify-center">
                              <Building2 className="w-6 h-6 text-white" />
                            </div>
                            <div>
                              <h3 className="text-2xl font-bold text-white">Sélectionner les clients</h3>
                              <p className="text-sm text-blue-100 mt-0.5">Choisissez les clients à inclure dans les statistiques</p>
                            </div>
                          </div>
                        </div>
                        <button
                          onClick={cancelFilter}
                          className="text-white/80 hover:text-white transition-colors p-2 hover:bg-white/20 rounded-lg backdrop-blur-sm"
                        >
                          <X className="w-5 h-5" />
                        </button>
                      </div>
                      
                      {/* Barre de recherche */}
                      <div className="relative">
                        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-white/70" />
                        <input
                          type="text"
                          placeholder="Rechercher un client..."
                          value={searchTerm}
                          onChange={(e) => setSearchTerm(e.target.value)}
                          className="w-full pl-10 pr-4 py-2.5 bg-white/20 backdrop-blur-sm border border-white/30 rounded-lg text-white placeholder-white/60 focus:outline-none focus:ring-2 focus:ring-white/50 focus:border-white/50 transition-all"
                        />
                      </div>
                      
                      {/* Actions rapides */}
                      <div className="flex items-center justify-between mt-4">
                        <div className="flex gap-2">
                          <button
                            onClick={selectAllClients}
                            className="px-4 py-2 text-sm font-medium text-white bg-white/20 backdrop-blur-sm rounded-lg hover:bg-white/30 transition-all border border-white/30"
                          >
                            Tout sélectionner
                          </button>
                          <button
                            onClick={deselectAllClients}
                            className="px-4 py-2 text-sm font-medium text-white bg-white/20 backdrop-blur-sm rounded-lg hover:bg-white/30 transition-all border border-white/30"
                          >
                            Tout désélectionner
                          </button>
                        </div>
                        <div className="flex items-center gap-2 px-4 py-2 bg-white/20 backdrop-blur-sm rounded-lg border border-white/30">
                          <CheckCircle2 className="w-4 h-4 text-white" />
                          <span className="text-sm font-semibold text-white">
                            {tempSelectedClients.size} / {clients.length}
                          </span>
                        </div>
                      </div>
                    </div>
                    
                    {/* Liste des clients avec scroll */}
                    <div className="flex-1 overflow-y-auto p-4 bg-gray-50">
                      <div className="space-y-2">
                        {clients
                          .filter(client => 
                            client.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                            client.id.toLowerCase().includes(searchTerm.toLowerCase())
                          )
                          .map((client) => (
                            <label
                              key={client.id}
                              className={`group flex items-center gap-4 p-4 rounded-xl cursor-pointer transition-all duration-200 ${
                                tempSelectedClients.has(client.id)
                                  ? 'bg-gradient-to-r from-blue-50 to-cyan-50 border-2 border-blue-300 shadow-md'
                                  : 'bg-white border-2 border-gray-200 hover:border-blue-200 hover:shadow-sm'
                              }`}
                            >
                              <div className="flex-shrink-0">
                                {tempSelectedClients.has(client.id) ? (
                                  <div className="w-6 h-6 bg-gradient-to-br from-blue-600 to-cyan-600 rounded-lg flex items-center justify-center shadow-lg transform scale-100 group-hover:scale-110 transition-transform">
                                    <Check className="w-4 h-4 text-white" />
                                  </div>
                                ) : (
                                  <div className="w-6 h-6 bg-white rounded-lg border-2 border-gray-300 group-hover:border-blue-400 transition-colors flex items-center justify-center">
                                    <div className="w-2 h-2 rounded-full bg-transparent group-hover:bg-blue-200 transition-colors" />
                                  </div>
                                )}
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 mb-1">
                                  <span className={`text-base font-semibold ${
                                    tempSelectedClients.has(client.id) ? 'text-blue-900' : 'text-gray-900'
                                  }`}>
                                    {client.name}
                                  </span>
                                  {tempSelectedClients.has(client.id) && (
                                    <span className="px-2 py-0.5 bg-blue-100 text-blue-700 text-xs font-medium rounded-full">
                                      Sélectionné
                                    </span>
                                  )}
                                </div>
                                <span className="text-xs text-gray-500 font-mono">ID: {client.id}</span>
                              </div>
                              <input
                                type="checkbox"
                                checked={tempSelectedClients.has(client.id)}
                                onChange={() => toggleClient(client.id)}
                                className="sr-only"
                              />
                            </label>
                          ))}
                        
                        {clients.filter(client => 
                          client.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                          client.id.toLowerCase().includes(searchTerm.toLowerCase())
                        ).length === 0 && (
                          <div className="text-center py-12">
                            <Search className="w-12 h-12 text-gray-300 mx-auto mb-3" />
                            <p className="text-gray-500 font-medium">Aucun client trouvé</p>
                            <p className="text-sm text-gray-400 mt-1">Essayez avec un autre terme de recherche</p>
                          </div>
                        )}
                      </div>
                    </div>
                    
                    {/* Footer avec actions */}
                    <div className="p-5 border-t border-gray-200 bg-white rounded-b-2xl flex items-center justify-between">
                      <div className="text-sm text-gray-600">
                        {tempSelectedClients.size === 0 ? (
                          <span className="text-orange-600 font-medium">⚠️ Sélectionnez au moins un client</span>
                        ) : (
                          <span className="text-green-600 font-medium">
                            ✓ {tempSelectedClients.size} client{tempSelectedClients.size > 1 ? 's' : ''} sélectionné{tempSelectedClients.size > 1 ? 's' : ''}
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-3">
                        <button
                          onClick={cancelFilter}
                          className="px-5 py-2.5 text-sm font-medium text-gray-700 bg-gray-100 rounded-xl hover:bg-gray-200 transition-all"
                        >
                          Annuler
                        </button>
                        <button
                          onClick={applyFilter}
                          disabled={tempSelectedClients.size === 0}
                          className="px-6 py-2.5 text-sm font-semibold text-white bg-gradient-to-r from-blue-600 to-cyan-600 rounded-xl hover:from-blue-700 hover:to-cyan-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg hover:shadow-xl transform hover:scale-105 disabled:hover:scale-100"
                        >
                          Appliquer le filtre
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      {selectedClients.size === 0 ? (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0">
              <Building2 className="w-8 h-8 text-blue-600" />
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-blue-900 mb-2">Aucun client sélectionné</h3>
              <p className="text-blue-800 text-sm mb-4">
                Veuillez sélectionner au moins un client pour afficher les statistiques. Vos sélections seront sauvegardées automatiquement.
              </p>
              <button
                onClick={() => {
                  setTempSelectedClients(new Set(selectedClients))
                  setShowClientSelector(true)
                }}
                className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium"
              >
                <Building2 className="w-5 h-5" />
                Sélectionner des clients
              </button>
            </div>
          </div>
        </div>
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
                            Aucun lead trouvé pour les agences sélectionnées depuis le 14 septembre
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
                          {/* Ligne Taux de conversion */}
                          <tr className="hover:bg-gray-50 transition-colors bg-purple-50">
                            <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-purple-50 z-10">
                              Taux de conversion
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
              <p>✓ Leads vendeurs depuis PostgreSQL - Clients sélectionnés uniquement</p>
            </div>
          </div>

          {/* Statistiques Étape 4 */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
              <div className="flex items-center">
                <div className="p-2 bg-blue-100 rounded-lg">
                  <TrendingUp className="w-5 h-5 text-blue-600" />
                </div>
                <div className="ml-3">
                  <p className="text-xs font-medium text-gray-600">Total Leads étape 4 vendeurs</p>
                  <p className="text-xl font-bold text-gray-900">{totalLeadsValidatedPhone.toLocaleString('fr-FR')}</p>
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
              <div className="flex items-center">
                <div className="p-2 bg-green-100 rounded-lg">
                  <BarChart3 className="w-5 h-5 text-green-600" />
                </div>
                <div className="ml-3">
                  <p className="text-xs font-medium text-gray-600">Moyenne leads étape 4 vendeur / jour</p>
                  <p className="text-xl font-bold text-gray-900">{averageLeadsValidatedPhone}</p>
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
                  <p className="text-xs font-medium text-gray-600">Taux conversion étape 4 vendeurs</p>
                  <p className="text-xl font-bold text-gray-900">{conversionStatsValidatedPhone.average.toFixed(2)}%</p>
                  <p className="text-xs text-gray-500 mt-0.5">±{conversionStatsValidatedPhone.stdDev.toFixed(2)}%</p>
                </div>
              </div>
            </div>
          </div>

          {/* Tableau 2 : Statistiques téléphone */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">Qualité des numéros de téléphone</h3>
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
                            Aucune donnée disponible
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
                                  <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                    {phoneStat?.leads_with_phone || 0}
                                  </span>
                                </td>
                              )
                            })}
                          </tr>
                          {/* Ligne Leads avec tel validé */}
                          <tr className="hover:bg-gray-50 transition-colors">
                            <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                              Leads avec tel validé
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
                          {/* Ligne Taux de conversion tel validé */}
                          <tr className="hover:bg-gray-50 transition-colors bg-purple-50">
                            <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-purple-50 z-10">
                              Taux de conversion (tel validé)
                            </td>
                            {dailyLeads.map((day, index) => {
                              const dateKey = day.date.split('T')[0]
                              const visitorsCount = visitors[dateKey] || 0
                              const phoneStat = dailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                              const validatedPhoneCount = phoneStat?.leads_with_validated_phone || 0
                              const rate = visitorsCount > 0
                                ? (validatedPhoneCount / visitorsCount) * 100
                                : 0
                              const conversionRate = rate.toFixed(2)
                              const anomalyType = getAnomalyTypeValidatedPhone(rate)

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
                        </>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>

          {/* Légende du tableau 2 */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-4">📊 Légende & Détection d'anomalies (Étape 4)</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="flex items-start space-x-3">
                <div className="flex-shrink-0 mt-1">
                  <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-purple-100 text-purple-800">
                    {conversionStatsValidatedPhone.average.toFixed(2)}%
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
                    {Math.max(0, conversionStatsValidatedPhone.average - conversionStatsValidatedPhone.stdDev).toFixed(2)}%
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
                    {(conversionStatsValidatedPhone.average + conversionStatsValidatedPhone.stdDev).toFixed(2)}%
                  </span>
                </div>
                <div>
                  <p className="text-sm font-medium text-green-900">✅ Anomalie haute</p>
                  <p className="text-xs text-gray-500">Taux anormalement haut - Performance exceptionnelle</p>
                </div>
              </div>
            </div>
            <div className="mt-4 pt-4 border-t border-gray-200 text-xs text-gray-500">
              <p>✓ Taux de conversion basé sur les téléphones validés uniquement</p>
              <p>✓ Détection automatique des anomalies basée sur l'écart-type</p>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

export default LeadsTestABPage

