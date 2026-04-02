import { useState, useEffect, useMemo } from 'react'
import { Building2, Users, Home, ArrowUpDown, Search, Filter } from 'lucide-react'
import { cachedFetch } from '../lib/fetchCache'

interface Agency {
  id: string
  name: string
  agency_count: number
  agent_count: number
  property_count: number
  seller_leads_count: number
  estimateur_version: 'V1' | 'V2'
}

type SortField = 'id' | 'name' | 'estimateur_version' | 'agency_count' | 'agent_count' | 'property_count' | 'seller_leads_count'
type SortOrder = 'asc' | 'desc'

function AgenciesPage() {
  const [agencies, setAgencies] = useState<Agency[]>([])
  const [loading, setLoading] = useState(true)
  const [sortField, setSortField] = useState<SortField>('id')
  const [sortOrder, setSortOrder] = useState<SortOrder>('asc')
  const [searchTerm, setSearchTerm] = useState('')
  const [filterEstimateur, setFilterEstimateur] = useState<'all' | 'V1' | 'V2'>('all')

  useEffect(() => {
    // Récupérer les vraies données depuis l'API avec cache
    const fetchAgencies = async () => {
      try {
        const data = await cachedFetch<Agency[]>(`${import.meta.env.VITE_API_URL || '/api'}/agencies`)
        setAgencies(data || [])
      } catch (error) {
        console.error('Erreur:', error)
      } finally {
        setLoading(false)
      }
    }

    fetchAgencies()
  }, [])

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortOrder('asc')
    }
  }

  const filteredAndSortedAgencies = useMemo(() => {
    let filtered = [...agencies]

    // Filtre par recherche
    if (searchTerm) {
      filtered = filtered.filter(agency =>
        agency.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        agency.id.toLowerCase().includes(searchTerm.toLowerCase())
      )
    }

    // Filtre par estimateur
    if (filterEstimateur !== 'all') {
      filtered = filtered.filter(agency => agency.estimateur_version === filterEstimateur)
    }

    // Tri
    filtered.sort((a, b) => {
      let comparison = 0

      switch (sortField) {
        case 'id':
          comparison = a.id.localeCompare(b.id)
          break
        case 'name':
          comparison = a.name.localeCompare(b.name)
          break
        case 'estimateur_version':
          comparison = a.estimateur_version.localeCompare(b.estimateur_version)
          break
        case 'agency_count':
          comparison = a.agency_count - b.agency_count
          break
        case 'agent_count':
          comparison = a.agent_count - b.agent_count
          break
        case 'property_count':
          comparison = a.property_count - b.property_count
          break
        case 'seller_leads_count':
          comparison = a.seller_leads_count - b.seller_leads_count
          break
      }

      return sortOrder === 'asc' ? comparison : -comparison
    })

    return filtered
  }, [agencies, searchTerm, filterEstimateur, sortField, sortOrder])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="text-gray-600 mt-4">Chargement des agences...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* En-tête avec statistiques */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                <Building2 className="w-6 h-6 text-blue-600" />
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Clients</p>
              <p className="text-2xl font-bold text-gray-900">{agencies.length}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-indigo-100 rounded-lg flex items-center justify-center">
                <Building2 className="w-6 h-6 text-indigo-600" />
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Agences</p>
              <p className="text-2xl font-bold text-gray-900">
                {agencies.reduce((sum, a) => sum + a.agency_count, 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center">
                <Users className="w-6 h-6 text-green-600" />
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Agents</p>
              <p className="text-2xl font-bold text-gray-900">
                {agencies.reduce((sum, a) => sum + a.agent_count, 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-purple-100 rounded-lg flex items-center justify-center">
                <Home className="w-6 h-6 text-purple-600" />
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total leads</p>
              <p className="text-2xl font-bold text-gray-900">
                {agencies.reduce((sum, a) => sum + a.property_count, 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-amber-100 rounded-lg flex items-center justify-center">
                <Home className="w-6 h-6 text-amber-600" />
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Leads vendeurs</p>
              <p className="text-2xl font-bold text-gray-900">
                {agencies.reduce((sum, a) => sum + a.seller_leads_count, 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-cyan-100 rounded-lg flex items-center justify-center">
                <Building2 className="w-6 h-6 text-cyan-600" />
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Comptes en V2</p>
              <p className="text-2xl font-bold text-gray-900">
                {agencies.filter((a) => a.estimateur_version === 'V2').length}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Tableau des clients */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Liste des clients</h3>
              <p className="text-sm text-gray-500 mt-1">Tous les clients de la base de données V3</p>
            </div>
          </div>

          {/* Filtres */}
          <div className="flex flex-wrap gap-4">
            {/* Recherche */}
            <div className="flex-1 min-w-64">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
                <input
                  type="text"
                  placeholder="Rechercher par nom ou ID client..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
            </div>

            {/* Filtre Estimateur */}
            <div className="flex items-center gap-2">
              <Filter className="h-4 w-4 text-gray-400" />
              <select
                value={filterEstimateur}
                onChange={(e) => setFilterEstimateur(e.target.value as 'all' | 'V1' | 'V2')}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="all">Tous les estimateurs</option>
                <option value="V1">V1 uniquement</option>
                <option value="V2">V2 uniquement</option>
              </select>
            </div>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th
                  onClick={() => handleSort('id')}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    ID
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
                <th
                  onClick={() => handleSort('name')}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    Client
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
                <th
                  onClick={() => handleSort('agency_count')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center justify-center gap-2">
                    Agences
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
                <th
                  onClick={() => handleSort('estimateur_version')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center justify-center gap-2">
                    Estimateur
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
                <th
                  onClick={() => handleSort('agent_count')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center justify-center gap-2">
                    Agents
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
                <th
                  onClick={() => handleSort('property_count')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center justify-center gap-2">
                    Leads
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
                <th
                  onClick={() => handleSort('seller_leads_count')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center justify-center gap-2">
                    Leads vendeurs
                    <ArrowUpDown className="h-4 w-4" />
                  </div>
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredAndSortedAgencies.map((agency) => (
                <tr key={agency.id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-mono text-gray-600">{agency.id}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="flex-shrink-0 h-10 w-10 bg-blue-100 rounded-lg flex items-center justify-center">
                        <Building2 className="h-5 w-5 text-blue-600" />
                      </div>
                      <div className="ml-4">
                        <div className="text-sm font-medium text-gray-900">{agency.name}</div>
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-center">
                    <div className="flex items-center justify-center">
                      <Building2 className="h-4 w-4 text-gray-400 mr-1" />
                      <span className="text-sm font-semibold text-gray-900">{agency.agency_count}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-center">
                    <span className={`inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold ${
                      agency.estimateur_version === 'V2'
                        ? 'bg-green-100 text-green-800'
                        : 'bg-blue-100 text-blue-800'
                    }`}>
                      {agency.estimateur_version}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-center">
                    <div className="flex items-center justify-center">
                      <Users className="h-4 w-4 text-gray-400 mr-1" />
                      <span className="text-sm font-semibold text-gray-900">{agency.agent_count}</span>
                    </div>
                  </td>
                <td className="px-6 py-4 whitespace-nowrap text-center">
                  <div className="flex items-center justify-center">
                    <Home className="h-4 w-4 text-gray-400 mr-1" />
                    <span className="text-sm font-semibold text-gray-900">{agency.property_count}</span>
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-center">
                  <div className="flex items-center justify-center">
                    <Home className="h-4 w-4 text-gray-400 mr-1" />
                    <span className="text-sm font-semibold text-gray-900">{agency.seller_leads_count}</span>
                  </div>
                </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="px-6 py-4 bg-gray-50 border-t border-gray-200">
          <div className="flex items-center justify-between">
            <p className="text-sm text-gray-600">
              Affichage de <span className="font-medium">{filteredAndSortedAgencies.length}</span> client(s) sur {agencies.length}
            </p>
            <p className="text-xs text-green-600 font-medium">
              ✓ Données en temps réel depuis PostgreSQL
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

export default AgenciesPage
