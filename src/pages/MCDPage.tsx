import { useEffect, useState, lazy, Suspense } from 'react'
import { Database, Calendar, DollarSign } from 'lucide-react'

// Lazy load de mermaid uniquement quand nécessaire
const MermaidDiagram = lazy(() => import('../components/MermaidDiagram'))

interface EstimateurAgency {
  id: string
  idAgence: string
  name: string
  createdAt: string
  externalId: number
  idClient: string
  codesPostaux: {
    codePostal: string
    tarif: number
  }[]
}

interface V2Agency {
  nom: string
  identifier: string
  date_start: string | null
  date_fin: string | null
  malinev3: number
  tarifs: {
    code_postal: string
    tarif: string
  }[]
}

function V2AgenciesTable() {
  const [agencies, setAgencies] = useState<V2Agency[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const fetchV2Data = async () => {
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/v2/agencies`)

        if (!response.ok) {
          throw new Error('Erreur lors de la récupération des données V2')
        }

        const data = await response.json()
        if (Array.isArray(data)) {
          setAgencies(data)
        }
      } catch (error) {
        console.error('Erreur:', error)
      } finally {
        setLoading(false)
      }
    }

    fetchV2Data()
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="text-gray-600 mt-4">Chargement des agences V2...</p>
        </div>
      </div>
    )
  }

  if (agencies.length === 0) {
    return (
      <div className="text-center py-16 text-gray-500">
        Aucune agence trouvée dans l'API V2
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="bg-green-50 border border-green-200 rounded-lg p-4">
        <h4 className="font-semibold text-green-900 mb-2">🔗 API Territory V2</h4>
        <p className="text-sm text-green-800">
          <strong>Endpoint:</strong> <code className="bg-green-100 px-2 py-0.5 rounded">https://back-api.maline-immobilier.fr/territory/api/agences</code>
        </p>
      </div>

      <div className="mb-4 text-sm text-gray-600">
        <Database className="inline w-4 h-4 mr-2" />
        {agencies.length} agence(s) dans la BDD V2
      </div>

      <div className="border border-gray-200 rounded-lg overflow-hidden mb-6">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Nom
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Identifier
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Date début
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Date fin
              </th>
              <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Maline V3
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Codes postaux
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Total Tarif (€)
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {agencies.map((agency, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-4 py-3 text-sm font-medium text-gray-900">
                  {agency.nom}
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  <code className="bg-gray-100 px-2 py-0.5 rounded text-xs">{agency.identifier}</code>
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  <div className="flex items-center">
                    <Calendar className="w-4 h-4 text-gray-400 mr-2" />
                    {agency.date_start ? new Date(agency.date_start).toLocaleDateString('fr-FR') : '-'}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  <div className="flex items-center">
                    <Calendar className="w-4 h-4 text-gray-400 mr-2" />
                    {agency.date_fin ? new Date(agency.date_fin).toLocaleDateString('fr-FR') : '-'}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-center">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                    agency.malinev3 === 1
                      ? 'bg-green-100 text-green-800'
                      : 'bg-gray-100 text-gray-800'
                  }`}>
                    {agency.malinev3 === 1 ? '✓ Oui' : '✗ Non'}
                  </span>
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  <div className="flex flex-wrap gap-1">
                    {agency.tarifs.map((tarif, tIndex) => (
                      <span
                        key={tIndex}
                        className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700"
                      >
                        {tarif.code_postal}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-gray-900 text-right font-semibold">
                  <div className="flex items-center justify-end">
                    <DollarSign className="w-4 h-4 text-green-600 mr-1" />
                    {agency.tarifs.reduce((sum, t) => sum + parseFloat(t.tarif), 0).toLocaleString('fr-FR', { minimumFractionDigits: 2 })}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Exemple de structure de réponse API */}
      <div className="bg-gray-50 rounded-lg p-4 border border-gray-200 overflow-auto">
        <h4 className="font-semibold text-gray-900 mb-2">Exemple de réponse API</h4>
        <pre className="text-xs text-gray-800 whitespace-pre-wrap">{JSON.stringify(agencies.slice(0, 2), null, 2)}</pre>
      </div>
    </div>
  )
}

function EstimateurTable() {
  const [agencies, setAgencies] = useState<EstimateurAgency[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchEstimateurData = async () => {
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/estimateur/agencies`)

        if (!response.ok) {
          setError(`API Estimateur indisponible (${response.status})`)
          setAgencies([])
          return
        }

        const result = await response.json()
        if (result.success && Array.isArray(result.data)) {
          setAgencies(result.data)
        }
      } catch (error) {
        console.error('Erreur:', error)
        setError(error instanceof Error ? error.message : 'Erreur inconnue')
      } finally {
        setLoading(false)
      }
    }

    fetchEstimateurData()
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="text-gray-600 mt-4">Chargement des agences Estimateur...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-center py-16">
        <div className="inline-flex items-center px-3 py-2 rounded-lg bg-red-50 border border-red-200 text-red-700">
          <span className="font-medium mr-2">Indisponible</span>
          <span className="text-sm">{error}</span>
        </div>
        <p className="text-gray-500 mt-2 text-sm">Le fournisseur Estimateur rencontre une erreur. Réessayez plus tard.</p>
      </div>
    )
  }

  if (agencies.length === 0) {
    return (
      <div className="text-center py-16 text-gray-500">
        Aucune agence trouvée dans l'API Estimateur
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="font-semibold text-blue-900 mb-2">📋 API Estimateur V2</h4>
        <p className="text-sm text-blue-800">
          <strong>Note:</strong> <code className="bg-blue-100 px-2 py-0.5 rounded">idClient</code> correspond à l'ID du client dans la BDD V3
        </p>
      </div>

      <div className="mb-4 text-sm text-gray-600">
        <Database className="inline w-4 h-4 mr-2" />
        {agencies.length} agence(s) utilisant l'Estimateur V2
      </div>

      <div className="border border-gray-200 rounded-lg overflow-hidden mb-6">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Nom
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Date de création
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Codes postaux
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Tarif (€)
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {agencies.map((agency) => (
              <tr key={agency.id} className="hover:bg-gray-50">
                <td className="px-4 py-3 text-sm font-medium text-gray-900">
                  {agency.name}
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  <div className="flex items-center">
                    <Calendar className="w-4 h-4 text-gray-400 mr-2" />
                    {new Date(agency.createdAt).toLocaleDateString('fr-FR', {
                      year: 'numeric',
                      month: 'short',
                      day: 'numeric'
                    })}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  <div className="flex flex-wrap gap-1">
                    {agency.codesPostaux.map((cp, index) => (
                      <span
                        key={index}
                        className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700"
                      >
                        {cp.codePostal}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-gray-900 text-right font-semibold">
                  <div className="flex items-center justify-end">
                    <DollarSign className="w-4 h-4 text-green-600 mr-1" />
                    {agency.codesPostaux.reduce((sum, cp) => sum + cp.tarif, 0).toLocaleString('fr-FR')}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Exemple de structure de réponse API */}
      <div className="bg-gray-50 rounded-lg p-4 border border-gray-200 overflow-auto">
        <h4 className="font-semibold text-gray-900 mb-2">Exemple de réponse API</h4>
        <pre className="text-xs text-gray-800 whitespace-pre-wrap">{`{
  "success": true,
  "message": "4 agence(s) récupérée(s) avec succès",
  "data": [
    {
      "id": "cmfy56mo20000rqtb2vm2akyn",
      "idAgence": "761681989",
      "name": "Cabinet Bonnenfant",
      "createdAt": "2025-09-24T15:31:42.530Z",
      "externalId": 200,
      "idClient": "d150fe5a-a2be-11ef-bf84-8b33a0818887", // ← ID du client V3
      "codesPostaux": [
        {
          "codePostal": "78100,78230",
          "tarif": 605
        }
      ]
    },
    {
      "id": "cmfy56nua0012rqtblg9jupzk",
      "idAgence": "5819739917",
      "name": "ERA Saint-Denis",
      "createdAt": "2025-09-24T15:31:44.051Z",
      "externalId": 1043,
      "idClient": "eab3f842-a12a-11ef-a7ca-49e48877f181", // ← ID du client V3
      "codesPostaux": [
        {
          "codePostal": "92700,95100,93300,93200,93400,92500",
          "tarif": 2505
        }
      ]
    }
  ]
}`}</pre>
      </div>
    </div>
  )
}

// Diagramme Mermaid complet représentant le schéma de la BDD v3
const bddV3Schema = `
erDiagram
    client ||--o{ agency : "possède"
    client ||--o{ agent : "a"
    client ||--o{ property : "gère"
    agency ||--o{ agent : "emploie"
    agency ||--o{ property : "contient"
    agent ||--o{ property : "gère"

    client {
        uuid id_client PK
        varchar id_v2
        json id_gocardless
        varchar zoho_client_id
        varchar name
        json sector_postal_codes
        boolean demo
        json default_reassignment_rule
        varchar locale
        json automatic_follow_type_actives
    }

    agency {
        uuid id PK
        uuid id_client FK
        varchar name
        varchar address
        varchar postal_code
        varchar city
        varchar phone
        varchar email
        varchar assignation_type
        json sector_postal_codes
        timestamp last_import_assignment
        varchar latitude
        varchar longitude
        boolean send_email_reminder_unprocessed_enabled
        varchar send_email_reminder_unprocessed_object
        text send_email_reminder_unprocessed_content
    }

    agent {
        uuid id PK
        uuid id_agency FK
        uuid id_client FK
        varchar login
        varchar civil_title
        varchar first_name
        varchar last_name
        varchar phone
        varchar email
        varchar agency_name
        varchar agent_type
        json emails
        json sector_postal_codes
        json sector_iris
        timestamp created_at
        boolean receives_from_new_non_seller_property
        varchar carbon_copy_emails
        varchar google_agenda_access_token
        timestamp last_import_assignment
        boolean demo
        varchar id_task
        boolean right_can_delete_property
        boolean right_can_export_property
        boolean info_estimator_expert
        boolean info_estimator_broker
        boolean info_estimator_on_estimator
        boolean carbon_copy_for_new_property_seller
        boolean carbon_copy_for_new_property_non_seller
        boolean carbon_copy_for_new_appointment
        boolean carbon_copy_for_new_callback_request
    }

    property {
        uuid id_property PK
        uuid id_agent FK
        uuid id_agency FK
        uuid id_client FK
        varchar id_dpe
        timestamp dpe_date
        varchar agent_first_name
        varchar agent_last_name
        boolean new
        boolean archived
        varchar status
        varchar address_entered
        varchar address
        varchar postal_code
        varchar city
        varchar address_valid
        numeric latitude
        numeric longitude
        varchar first_name
        varchar last_name
        varchar phone
        varchar phone_valid
        varchar email
        varchar email_valid
        varchar property_type
        varchar sale_project
        timestamp sale_project_date
        timestamp sale_project_date_min
        timestamp sale_project_date_max
        timestamp appointment
        timestamp callback_request
        json advices
        json features
        json advantages
        json disadvantages
        json annexes
        json works
        json sources
        varchar search_sources_helper
        varchar source_tag_order
        text notepad
        boolean follow
        varchar refusal
        text refusal_information
        varchar signed_status
        boolean has_google_street
        varchar commission_type
        bigint commission
        timestamp created_date
        timestamp last_owner_interaction_date
        boolean exact_address
        timestamp reference_date_to_reassign
        integer state_of_day
        bigint reference_price
        varchar origin
        varchar for_search
        varchar automatic_follow_state
        timestamp automatic_follow_state_date_next
        integer reminders_info_done_all
        integer reminders_info_done_call
        integer reminders_info_done_sms
        integer reminders_info_done_email
        integer reminders_info_done_door_to_door
        integer reminders_info_done_mail
        integer reminders_info_done_appointment_request
        integer reminders_info_done_callback_request
        integer reminders_info_done_automation_email
        integer reminders_info_done_automation_sms
        integer reminders_info_to_do_all
        integer reminders_info_to_do_call
        integer reminders_info_to_do_sms
        integer reminders_info_to_do_email
        integer reminders_info_to_do_door_to_door
        integer reminders_info_to_do_mail
        integer reminders_info_to_do_appointment_request
        integer reminders_info_to_do_callback_request
        integer reminders_info_to_do_automation_email
        integer reminders_info_to_do_automation_sms
        integer reminders_info_today_all
        integer reminders_info_tomorrow_all
        integer reminders_info_upcoming_all
        integer reminders_info_in_past_all
        json reminders_info_reminder_sent_email
        json reminders_info_reminder_sent_call
        json reminders_info_reminder_sent_sms
        json reminders_info_reminder_sent_door_to_door
        json reminders_info_reminder_sent_mail
        uuid reminders_info_next_reminder_id_reminder
        timestamp reminders_info_next_reminder_date
        varchar reminders_info_next_reminder_type
        varchar reminders_info_next_reminder_status
        uuid reminders_info_next_appointment_id_reminder
        timestamp reminders_info_next_appointment_date
        varchar reminders_info_next_appointment_type
        varchar reminders_info_next_appointment_status
    }
`

type ViewMode = 'schema' | 'data'
type TableName = 'client' | 'agency' | 'agent' | 'property'

function MCDPage() {
  const [selectedDB, setSelectedDB] = useState<'v3' | 'v2' | 'estimateur'>('v3')
  const [viewMode, setViewMode] = useState<ViewMode>('schema')
  const [selectedTable, setSelectedTable] = useState<TableName>('client')
  const [tableData, setTableData] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    const fetchTableData = async () => {
      if (viewMode === 'data' && selectedDB === 'v3') {
        setLoading(true)
        try {
          const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/table/${selectedTable}`)
          if (!response.ok) {
            throw new Error('Erreur lors de la récupération des données')
          }
          const data = await response.json()
          setTableData(data)
        } catch (error) {
          console.error('Erreur:', error)
          setTableData([])
        } finally {
          setLoading(false)
        }
      }
    }
    fetchTableData()
  }, [viewMode, selectedTable, selectedDB])

  return (
    <div className="space-y-6">
      {/* Statistiques en haut */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                <svg className="w-6 h-6 text-blue-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
                </svg>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Tables</p>
              <p className="text-2xl font-bold text-gray-900">4</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center">
                <svg className="w-6 h-6 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Relations</p>
              <p className="text-2xl font-bold text-gray-900">6</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-purple-100 rounded-lg flex items-center justify-center">
                <svg className="w-6 h-6 text-purple-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Champs totaux</p>
              <p className="text-2xl font-bold text-gray-900">127+</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-orange-100 rounded-lg flex items-center justify-center">
                <svg className="w-6 h-6 text-orange-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
                </svg>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Version PostgreSQL</p>
              <p className="text-2xl font-bold text-gray-900">16.9</p>
            </div>
          </div>
        </div>
      </div>

      {/* Carte principale avec schéma */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        {/* En-tête */}
        <div className="px-6 py-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Schéma de la base de données</h3>
              <p className="text-sm text-gray-500 mt-1">Modèle conceptuel de données simplifié</p>
            </div>
            <div className="flex space-x-2">
              <button
                onClick={() => setSelectedDB('v3')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors cursor-pointer ${
                  selectedDB === 'v3'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                BDD V3
              </button>
              <button
                onClick={() => setSelectedDB('v2')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors cursor-pointer ${
                  selectedDB === 'v2'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                BDD V2
              </button>
              <button
                onClick={() => setSelectedDB('estimateur')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors cursor-pointer ${
                  selectedDB === 'estimateur'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                BDD Estimateur
              </button>
            </div>
          </div>
        </div>

        {/* Sous-menu pour BDD V3 */}
        {selectedDB === 'v3' && (
          <div className="px-6 py-3 border-b border-gray-200 bg-gray-50">
            <div className="flex items-center gap-2">
              <button
                onClick={() => setViewMode('schema')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
                  viewMode === 'schema'
                    ? 'bg-white text-blue-600 shadow-sm border border-blue-200'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
              >
                Schéma
              </button>
              <button
                onClick={() => setViewMode('data')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
                  viewMode === 'data'
                    ? 'bg-white text-blue-600 shadow-sm border border-blue-200'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
              >
                Données
              </button>

              {viewMode === 'data' && (
                <>
                  <div className="ml-4 h-6 w-px bg-gray-300"></div>
                  <select
                    value={selectedTable}
                    onChange={(e) => setSelectedTable(e.target.value as TableName)}
                    className="px-4 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white"
                  >
                    <option value="client">Table: client</option>
                    <option value="agency">Table: agency</option>
                    <option value="agent">Table: agent</option>
                    <option value="property">Table: property</option>
                  </select>
                </>
              )}
            </div>
          </div>
        )}

        {/* Contenu du schéma */}
        <div className="p-6">
          {selectedDB === 'v3' && viewMode === 'schema' && (
            <Suspense fallback={
              <div className="flex items-center justify-center h-64">
                <div className="text-center">
                  <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
                  <p className="text-gray-600 mt-4">Chargement du diagramme...</p>
                </div>
              </div>
            }>
              <MermaidDiagram schema={bddV3Schema} />
            </Suspense>
          )}
          {selectedDB === 'v3' && viewMode === 'data' && (
            <div>
              {loading ? (
                <div className="flex items-center justify-center h-64">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
                    <p className="text-gray-600 mt-4">Chargement des données...</p>
                  </div>
                </div>
              ) : tableData.length === 0 ? (
                <div className="text-center py-16 text-gray-500">
                  Aucune donnée disponible
                </div>
              ) : (() => {
                // Récupérer toutes les colonnes uniques de tous les enregistrements (calcul une seule fois)
                const allColumns = new Set<string>()
                tableData.forEach(row => {
                  Object.keys(row).forEach(col => allColumns.add(col))
                })
                const columnsArray = Array.from(allColumns)

                return (
                  <>
                    <div className="mb-4 text-sm text-gray-600">
                      <Database className="inline w-4 h-4 mr-2" />
                      {tableData.length} ligne(s) affichée(s) - {columnsArray.length} colonne(s) (limite: 100 lignes)
                    </div>
                    <div className="border border-gray-200 rounded-lg overflow-x-auto max-h-[600px] overflow-y-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                          <tr>
                            {columnsArray.map((column) => (
                              <th
                                key={column}
                                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap"
                              >
                                {column}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {tableData.map((row, rowIndex) => (
                            <tr key={rowIndex} className="hover:bg-gray-50">
                              {columnsArray.map((column, colIndex) => {
                                const value = row[column]
                                return (
                                  <td key={colIndex} className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap">
                                    {value === null || value === undefined ? (
                                      <span className="text-gray-400 italic">null</span>
                                    ) : typeof value === 'object' ? (
                                      <span className="text-xs text-gray-600 font-mono bg-gray-100 px-2 py-1 rounded">
                                        {JSON.stringify(value).substring(0, 50)}...
                                      </span>
                                    ) : typeof value === 'boolean' ? (
                                      <span className={`px-2 py-1 rounded text-xs font-medium ${value ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                                        {value ? 'true' : 'false'}
                                      </span>
                                    ) : (
                                      String(value).substring(0, 100)
                                    )}
                                  </td>
                                )
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )
              })()}
            </div>
          )}
          {selectedDB === 'v2' && <V2AgenciesTable />}
          {selectedDB === 'estimateur' && <EstimateurTable />}
        </div>

        {/* Pied de page avec légende */}
        {selectedDB === 'v3' && (
          <div className="px-6 py-4 bg-gray-50 border-t border-gray-200">
            <div className="flex items-center space-x-6 text-xs text-gray-600">
              <div className="flex items-center space-x-2">
                <span className="font-mono bg-blue-100 text-blue-700 px-2 py-1 rounded">PK</span>
                <span>Clé primaire</span>
              </div>
              <div className="flex items-center space-x-2">
                <span className="font-mono bg-green-100 text-green-700 px-2 py-1 rounded">FK</span>
                <span>Clé étrangère</span>
              </div>
              <div className="flex items-center space-x-2">
                <span>•</span>
                <span>Schéma simplifié - principaux champs uniquement</span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default MCDPage
