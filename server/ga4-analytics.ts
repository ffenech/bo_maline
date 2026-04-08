import { BetaAnalyticsDataClient } from '@google-analytics/data'
import fs from 'fs/promises'
import path from 'path'

const propertyIdV2 = '503799984' // Nouvel estimateur (estimerlogement.fr)
const propertyIdV1 = '282152729' // Ancien estimateur
const propertyIdEs = '508560412' // Estimateur Espagne (valorar-vivienda.es)
const cacheFilePathV2 = path.join(process.cwd(), 'ga4-cache-v2.json')
const cacheFilePathV1 = path.join(process.cwd(), 'ga4-cache-v1.json')
const cacheFilePathEs = path.join(process.cwd(), 'ga4-cache-es.json')
const CACHE_DURATION = 60 * 60 * 1000 // 1 heure en millisecondes

interface DailyVisitors {
  date: string
  visitors: number
}

interface CacheData {
  timestamp: number
  data: DailyVisitors[]
}

let analyticsClient: BetaAnalyticsDataClient | null = null
let ga4Available = true

// Initialiser le client GA4
async function initGA4Client(): Promise<BetaAnalyticsDataClient | null> {
  if (!ga4Available) {
    return null
  }

  if (!analyticsClient) {
    const credentialsPath = path.join(process.cwd(), 'ga4-service-account.json')

    // Vérifier si le fichier existe avant de créer le client
    try {
      await fs.access(credentialsPath)
    } catch {
      console.warn('⚠️ Fichier ga4-service-account.json non trouvé - GA4 désactivé')
      ga4Available = false
      return null
    }

    try {
      analyticsClient = new BetaAnalyticsDataClient({
        keyFilename: credentialsPath
      })
      console.log('✅ Client GA4 initialisé')
    } catch (error) {
      console.error('❌ Erreur initialisation GA4:', error)
      ga4Available = false
      return null
    }
  }
  return analyticsClient
}

// Récupérer les visiteurs quotidiens depuis GA4 pour une propriété donnée
async function fetchGA4DailyVisitors(propertyId: string): Promise<DailyVisitors[]> {
  const client = await initGA4Client()

  if (!client) {
    throw new Error('Client GA4 non disponible')
  }

  try {
    console.log(`📊 Récupération des données GA4 (${propertyId})...`)

    const [response] = await client.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [
        {
          startDate: '2024-09-14',
          endDate: 'today',
        },
      ],
      dimensions: [
        {
          name: 'date',
        },
      ],
      metrics: [
        {
          name: 'activeUsers', // Nombre de visiteurs actifs
        },
      ],
    })

    const data: DailyVisitors[] = []

    if (response.rows) {
      for (const row of response.rows) {
        const dateStr = row.dimensionValues?.[0]?.value || ''
        const visitors = parseInt(row.metricValues?.[0]?.value || '0')

        // Convertir format YYYYMMDD en YYYY-MM-DD
        const year = dateStr.substring(0, 4)
        const month = dateStr.substring(4, 6)
        const day = dateStr.substring(6, 8)
        const formattedDate = `${year}-${month}-${day}`

        data.push({
          date: formattedDate,
          visitors: visitors
        })
      }
    }

    console.log(`✅ ${data.length} jours de données GA4 récupérés (${propertyId})`)
    return data
  } catch (error) {
    console.error(`❌ Erreur lors de la récupération GA4 (${propertyId}):`, error)
    throw error
  }
}

// Lire le cache
async function readCache(cacheFilePath: string): Promise<CacheData | null> {
  try {
    const content = await fs.readFile(cacheFilePath, 'utf-8')
    return JSON.parse(content)
  } catch (error) {
    return null
  }
}

// Écrire le cache
async function writeCache(cacheFilePath: string, data: DailyVisitors[]): Promise<void> {
  const cacheData: CacheData = {
    timestamp: Date.now(),
    data: data
  }
  await fs.writeFile(cacheFilePath, JSON.stringify(cacheData, null, 2))
  console.log('💾 Cache GA4 mis à jour')
}

// Obtenir les visiteurs quotidiens V2 (avec cache)
export async function getDailyVisitorsV2(): Promise<DailyVisitors[]> {
  const cache = await readCache(cacheFilePathV2)

  if (cache) {
    const age = Date.now() - cache.timestamp
    if (age < CACHE_DURATION) {
      console.log('✅ Utilisation du cache GA4 V2 (age: ' + Math.round(age / 1000 / 60) + ' min)')
      return cache.data
    }
  }

  try {
    const data = await fetchGA4DailyVisitors(propertyIdV2)
    await writeCache(cacheFilePathV2, data)
    return data
  } catch (error) {
    if (cache) {
      console.log('⚠️ Erreur GA4 V2, utilisation du cache expiré')
      return cache.data
    }
    console.error('⚠️ Erreur GA4 V2 et pas de cache disponible, retour tableau vide')
    return []
  }
}

// Obtenir les visiteurs quotidiens V1 (avec cache)
export async function getDailyVisitorsV1(): Promise<DailyVisitors[]> {
  const cache = await readCache(cacheFilePathV1)

  if (cache) {
    const age = Date.now() - cache.timestamp
    if (age < CACHE_DURATION) {
      console.log('✅ Utilisation du cache GA4 V1 (age: ' + Math.round(age / 1000 / 60) + ' min)')
      return cache.data
    }
  }

  try {
    const data = await fetchGA4DailyVisitors(propertyIdV1)
    await writeCache(cacheFilePathV1, data)
    return data
  } catch (error) {
    if (cache) {
      console.log('⚠️ Erreur GA4 V1, utilisation du cache expiré')
      return cache.data
    }
    console.error('⚠️ Erreur GA4 V1 et pas de cache disponible, retour tableau vide')
    return []
  }
}

// Obtenir les visiteurs quotidiens ES (avec cache)
export async function getDailyVisitorsEs(): Promise<DailyVisitors[]> {
  const cache = await readCache(cacheFilePathEs)

  if (cache) {
    const age = Date.now() - cache.timestamp
    if (age < CACHE_DURATION) {
      console.log('✅ Utilisation du cache GA4 ES (age: ' + Math.round(age / 1000 / 60) + ' min)')
      return cache.data
    }
  }

  try {
    const data = await fetchGA4DailyVisitors(propertyIdEs)
    await writeCache(cacheFilePathEs, data)
    return data
  } catch (error) {
    if (cache) {
      console.log('⚠️ Erreur GA4 ES, utilisation du cache expiré')
      return cache.data
    }
    console.error('⚠️ Erreur GA4 ES et pas de cache disponible, retour tableau vide')
    return []
  }
}

// Tunnel de conversion : page views par pagePath
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

// Étapes principales du tunnel (parcours linéaire après convergence)
// Note: /ask-address et /validate-address sont des pages de fallback (adresse mal saisie sur la HP)
// Le vrai début du formulaire est /prix-m2/estimation-typologie où tous les utilisateurs convergent
const FUNNEL_STEPS_MAIN = [
  { path: '/', label: 'Homepage' },
  { path: '/prix-m2/estimation-typologie', label: 'Typologie (début formulaire)' },
  { path: '/prix-m2/estimation-surface', label: 'Surface' },
  { path: '/prix-m2/estimation-renove', label: 'État / Rénovation' },
  { path: '/prix-m2/estimation-caracteristiques', label: 'Caractéristiques' },
  { path: '/prix-m2/estimation-vue', label: 'Vue' },
  { path: '/prix-m2/estimation-exterieurs', label: 'Extérieurs' },
  { path: '/prix-m2/estimation-etage', label: 'Étage' },
  { path: '/prix-m2/estimation-proprietaire', label: 'Propriétaire' },
  { path: '/prix-m2/estimation-projet-vente', label: 'Projet de vente' },
  { path: '/prix-m2/estimation-coordonnees', label: 'Coordonnées' },
  { path: '/prix-m2/estimation-telephone', label: 'Téléphone' },
  { path: '/prix-m2/verification-sms', label: 'Vérification SMS' },
  { path: '/prix-m2/confirmation-estimation', label: 'Confirmation' },
  { path: '/estimation', label: 'Résultat estimation' },
]

// Étapes alternatives (fallback adresse)
const FUNNEL_STEPS_ALT = [
  { path: '/ask-address', label: 'Saisie adresse (fallback)' },
  { path: '/validate-address', label: 'Validation adresse (fallback)' },
]

const FUNNEL_STEPS = [...FUNNEL_STEPS_MAIN, ...FUNNEL_STEPS_ALT]

const funnelCachePathFr = path.join(process.cwd(), 'ga4-cache-funnel-fr.json')
const funnelCachePathEs = path.join(process.cwd(), 'ga4-cache-funnel-es.json')
const FUNNEL_CACHE_DURATION = 2 * 60 * 60 * 1000 // 2h

async function fetchGA4Funnel(propertyId: string, startDate: string, endDate: string): Promise<FunnelStep[]> {
  const client = await initGA4Client()
  if (!client) throw new Error('Client GA4 non disponible')

  const [response] = await client.runReport({
    property: `properties/${propertyId}`,
    dateRanges: [{ startDate, endDate }],
    dimensions: [{ name: 'pagePath' }],
    metrics: [
      { name: 'screenPageViews' },
      { name: 'activeUsers' },
    ],
    dimensionFilter: {
      orGroup: {
        expressions: FUNNEL_STEPS.map(step => ({
          filter: {
            fieldName: 'pagePath',
            stringFilter: {
              matchType: 'EXACT' as const,
              value: step.path,
              caseSensitive: false,
            }
          }
        }))
      }
    },
  })

  // Indexer les résultats par path
  const resultsByPath: Record<string, { pageViews: number; users: number }> = {}
  if (response.rows) {
    for (const row of response.rows) {
      const pagePath = row.dimensionValues?.[0]?.value || ''
      resultsByPath[pagePath] = {
        pageViews: parseInt(row.metricValues?.[0]?.value || '0'),
        users: parseInt(row.metricValues?.[1]?.value || '0'),
      }
    }
  }

  // Retourner les étapes principales dans l'ordre + les alternatives séparément
  const mainSteps = FUNNEL_STEPS_MAIN.map(step => ({
    path: step.path,
    label: step.label,
    pageViews: resultsByPath[step.path]?.pageViews || 0,
    users: resultsByPath[step.path]?.users || 0,
  }))

  const altSteps = FUNNEL_STEPS_ALT.map(step => ({
    path: step.path,
    label: step.label,
    pageViews: resultsByPath[step.path]?.pageViews || 0,
    users: resultsByPath[step.path]?.users || 0,
  }))

  return { mainSteps, altSteps }
}

export async function getConversionFunnel(country: 'fr' | 'es', startDate?: string, endDate?: string): Promise<FunnelData> {
  const propertyId = country === 'es' ? propertyIdEs : propertyIdV2
  const cachePath = country === 'es' ? funnelCachePathEs : funnelCachePathFr
  const start = startDate || '2026-03-01'
  const end = endDate || 'today'
  const cacheKey = `${start}_${end}`

  // Lire cache
  try {
    const content = await fs.readFile(cachePath, 'utf-8')
    const cache = JSON.parse(content)
    if (cache.key === cacheKey && Date.now() - cache.timestamp < FUNNEL_CACHE_DURATION) {
      console.log(`✅ Cache funnel ${country.toUpperCase()} utilisé`)
      return cache.data
    }
  } catch {}

  const { mainSteps, altSteps } = await fetchGA4Funnel(propertyId, start, end)
  const data: FunnelData = { steps: mainSteps, altSteps, period: { startDate: start, endDate: end } }

  // Écrire cache
  try {
    await fs.writeFile(cachePath, JSON.stringify({ key: cacheKey, timestamp: Date.now(), data }, null, 2))
  } catch {}

  return data
}

// Obtenir le nombre de visiteurs V2 pour une date spécifique
export async function getVisitorsForDateV2(date: string): Promise<number> {
  const dailyData = await getDailyVisitorsV2()
  const entry = dailyData.find(d => d.date === date)
  return entry?.visitors || 0
}

// Obtenir le nombre de visiteurs V1 pour une date spécifique
export async function getVisitorsForDateV1(date: string): Promise<number> {
  const dailyData = await getDailyVisitorsV1()
  const entry = dailyData.find(d => d.date === date)
  return entry?.visitors || 0
}

// Récupérer les visiteurs quotidiens filtrés par paramètre d'URL GA4
async function fetchGA4DailyVisitorsByUrlFilter(propertyId: string, urlFilter: string): Promise<DailyVisitors[]> {
  const client = await initGA4Client()

  if (!client) {
    throw new Error('Client GA4 non disponible')
  }

  try {
    console.log(`📊 [GA4] Début récupération données filtrées`)
    console.log(`📊 [GA4] Property ID: ${propertyId}`)
    console.log(`📊 [GA4] URL Filter: ${urlFilter}`)
    console.log(`📊 [GA4] Filtre complet: pagePathPlusQueryString CONTAINS "${urlFilter}"`)

    const requestConfig = {
      property: `properties/${propertyId}`,
      dateRanges: [
        {
          startDate: '2024-09-14',
          endDate: 'today',
        },
      ],
      dimensions: [
        {
          name: 'date',
        },
      ],
      metrics: [
        {
          name: 'activeUsers',
        },
      ],
      dimensionFilter: {
        filter: {
          fieldName: 'pagePathPlusQueryString',
          stringFilter: {
            matchType: 'CONTAINS' as const,
            value: urlFilter,
            caseSensitive: false
          }
        }
      },
    }

    console.log(`📊 [GA4] Configuration de la requête:`, JSON.stringify(requestConfig, null, 2))

    const [response] = await client.runReport(requestConfig)

    console.log(`📊 [GA4] Réponse reçue de GA4`)
    console.log(`📊 [GA4] Nombre de lignes: ${response.rows?.length || 0}`)
    console.log(`📊 [GA4] Row count: ${response.rowCount || 0}`)

    const data: DailyVisitors[] = []

    if (response.rows) {
      console.log(`📊 [GA4] Traitement de ${response.rows.length} lignes`)
      for (let i = 0; i < response.rows.length; i++) {
        const row = response.rows[i]
        const dateStr = row.dimensionValues?.[0]?.value || ''
        const visitors = parseInt(row.metricValues?.[0]?.value || '0')

        // Convertir format YYYYMMDD en YYYY-MM-DD
        const year = dateStr.substring(0, 4)
        const month = dateStr.substring(4, 6)
        const day = dateStr.substring(6, 8)
        const formattedDate = `${year}-${month}-${day}`

        if (i < 5) {
          console.log(`📊 [GA4] Ligne ${i + 1}: date=${formattedDate}, visitors=${visitors}`)
        }

        data.push({
          date: formattedDate,
          visitors: visitors
        })
      }
    } else {
      console.log(`⚠️ [GA4] Aucune ligne dans la réponse`)
    }

    console.log(`✅ [GA4] ${data.length} jours de données filtrées récupérées pour "${urlFilter}"`)
    if (data.length > 0) {
      console.log(`📊 [GA4] Exemple de dates: ${data.slice(0, 3).map(d => d.date).join(', ')}`)
      console.log(`📊 [GA4] Total visiteurs (premiers jours): ${data.slice(0, 3).map(d => d.visitors).join(', ')}`)
    }
    return data
  } catch (error: any) {
    console.error(`❌ [GA4] Erreur lors de la récupération filtrée (${propertyId}, ${urlFilter}):`, error)
    console.error(`❌ [GA4] Message d'erreur:`, error.message)
    console.error(`❌ [GA4] Stack:`, error.stack)
    throw error
  }
}

// Cache pour les requêtes filtrées par agences
const filteredCachePath = path.join(process.cwd(), 'ga4-cache-filtered.json')
interface FilteredCacheData {
  [cacheKey: string]: {
    timestamp: number
    data: DailyVisitors[]
  }
}

async function readFilteredCache(): Promise<FilteredCacheData> {
  try {
    const content = await fs.readFile(filteredCachePath, 'utf-8')
    return JSON.parse(content)
  } catch {
    return {}
  }
}

async function writeFilteredCache(cache: FilteredCacheData): Promise<void> {
  await fs.writeFile(filteredCachePath, JSON.stringify(cache, null, 2))
}

// Récupérer les visiteurs avec un filtre OR sur plusieurs agences (une seule requête GA4)
async function fetchGA4DailyVisitorsByMultipleAgencies(propertyId: string, agencyRewrites: string[]): Promise<DailyVisitors[]> {
  const client = await initGA4Client()

  if (!client) {
    throw new Error('Client GA4 non disponible')
  }

  // Construire les filtres OR pour toutes les agences
  // Format: agence=rewrite (le format le plus courant)
  const orFilters = agencyRewrites.map(rewrite => ({
    filter: {
      fieldName: 'pagePathPlusQueryString',
      stringFilter: {
        matchType: 'CONTAINS' as const,
        value: `agence=${rewrite}`,
        caseSensitive: false
      }
    }
  }))

  console.log(`📊 [GA4] Requête unique avec filtre OR sur ${agencyRewrites.length} agences`)

  try {
    const [response] = await client.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [
        {
          startDate: '2024-09-14',
          endDate: 'today',
        },
      ],
      dimensions: [
        {
          name: 'date',
        },
      ],
      metrics: [
        {
          name: 'activeUsers',
        },
      ],
      dimensionFilter: {
        orGroup: {
          expressions: orFilters
        }
      },
    })

    const data: DailyVisitors[] = []

    if (response.rows) {
      for (const row of response.rows) {
        const dateStr = row.dimensionValues?.[0]?.value || ''
        const visitors = parseInt(row.metricValues?.[0]?.value || '0')

        // Convertir format YYYYMMDD en YYYY-MM-DD
        const year = dateStr.substring(0, 4)
        const month = dateStr.substring(4, 6)
        const day = dateStr.substring(6, 8)
        const formattedDate = `${year}-${month}-${day}`

        data.push({
          date: formattedDate,
          visitors: visitors
        })
      }
    }

    console.log(`✅ [GA4] ${data.length} jours de données récupérées en une seule requête`)
    return data.sort((a, b) => a.date.localeCompare(b.date))
  } catch (error: any) {
    console.error(`❌ [GA4] Erreur requête OR:`, error.message)
    throw error
  }
}

// Obtenir les visiteurs temps réel (activeUsers des 30 dernières minutes) pour une propriété
async function fetchRealtimeActiveUsers(propertyId: string): Promise<number> {
  const client = await initGA4Client()
  if (!client) throw new Error('Client GA4 non disponible')

  const [response] = await client.runRealtimeReport({
    property: `properties/${propertyId}`,
    metrics: [{ name: 'activeUsers' }],
  })

  return parseInt(response.rows?.[0]?.metricValues?.[0]?.value || '0')
}

// Obtenir les visiteurs du jour via rapport GA4 (dateRange = today)
async function fetchTodayVisitors(propertyId: string): Promise<number> {
  const client = await initGA4Client()
  if (!client) throw new Error('Client GA4 non disponible')

  const [response] = await client.runReport({
    property: `properties/${propertyId}`,
    dateRanges: [{ startDate: 'today', endDate: 'today' }],
    metrics: [{ name: 'activeUsers' }],
  })

  return parseInt(response.rows?.[0]?.metricValues?.[0]?.value || '0')
}

export interface RealtimeVisitors {
  v2: number
  v1: number
  es: number
  realtime_v2: number
  realtime_es: number
  date: string
}

// Obtenir les visiteurs d'aujourd'hui (combinaison runReport today + runRealtimeReport)
export async function getTodayVisitors(): Promise<RealtimeVisitors> {
  const today = new Date()
  const date = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`

  const [todayV2, todayV1, todayEs, realtimeV2, realtimeEs] = await Promise.all([
    fetchTodayVisitors(propertyIdV2).catch(() => 0),
    fetchTodayVisitors(propertyIdV1).catch(() => 0),
    fetchTodayVisitors(propertyIdEs).catch(() => 0),
    fetchRealtimeActiveUsers(propertyIdV2).catch(() => 0),
    fetchRealtimeActiveUsers(propertyIdEs).catch(() => 0),
  ])

  // Prendre le max entre le rapport du jour et le temps réel
  // Le rapport du jour a du retard, le realtime est instantané mais ne couvre que 30 min
  return {
    v2: Math.max(todayV2, realtimeV2),
    v1: todayV1,
    es: Math.max(todayEs, realtimeEs),
    realtime_v2: realtimeV2,
    realtime_es: realtimeEs,
    date,
  }
}

// Série quotidienne des visiteurs sur la HP + la page typologie
// Permet de mesurer le taux de passage HP → début du formulaire
export interface DailyFunnelEntry {
  date: string
  homepage: number
  typology: number
}

const funnelStepsCachePath = path.join(process.cwd(), 'ga4-cache-funnel-steps-fr.json')

async function fetchGA4DailyFunnelSteps(propertyId: string): Promise<DailyFunnelEntry[]> {
  const client = await initGA4Client()
  if (!client) throw new Error('Client GA4 non disponible')

  console.log(`📊 [GA4] Récupération daily funnel HP→typologie (${propertyId})...`)

  const [response] = await client.runReport({
    property: `properties/${propertyId}`,
    dateRanges: [{ startDate: '2024-09-14', endDate: 'today' }],
    dimensions: [{ name: 'date' }, { name: 'pagePath' }],
    metrics: [{ name: 'activeUsers' }],
    dimensionFilter: {
      orGroup: {
        expressions: [
          {
            filter: {
              fieldName: 'pagePath',
              stringFilter: { matchType: 'EXACT' as const, value: '/', caseSensitive: false },
            },
          },
          {
            filter: {
              fieldName: 'pagePath',
              stringFilter: { matchType: 'EXACT' as const, value: '/prix-m2/estimation-typologie', caseSensitive: false },
            },
          },
        ],
      },
    },
    limit: 100000,
  })

  const byDate = new Map<string, { homepage: number; typology: number }>()
  if (response.rows) {
    for (const row of response.rows) {
      const dateStr = row.dimensionValues?.[0]?.value || ''
      const pagePath = row.dimensionValues?.[1]?.value || ''
      const users = parseInt(row.metricValues?.[0]?.value || '0')
      const formattedDate = `${dateStr.substring(0, 4)}-${dateStr.substring(4, 6)}-${dateStr.substring(6, 8)}`
      const entry = byDate.get(formattedDate) || { homepage: 0, typology: 0 }
      if (pagePath === '/') entry.homepage += users
      else if (pagePath === '/prix-m2/estimation-typologie') entry.typology += users
      byDate.set(formattedDate, entry)
    }
  }

  const result: DailyFunnelEntry[] = Array.from(byDate.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, v]) => ({ date, homepage: v.homepage, typology: v.typology }))

  console.log(`✅ [GA4] ${result.length} jours de funnel HP→typologie récupérés`)
  return result
}

export async function getDailyFunnelHpToTypology(): Promise<DailyFunnelEntry[]> {
  try {
    const content = await fs.readFile(funnelStepsCachePath, 'utf-8')
    const cache = JSON.parse(content) as { timestamp: number; data: DailyFunnelEntry[] }
    if (Date.now() - cache.timestamp < CACHE_DURATION) {
      console.log(`✅ Utilisation du cache funnel HP→typologie (age: ${Math.round((Date.now() - cache.timestamp) / 1000 / 60)} min)`)
      return cache.data
    }
  } catch {}

  try {
    const data = await fetchGA4DailyFunnelSteps(propertyIdV2)
    await fs.writeFile(funnelStepsCachePath, JSON.stringify({ timestamp: Date.now(), data }, null, 2))
    console.log('💾 Cache funnel HP→typologie mis à jour')
    return data
  } catch (error) {
    console.error('❌ Erreur GA4 funnel HP→typologie:', error)
    try {
      const content = await fs.readFile(funnelStepsCachePath, 'utf-8')
      const cache = JSON.parse(content) as { timestamp: number; data: DailyFunnelEntry[] }
      console.log('⚠️ Fallback sur cache expiré funnel HP→typologie')
      return cache.data
    } catch {
      return []
    }
  }
}

// Obtenir les visiteurs quotidiens V2 filtrés par agences (rewrite) - OPTIMISÉ
export async function getDailyVisitorsV2ByAgencies(agencyRewrites: string[]): Promise<DailyVisitors[]> {
  console.log(`🔍 [GA4] getDailyVisitorsV2ByAgencies appelé avec ${agencyRewrites.length} agences`)

  if (agencyRewrites.length === 0) {
    return []
  }

  // Créer une clé de cache basée sur les agences triées
  const cacheKey = agencyRewrites.sort().join(',')

  // Vérifier le cache
  const cache = await readFilteredCache()
  const cached = cache[cacheKey]
  if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
    console.log(`✅ [GA4] Utilisation du cache filtré (age: ${Math.round((Date.now() - cached.timestamp) / 1000 / 60)} min)`)
    return cached.data
  }

  try {
    // Utiliser une seule requête GA4 avec filtre OR
    const data = await fetchGA4DailyVisitorsByMultipleAgencies(propertyIdV2, agencyRewrites)

    // Mettre en cache
    cache[cacheKey] = { timestamp: Date.now(), data }
    await writeFilteredCache(cache)
    console.log(`💾 [GA4] Cache filtré mis à jour pour ${agencyRewrites.length} agences`)

    return data
  } catch (error: any) {
    // En cas d'erreur, retourner le cache périmé si disponible
    if (cached) {
      console.log(`⚠️ [GA4] Erreur, utilisation du cache filtré expiré`)
      return cached.data
    }
    console.error('⚠️ [GA4] Erreur et pas de cache disponible, retour tableau vide:', error.message)
    return []
  }
}