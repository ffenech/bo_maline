import express from 'express'
import cors from 'cors'
import dotenv from 'dotenv'
import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { execSync } from 'node:child_process'
import { Pool, type PoolConfig } from 'pg'
import mysql from 'mysql2/promise'
import { createSSHTunnel, closeSSHTunnel, setOnTunnelDied } from './ssh-tunnel.js'
import { getDailyVisitorsV2, getDailyVisitorsV1, getDailyVisitorsEs, getConversionFunnel, getTodayVisitors, getDailyFunnelHpToTypology } from './ga4-analytics.js'
import {
  getAuthUrl,
  exchangeCodeForToken,
  isAuthenticated,
  fetchInvoices,
  groupInvoices,
  downloadAttachment,
  downloadInvoicesAsZip,
  extractFirstPage,
  invalidateInvoicesCache,
  type InvoiceData
} from './gmail-invoices.js'
import { supabaseAdmin } from './supabase-admin.js'

dotenv.config({ override: true })

// Relire le .env à chaud pour récupérer les credentials Google Ads mis à jour sans redémarrer
function reloadGoogleAdsEnv() {
  dotenv.config({ override: true })
}

const app = express()
const PORT = process.env.PORT || 3001

// Support .env côté serveur ET .env Vite (prefix VITE_)
const ESTIMATEUR_API_URL = process.env.ESTIMATEUR_API_URL || process.env.VITE_ESTIMATEUR_API_URL
const ESTIMATEUR_API_TOKEN = process.env.ESTIMATEUR_API_TOKEN || process.env.VITE_ESTIMATEUR_API_TOKEN

app.use(cors())
app.use(express.json({ limit: '100mb' }))

// Variables globales pour la connexion
let dbPool: Pool | null = null
let sshProcess: any = null
let hasAgencySlugColumn = true // sera mis a jour au demarrage

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const ENV_FILE_PATH = path.resolve(__dirname, '../.env')
const DEFAULT_DB_INCREMENT_LIMIT = parseInt(process.env.DB_NAME_MAX_INCREMENT ?? '5', 10)

function buildPoolConfig(overrides: Partial<PoolConfig> = {}): PoolConfig {
  return {
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || '20184', 10),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    ssl: {
      rejectUnauthorized: false
    },
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
    ...overrides
  }
}

async function persistDatabaseName(dbName: string) {
  process.env.DB_NAME = dbName

  try {
    let envContent = ''
    let newline = '\n'

    try {
      envContent = await fs.readFile(ENV_FILE_PATH, 'utf8')
      const match = envContent.match(/\r\n/)
      if (match) {
        newline = '\r\n'
      }
    } catch (readError: any) {
      if (readError.code !== 'ENOENT') {
        console.warn('⚠️  Impossible de lire le fichier .env:', readError.message)
      }
    }

    const lines = envContent ? envContent.split(/\r?\n/) : []
    let updated = false
    const updatedLines = lines.map((line) => {
      if (line.startsWith('DB_NAME=')) {
        updated = true
        return `DB_NAME=${dbName}`
      }
      return line
    })

    if (!updated) {
      updatedLines.push(`DB_NAME=${dbName}`)
    }

    const sanitized = updatedLines.filter((line, index, arr) => !(line === '' && index === arr.length - 1))
    const finalContent = sanitized.join(newline) + newline
    await fs.writeFile(ENV_FILE_PATH, finalContent, 'utf8')
    console.log(`💾 DB_NAME mis à jour dans .env (${dbName})`)
  } catch (writeError: any) {
    console.warn('⚠️  Impossible de sauvegarder le DB_NAME dans .env:', writeError.message)
  }
}

async function tryCreatePool(dbName: string): Promise<Pool> {
  const pool = new Pool(buildPoolConfig({ database: dbName }))

  try {
    const client = await pool.connect()
    await client.query('SELECT 1')
    client.release()
    return pool
  } catch (error) {
    await pool.end().catch(() => {
      // Ignorer les erreurs de fermeture
    })
    throw error
  }
}

function generateCandidateDbNames(initialName: string): string[] {
  const candidates = [initialName]
  const match = initialName.match(/^(.*?)(\d+)$/)
  const incrementLimit = Number.isFinite(DEFAULT_DB_INCREMENT_LIMIT) && DEFAULT_DB_INCREMENT_LIMIT > 0
    ? DEFAULT_DB_INCREMENT_LIMIT
    : 5

  if (!match) {
    return candidates
  }

  const [, prefix, suffix] = match
  let current = parseInt(suffix, 10)
  if (Number.isNaN(current)) {
    return candidates
  }

  for (let i = 1; i <= incrementLimit; i += 1) {
    current += 1
    candidates.push(`${prefix}${current}`)
  }

  return candidates
}

async function findWorkingDatabase(): Promise<{ pool: Pool; dbName: string }> {
  const initialName = process.env.DB_NAME

  if (!initialName) {
    throw new Error('DB_NAME must be defined in environment variables')
  }

  const candidates = generateCandidateDbNames(initialName)

  for (const candidate of candidates) {
    try {
      console.log(`🔄 Test de connexion sur la base "${candidate}"...`)
      const pool = await tryCreatePool(candidate)
      console.log(`✅ Connexion réussie sur "${candidate}"`)

      await persistDatabaseName(candidate)

      return { pool, dbName: candidate }
    } catch (error) {
      console.error(`❌ Echec de connexion sur "${candidate}":`, error)
    }
  }

  throw new Error(`Impossible de se connecter a aucune base parmi: ${candidates.join(', ')}`)
}

// --- Reconnexion automatique SSH + PostgreSQL ---
let isReconnecting = false
const RECONNECT_DELAY_MS = 5_000
const RECONNECT_MAX_ATTEMPTS = 10
const DB_HEALTH_CHECK_INTERVAL_MS = 60_000 // vérifie la BDD toutes les 60s

async function reconnectDatabase() {
  if (isReconnecting) return
  isReconnecting = true
  console.log('🔄 [reconnect] Début de la procédure de reconnexion...')

  for (let attempt = 1; attempt <= RECONNECT_MAX_ATTEMPTS; attempt++) {
    try {
      // Fermer l'ancien pool proprement
      if (dbPool) {
        try { await dbPool.end() } catch (_) {}
        dbPool = null
      }
      // Fermer l'ancien tunnel
      closeSSHTunnel(sshProcess)
      sshProcess = null

      console.log(`🔄 [reconnect] Tentative ${attempt}/${RECONNECT_MAX_ATTEMPTS}...`)
      await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS * attempt))

      sshProcess = await createSSHTunnel()
      const { pool, dbName } = await findWorkingDatabase()
      dbPool = pool

      // Test réel
      const client = await dbPool.connect()
      await client.query('SELECT 1')
      client.release()

      console.log(`✅ [reconnect] Reconnexion réussie sur "${dbName}" (tentative ${attempt})`)
      isReconnecting = false
      return
    } catch (err) {
      console.error(`❌ [reconnect] Tentative ${attempt} échouée:`, (err as Error).message)
    }
  }

  console.error(`❌ [reconnect] Échec après ${RECONNECT_MAX_ATTEMPTS} tentatives. Le serveur continue sans BDD.`)
  isReconnecting = false
}

// Vérifie périodiquement que la connexion BDD est vivante
function startDatabaseHealthCheck() {
  setInterval(async () => {
    if (!dbPool || isReconnecting) return
    try {
      const client = await dbPool.connect()
      await client.query('SELECT 1')
      client.release()
    } catch (err) {
      console.error('❌ [db-healthcheck] Connexion perdue:', (err as Error).message)
      reconnectDatabase()
    }
  }, DB_HEALTH_CHECK_INTERVAL_MS)
}

// Initialisation de la connexion PostgreSQL
async function initDatabase() {
  try {
    console.log('🔄 Configuration du tunnel SSH...')

    // Établir le tunnel SSH
    sshProcess = await createSSHTunnel()

    // Déclencher la reconnexion si le tunnel tombe
    setOnTunnelDied(() => reconnectDatabase())

    console.log('🔄 Connexion à PostgreSQL...')

    // Configuration du pool PostgreSQL
    const { pool, dbName } = await findWorkingDatabase()
    dbPool = pool

    console.log(`📚 Base utilisée: ${dbName}`)

    // Test de la connexion
    const client = await dbPool.connect()
    console.log('✅ Connexion PostgreSQL établie')

    // Vérifier les tables disponibles
    const result = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name
    `)

    console.log('📋 Tables disponibles:', result.rows.map(r => r.table_name).join(', '))

    // Verifier si la colonne slug existe dans la table agency
    const slugCheck = await client.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'agency' AND column_name = 'slug'
    `)
    hasAgencySlugColumn = slugCheck.rows.length > 0
    console.log(`📋 Colonne agency.slug: ${hasAgencySlugColumn ? 'presente' : 'absente'}`)

    client.release()

    // Lancer la vérification périodique de la BDD
    startDatabaseHealthCheck()

  } catch (error) {
    console.error('❌ Erreur de connexion à la base de données:', error)
    throw error
  }
}

// Helper: filtre slug conditionnel
function slugFilter(): string {
  return hasAgencySlugColumn ? "AND a.slug NOT LIKE 'demo_%'" : ''
}

// Routes API

// Route racine
app.get('/', (req, res) => {
  res.redirect('http://localhost:5173/gestion-pub')
})

// Santé de l'API (test réel de la connexion BDD)
app.get('/api/health', async (req, res) => {
  let dbStatus = 'disconnected'
  if (dbPool) {
    try {
      const client = await dbPool.connect()
      await client.query('SELECT 1')
      client.release()
      dbStatus = 'connected'
    } catch {
      dbStatus = 'error'
      // Déclencher une reconnexion en arrière-plan
      reconnectDatabase()
    }
  }
  const isOk = dbStatus === 'connected'
  res.status(isOk ? 200 : 503).json({
    status: isOk ? 'ok' : 'degraded',
    database: dbStatus,
    reconnecting: isReconnecting,
    timestamp: new Date().toISOString()
  })
})

// ============== SYSTÈME DE CACHE GÉNÉRIQUE ==============
const CACHE_TTL_MS = 5 * 60 * 1000 // 5 minutes par défaut
const CACHE_TTL_LONG_MS = 15 * 60 * 1000 // 15 minutes pour les données moins volatiles

interface CacheEntry<T> {
  data: T
  timestamp: number
  ttl: number
}

const apiCache = new Map<string, CacheEntry<any>>()

function getCached<T>(key: string): T | null {
  const entry = apiCache.get(key)
  if (entry && Date.now() - entry.timestamp < entry.ttl) {
    console.log(`📦 Cache HIT: ${key}`)
    return entry.data
  }
  if (entry) {
    console.log(`⏰ Cache EXPIRED: ${key}`)
  }
  return null
}

// Retourne les données même expirées (pour stale-while-revalidate)
function getStaleCache<T>(key: string): T | null {
  const entry = apiCache.get(key)
  if (entry) return entry.data
  return null
}

function setCache<T>(key: string, data: T, ttl: number = CACHE_TTL_MS): void {
  apiCache.set(key, { data, timestamp: Date.now(), ttl })
  console.log(`💾 Cache SET: ${key} (TTL: ${ttl / 1000}s)`)
}

function invalidateCache(keyPattern?: string): void {
  if (keyPattern) {
    for (const key of apiCache.keys()) {
      if (key.includes(keyPattern)) {
        apiCache.delete(key)
        console.log(`🗑️ Cache INVALIDATED: ${key}`)
      }
    }
  } else {
    apiCache.clear()
    console.log('🗑️ Cache CLEARED')
  }
}

// Cache pour les agences de l'estimateur (évite les appels répétés à l'API externe)
let estimateurAgenciesCache: { data: Set<string>; timestamp: number } | null = null

// Fonction pour récupérer les agences de l'estimateur (avec cache)
async function fetchEstimateurAgencies(): Promise<Set<string>> {
  // Vérifier si le cache est valide
  if (estimateurAgenciesCache && Date.now() - estimateurAgenciesCache.timestamp < CACHE_TTL_MS) {
    return estimateurAgenciesCache.data
  }

  try {
    if (!ESTIMATEUR_API_URL || !ESTIMATEUR_API_TOKEN) {
      console.warn('fetchEstimateurAgencies: variables ESTIMATEUR_API_URL/TOKEN manquantes')
      return new Set()
    }

    const response = await fetch(`${ESTIMATEUR_API_URL}/agences/list`, {
      headers: {
        'Authorization': `Bearer ${ESTIMATEUR_API_TOKEN}`,
        'Content-Type': 'application/json'
      }
    })

    if (!response.ok) {
      console.error('Erreur API Estimateur:', response.status)
      return estimateurAgenciesCache?.data || new Set()
    }

    const data = await response.json()

    // Extraire les idClient des agences qui utilisent l'estimateur V2
    const v2AgencyIds = new Set<string>()
    if (data.success && Array.isArray(data.data)) {
      data.data.forEach((agency: any) => {
        if (agency.idClient) {
          v2AgencyIds.add(agency.idClient)
        }
      })
    }

    // Mettre en cache
    estimateurAgenciesCache = { data: v2AgencyIds, timestamp: Date.now() }
    console.log(`Cache agences V2 mis à jour: ${v2AgencyIds.size} agences`)

    return v2AgencyIds
  } catch (error) {
    console.error('Erreur lors de la récupération des agences estimateur:', error)
    // Retourner le cache périmé en cas d'erreur
    return estimateurAgenciesCache?.data || new Set()
  }
}

// Récupérer tous les clients avec leurs statistiques agrégées
app.get('/api/agencies', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any[]>('agencies')
    if (cached) {
      return res.json(cached)
    }

    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    // Récupérer tous les clients de la BDD V3
    const query = `
      SELECT
        c.id_client as id,
        c.name,
        COUNT(DISTINCT a.id)::integer as agency_count,
        COUNT(DISTINCT ag.id)::integer as agent_count,
        COUNT(DISTINCT p.id_property)::integer as property_count,
        COUNT(DISTINCT CASE
          WHEN p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          THEN p.id_property
        END)::integer as seller_leads_count
      FROM client c
      LEFT JOIN agency a ON a.id_client = c.id_client
      LEFT JOIN agent ag ON ag.id_agency = a.id
      LEFT JOIN property p ON p.id_agency = a.id
      GROUP BY c.id_client, c.name
      ORDER BY c.id_client
    `

    const result = await dbPool.query(query)

    // Récupérer les clients qui utilisent l'estimateur V2 depuis l'API
    const v2ClientIds = await fetchEstimateurAgencies()

    // Ajouter la version de l'estimateur en fonction de la présence dans l'API Estimateur
    const clients = result.rows.map(client => ({
      ...client,
      estimateur_version: v2ClientIds.has(client.id) ? 'V2' : 'V1'
    }))

    setCache('agencies', clients, CACHE_TTL_LONG_MS)
    res.json(clients)
  } catch (error) {
    console.error('Erreur lors de la récupération des agences:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Helper: récupérer les agences V2 depuis l'API Territory
async function fetchAgenciesV2Raw(): Promise<any[]> {
  const response = await fetch('https://back-api.maline-immobilier.fr/territory/api/agences', {
    headers: {
      'x-api-key': '70c51af056cccd8a1fa1434be9fddfa4a0e86929e5b65055db844f38ba4b3fce'
    }
  })

  if (!response.ok) {
    throw new Error('Erreur API V2')
  }
  const data = await response.json()
  // Certaines API renvoient { data: [...] }
  if (Array.isArray(data)) return data
  if (Array.isArray(data?.data)) return data.data
  return []
}

// --- Utilitaires V2 pour pub-stats (alignés sur sync-agency-stats) ---

interface V2AgencyData {
  nom: string
  identifier: string
  id_gocardless: string | null
  nombre_logements: number | null
  tarifs: { code_postal: string; tarif: string }[]
  startDate: Date | null
  endDate: Date | null
}

interface V2Data {
  byGocardless: Map<string, V2AgencyData>
  byName: Map<string, V2AgencyData>
}

function normalizeAgencyName(name: string): string {
  return name
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]/g, '')
    .trim()
}

const V2_START_DATE_KEYS = [
  'date_start', 'dateStart',
  'startDate', 'start_at', 'startAt', 'startedAt',
  'subscriptionStart', 'subscription.startDate', 'subscription.start_at',
  'activationDate', 'activatedAt', 'activated_at',
  'createdAt', 'created_at', 'created_date',
  'dateDebut', 'date_debut', 'debut'
]

async function fetchV2AgenciesData(): Promise<V2Data> {
  const agencies = await fetchAgenciesV2Raw()
  const byGocardless = new Map<string, V2AgencyData>()
  const byName = new Map<string, V2AgencyData>()

  for (const agency of agencies) {
    // Enrichir avec les dates (avec fallback comme agency-stats)
    let start = pickDate(agency, V2_START_DATE_KEYS)
    if (!start) start = pickDate(agency, ['created', 'creationDate'])
    agency.startDate = start
    agency.endDate = pickDate(agency, ['date_fin', 'dateFin', 'endDate', 'end_at', 'endAt', 'canceledAt', 'cancelledAt'])

    if (agency.id_gocardless) {
      const cleanGcl = agency.id_gocardless.replace(/[^\x20-\x7E]/g, '').trim()
      if (cleanGcl) byGocardless.set(cleanGcl, agency)
    }
    if (agency.nom) {
      const normalized = normalizeAgencyName(agency.nom)
      if (normalized) byName.set(normalized, agency)
    }
  }

  return { byGocardless, byName }
}

function findV2Agency(row: { id_gocardless?: string; client_name?: string }, v2Data: V2Data): V2AgencyData | null {
  const gcl = row.id_gocardless?.replace(/"/g, '')
  if (gcl && v2Data.byGocardless.has(gcl)) return v2Data.byGocardless.get(gcl)!
  if (row.client_name) {
    const normalized = normalizeAgencyName(row.client_name)
    if (normalized && v2Data.byName.has(normalized)) return v2Data.byName.get(normalized)!
  }
  return null
}

function coerceDate(input: any): Date | null {
  if (!input) return null
  const d = new Date(input)
  return isNaN(d.getTime()) ? null : d
}

function getValueByPath(obj: any, path: string): any {
  if (!obj || !path) return undefined
  const parts = path.split('.')
  let current: any = obj
  for (const p of parts) {
    if (current && Object.prototype.hasOwnProperty.call(current, p)) {
      current = current[p]
    } else {
      return undefined
    }
  }
  return current
}

function pickDate(obj: any, candidates: string[]): Date | null {
  for (const key of candidates) {
    // support des champs imbriqués par notation pointée
    const raw = getValueByPath(obj, key)
    const d = coerceDate(raw)
    if (d) return d
  }
  return null
}

function monthStart(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth(), 1, 0, 0, 0, 0)
}

function monthEnd(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth() + 1, 0, 23, 59, 59, 999)
}

function addMonths(d: Date, n: number): Date {
  return new Date(d.getFullYear(), d.getMonth() + n, 1)
}

// Statistiques mensuelles des agences (BDD V2)
app.get('/api/agency-stats', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any[]>('agency-stats')
    if (cached) {
      return res.json(cached)
    }

    // 1) Récupération des agences V2 (doivent contenir date de début et éventuellement date de fin)
    const agencies = await fetchAgenciesV2Raw()

    // 2) Normalisation des dates (robuste aux variations de nommage)
    const startKeys = [
      // anglais usuels
      'startDate', 'start_at', 'startAt', 'startedAt',
      'subscriptionStart', 'subscription.startDate', 'subscription.start_at', 'subscription.startedAt',
      'activationDate', 'activatedAt', 'activated_at',
      'createdAt', 'created_at', 'created_date',
      'dateStart', 'date_start',
      // français usuels
      'dateDebut', 'date_debut', 'debut'
    ]
    const endKeys = [
      'endDate', 'end_at', 'endAt', 'endedAt',
      'subscriptionEnd', 'subscription.endDate', 'subscription.end_at', 'subscription.endedAt',
      'deactivationDate', 'deactivatedAt', 'deactivated_at',
      'canceledAt', 'cancelledAt', 'cancellationDate',
      'closedAt', 'closed_at',
      'dateEnd', 'date_end',
      'dateFin', 'date_fin', 'resiliationDate', 'resiliation_date', 'fin'
    ]

    const normalized = agencies.map((a) => {
      let start = pickDate(a, startKeys)
      let end = pickDate(a, endKeys)
      // fallback ultime: certains jeux renvoient "created" ou "creationDate"
      if (!start) start = pickDate(a, ['created', 'creationDate'])
      return { start, end }
    }).filter(a => a.start) // on conserve uniquement celles avec date de début valide

    if (normalized.length === 0) {
      return res.json([])
    }

    // 3) Déterminer l'intervalle mensuel [minStart .. now]
    const minStart = normalized.reduce((min: Date, a) => (a.start! < min ? a.start! : min), normalized[0].start!)
    const today = new Date()
    const firstMonth = monthStart(minStart)
    const lastMonth = monthStart(today)

    // 4) Construire la série mensuelle
    const months: { label: string; active: number; churned: number; churnRate: number }[] = []
    for (let cursor = new Date(firstMonth); cursor <= lastMonth; cursor = addMonths(cursor, 1)) {
      const mStart = monthStart(cursor)
      const mEnd = monthEnd(cursor)

      // Actifs au début du mois (pour calcul du churn)
      const activeAtStart = normalized.filter(a => a.start! <= mStart && (!a.end || a.end > mStart)).length
      // Actifs en fin de mois (état du parc)
      const activeAtEnd = normalized.filter(a => a.start! <= mEnd && (!a.end || a.end > mEnd)).length
      // Résiliés durant le mois
      const churnedInMonth = normalized.filter(a => a.end && a.end >= mStart && a.end <= mEnd).length

      const churnRate = activeAtStart > 0 ? (churnedInMonth / activeAtStart) * 100 : 0

      const yyyy = mStart.getFullYear()
      const mm = String(mStart.getMonth() + 1).padStart(2, '0')
      months.push({
        label: `${yyyy}-${mm}`,
        active: activeAtEnd,
        churned: churnedInMonth,
        churnRate: Math.round(churnRate * 100) / 100,
      })
    }

    setCache('agency-stats', months, CACHE_TTL_LONG_MS)
    res.json(months)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats agences V2:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer une agence spécifique
app.get('/api/agencies/:id', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const { id } = req.params

    const query = `
      SELECT
        a.*,
        COUNT(DISTINCT ag.id) as agent_count,
        COUNT(DISTINCT p.id_property) as property_count
      FROM agency a
      LEFT JOIN agent ag ON ag.id_agency = a.id
      LEFT JOIN property p ON p.id_agency = a.id
      WHERE a.id = $1
      GROUP BY a.id
    `

    const result = await dbPool.query(query, [id])

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Agency not found' })
    }

    res.json(result.rows[0])
  } catch (error) {
    console.error('Erreur lors de la récupération de l\'agence:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les leads vendeur jour par jour pour les clients espagnols (locale='es')
app.get('/api/leads/v2-daily-es', async (req, res) => {
  try {
    const cacheKey = 'leads-es-daily'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    // Calculer la date 90 jours en arrière
    const since90d = new Date()
    since90d.setDate(since90d.getDate() - 90)
    const since90dStr = since90d.toISOString().slice(0, 10)

    const { data: sbData, error: sbErr } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, total_leads')
      .eq('source', 'es')
      .gte('stat_date', since90dStr)
      .order('stat_date', { ascending: true })

    if (!sbErr && sbData && sbData.length > 0) {
      const result = sbData.map(r => ({ date: r.stat_date, total_leads: r.total_leads }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback PostgreSQL
    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })
    const result = await dbPool.query(`
      SELECT DATE(p.created_date) as date, COUNT(DISTINCT p.id_property)::integer as total_leads
      FROM property p INNER JOIN agency a ON p.id_agency = a.id INNER JOIN client c ON a.id_client = c.id_client
      WHERE c.locale = 'es_ES'
        AND c.demo IS NOT TRUE
        ${slugFilter()}
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND DATE(p.created_date) >= CURRENT_DATE - INTERVAL '90 days' AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) ASC
    `)
    setCache(cacheKey, result.rows)
    return res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des leads V2 ES:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les statistiques téléphone quotidiennes pour les clients espagnols
app.get('/api/leads/v2-daily-phone-es', async (req, res) => {
  try {
    const cacheKey = 'leads-es-daily-phone'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const since90d = new Date()
    since90d.setDate(since90d.getDate() - 90)
    const since90dStr = since90d.toISOString().slice(0, 10)

    const { data: sbData, error: sbErr } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, leads_with_phone, leads_with_validated_phone')
      .eq('source', 'es')
      .gte('stat_date', since90dStr)
      .order('stat_date', { ascending: true })

    if (!sbErr && sbData && sbData.length > 0) {
      const result = sbData.map(r => ({ date: r.stat_date, leads_with_phone: r.leads_with_phone, leads_with_validated_phone: r.leads_with_validated_phone }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback PostgreSQL
    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })
    const result = await dbPool.query(`
      SELECT DATE(p.created_date) as date,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
      FROM property p INNER JOIN agency a ON p.id_agency = a.id INNER JOIN client c ON a.id_client = c.id_client
      WHERE c.locale = 'es_ES'
        AND c.demo IS NOT TRUE
        ${slugFilter()}
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND DATE(p.created_date) >= CURRENT_DATE - INTERVAL '90 days' AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) ASC
    `)
    setCache(cacheKey, result.rows)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats téléphone ES:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les leads (properties) avec toutes leurs données
app.get('/api/leads', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const query = `
      SELECT
        p.*,
        a.name as agency_name,
        ag.first_name as agent_first_name_from_agent,
        ag.last_name as agent_last_name_from_agent,
        c.name as client_name
      FROM property p
      LEFT JOIN agency a ON p.id_agency = a.id
      LEFT JOIN agent ag ON p.id_agent = ag.id
      LEFT JOIN client c ON p.id_client = c.id_client
      ORDER BY p.created_date DESC
      LIMIT 1
    `

    const result = await dbPool.query(query)

    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des leads:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer toutes les données d'une table
app.get('/api/table/:tableName', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const { tableName } = req.params
    const allowedTables = ['client', 'agency', 'agent', 'property']

    if (!allowedTables.includes(tableName)) {
      return res.status(400).json({ error: 'Invalid table name' })
    }

    // Définir l'ordre selon la table
    let orderBy = ''
    if (tableName === 'client') {
      orderBy = 'ORDER BY id_client LIMIT 100'
    } else if (tableName === 'agency') {
      orderBy = 'ORDER BY id LIMIT 100'
    } else if (tableName === 'agent') {
      orderBy = 'ORDER BY created_at DESC LIMIT 100'
    } else if (tableName === 'property') {
      orderBy = 'ORDER BY created_date DESC LIMIT 100'
    }

    const query = `SELECT * FROM ${tableName} ${orderBy}`
    const result = await dbPool.query(query)

    res.json(result.rows)
  } catch (error) {
    console.error(`Erreur lors de la récupération de la table ${req.params.tableName}:`, error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les leads quotidiens pour les clients V1 (ceux qui ne sont PAS en V2)
app.get('/api/leads/v1-daily', async (req, res) => {
  try {
    const cacheKey = 'leads-v1-daily'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const { data: sbData, error: sbErr } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, total_leads')
      .eq('source', 'v1')
      .gte('stat_date', '2024-09-14')
      .order('stat_date', { ascending: false })

    if (!sbErr && sbData && sbData.length > 0) {
      const result = sbData.map(r => ({ date: r.stat_date, total_leads: r.total_leads }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback PostgreSQL
    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })
    const v2ClientIds = await fetchEstimateurAgencies()
    const v2ClientIdsArray = Array.from(v2ClientIds)
    let query; let params: any[]
    if (v2ClientIdsArray.length > 0) {
      const placeholders = v2ClientIdsArray.map((_, index) => `$${index + 1}`).join(',')
      query = `SELECT DATE(p.created_date) as date, COUNT(*) as total_leads
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client NOT IN (${placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) DESC`
      params = v2ClientIdsArray
    } else {
      query = `SELECT DATE(p.created_date) as date, COUNT(*) as total_leads
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) DESC`
      params = []
    }
    const result = await dbPool.query(query, params)
    setCache(cacheKey, result.rows)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des leads quotidiens V1:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les leads quotidiens pour les clients V2
app.get('/api/leads/v2-daily', async (req, res) => {
  try {
    const cacheKey = 'leads-v2-daily'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    // Essayer Supabase
    const { data: sbData, error: sbErr } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, total_leads')
      .eq('source', 'v2')
      .gte('stat_date', '2024-09-14')
      .order('stat_date', { ascending: true })

    if (!sbErr && sbData && sbData.length > 0) {
      const result = sbData.map(r => ({ date: r.stat_date, total_leads: r.total_leads }))
      setCache(cacheKey, result)
      return res.json(result)
    }
    // Fallback PostgreSQL
    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })

    const v2ClientIds = await fetchEstimateurAgencies()
    if (v2ClientIds.size === 0) return res.json([])

    const clientIdsArray = Array.from(v2ClientIds)
    const placeholders = clientIdsArray.map((_, index) => `$${index + 1}`).join(',')
    const result = await dbPool.query(`
      SELECT DATE(p.created_date) as date, COUNT(*) as total_leads
      FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
      WHERE a.id_client IN (${placeholders})
        AND c.demo IS NOT TRUE
          ${slugFilter()}
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) ASC
    `, clientIdsArray)

    setCache(cacheKey, result.rows)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des leads quotidiens V2:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les statistiques téléphone quotidiennes pour les clients V2
app.get('/api/leads/v2-daily-phone', async (req, res) => {
  try {
    const cacheKey = 'leads-v2-daily-phone'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const { data: sbData, error: sbErr } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, leads_with_phone, leads_with_validated_phone')
      .eq('source', 'v2')
      .gte('stat_date', '2024-09-14')
      .order('stat_date', { ascending: true })

    if (!sbErr && sbData && sbData.length > 0) {
      const result = sbData.map(r => ({ date: r.stat_date, leads_with_phone: r.leads_with_phone, leads_with_validated_phone: r.leads_with_validated_phone }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback PostgreSQL
    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })
    const v2ClientIds = await fetchEstimateurAgencies()
    if (v2ClientIds.size === 0) return res.json([])
    const clientIdsArray = Array.from(v2ClientIds)
    const placeholders = clientIdsArray.map((_, index) => `$${index + 1}`).join(',')
    const result = await dbPool.query(`
      SELECT DATE(p.created_date) as date,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
      FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
      WHERE a.id_client IN (${placeholders})
        AND c.demo IS NOT TRUE
          ${slugFilter()}
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) ASC
    `, clientIdsArray)
    setCache(cacheKey, result.rows)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats téléphone V2:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// ─── BDD V2 (MySQL bien_immobilier) — daily leads ───
// Onglet "BDD V2" de la page leads. Mesure ce qui est effectivement arrivé dans
// bien_immobilier (le backoffice intermédiaire entre estimateur et V3).
// Permet de comparer avec V3 pour détecter les pertes de sync.
app.get('/api/leads/bdd-v2-daily', async (req, res) => {
  try {
    const cacheKey = 'leads-bdd-v2-daily'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const pool = getMysqlPool()
    // Vendeurs = tout sauf "Pas de projet de Vente" et "Projet de location"
    // Source estimateur = estimation_id non vide
    const [rows] = await pool.query(`
      SELECT DATE(date_acquisition) as date, COUNT(*) as total_leads
      FROM bien_immobilier
      WHERE date_acquisition >= '2024-09-14'
        AND estimation_id IS NOT NULL AND estimation_id != ''
        AND vente_prevue IS NOT NULL
        AND vente_prevue NOT IN ('Pas de projet de Vente', 'Projet de location', '')
      GROUP BY DATE(date_acquisition)
      ORDER BY DATE(date_acquisition) ASC
    `) as any
    const result = rows.map((r: any) => ({
      date: typeof r.date === 'string' ? r.date.slice(0, 10) : new Date(r.date).toISOString().slice(0, 10),
      total_leads: Number(r.total_leads) || 0,
    }))
    setCache(cacheKey, result)
    res.json(result)
  } catch (error) {
    console.error('Erreur lors de la récupération BDD V2 daily:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// BDD V2 daily phone stats — autant que possible depuis bien_immobilier
// Note: bien_immobilier a sms_envoye (envoyé) mais pas de champ explicite "validated phone".
// On retourne le même format que v2-daily-phone pour compatibilité avec le frontend.
app.get('/api/leads/bdd-v2-daily-phone', async (req, res) => {
  try {
    const cacheKey = 'leads-bdd-v2-daily-phone'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const pool = getMysqlPool()
    // Proxy pour "leads_with_phone" = rows où contact existe avec un tel (via contact44 ? via vendeur_no_tel = 0 ?)
    // Proxy pour "leads_with_validated_phone" = rows où sms_envoye = 1 (SMS OTP envoyé = tel validé en amont)
    const [rows] = await pool.query(`
      SELECT DATE(date_acquisition) as date,
             SUM(CASE WHEN (vendeur_no_tel = 0 OR vendeur_no_tel IS NULL) THEN 1 ELSE 0 END) as leads_with_phone,
             SUM(CASE WHEN sms_envoye = 1 THEN 1 ELSE 0 END) as leads_with_validated_phone
      FROM bien_immobilier
      WHERE date_acquisition >= '2024-09-14'
        AND estimation_id IS NOT NULL AND estimation_id != ''
        AND vente_prevue IS NOT NULL
        AND vente_prevue NOT IN ('Pas de projet de Vente', 'Projet de location', '')
      GROUP BY DATE(date_acquisition)
      ORDER BY DATE(date_acquisition) ASC
    `) as any
    const result = rows.map((r: any) => ({
      date: typeof r.date === 'string' ? r.date.slice(0, 10) : new Date(r.date).toISOString().slice(0, 10),
      leads_with_phone: Number(r.leads_with_phone) || 0,
      leads_with_validated_phone: Number(r.leads_with_validated_phone) || 0,
    }))
    setCache(cacheKey, result)
    res.json(result)
  } catch (error) {
    console.error('Erreur lors de la récupération BDD V2 daily phone:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les leads quotidiens V2 filtrés par agences
app.get('/api/leads/v2-daily-filtered', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const { agencies } = req.query
    
    if (!agencies || typeof agencies !== 'string') {
      return res.status(400).json({ error: 'Parameter "agencies" is required (comma-separated client IDs)' })
    }

    const agencyIds = agencies.split(',').map(id => id.trim()).filter(id => id.length > 0)
    
    if (agencyIds.length === 0) {
      return res.json([])
    }

    // Fenêtre temporelle: depuis le 14 septembre
    const whereDate = `DATE(p.created_date) >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE`

    const placeholders = agencyIds.map((_, index) => `$${index + 1}`).join(',')
    const query = `
      SELECT
        DATE(p.created_date) as date,
        COUNT(DISTINCT p.id_property)::integer as total_leads
      FROM property p
      INNER JOIN agency a ON p.id_agency = a.id
      INNER JOIN client c ON a.id_client = c.id_client
      WHERE a.id_client::text IN (${placeholders})
        AND c.demo IS NOT TRUE
          ${slugFilter()}
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND ${whereDate}
      GROUP BY DATE(p.created_date)
      ORDER BY DATE(p.created_date) ASC
    `
    
    const result = await dbPool.query(query, agencyIds)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des leads V2 filtrés:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les statistiques téléphone quotidiennes V2 filtrées par agences
app.get('/api/leads/v2-daily-phone-filtered', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const { agencies } = req.query
    
    if (!agencies || typeof agencies !== 'string') {
      return res.status(400).json({ error: 'Parameter "agencies" is required (comma-separated client IDs)' })
    }

    const agencyIds = agencies.split(',').map(id => id.trim()).filter(id => id.length > 0)
    
    if (agencyIds.length === 0) {
      return res.json([])
    }

    const placeholders = agencyIds.map((_, index) => `$${index + 1}`).join(',')
    const query = `
      SELECT
        DATE(p.created_date) as date,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
      FROM property p
      LEFT JOIN agency a ON p.id_agency = a.id
      LEFT JOIN client c ON a.id_client = c.id_client
      WHERE a.id_client::text IN (${placeholders})
        AND c.demo IS NOT TRUE
          ${slugFilter()}
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND p.created_date >= '2024-09-14'
        AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date)
      ORDER BY DATE(p.created_date) DESC
    `

    const result = await dbPool.query(query, agencyIds)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats téléphone V2 filtrées:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les clients sélectionnés pour le test A/B
// Chemin vers le fichier JSON pour stocker les clients sélectionnés
const SELECTED_CLIENTS_FILE = path.join(process.cwd(), 'leads-test-ab-selected-clients.json')

app.get('/api/leads-test-ab/selected-clients', async (req, res) => {
  try {
    // Lire le fichier JSON
    try {
      const fileContent = await fs.readFile(SELECTED_CLIENTS_FILE, 'utf-8')
      const data = JSON.parse(fileContent)
      const clientIds = Array.isArray(data.selected_client_ids) ? data.selected_client_ids : []
      console.log('✅ Clients sélectionnés récupérés depuis JSON:', clientIds)
      res.json(clientIds)
    } catch (readError: any) {
      // Si le fichier n'existe pas, retourner un tableau vide
      if (readError.code === 'ENOENT') {
        console.log('ℹ️ Fichier JSON inexistant, retour d\'un tableau vide')
        return res.json([])
      }
      throw readError
    }
  } catch (error: any) {
    console.error('❌ Erreur lors de la récupération des clients sélectionnés:', error)
    res.status(500).json({ 
      error: 'Internal server error',
      details: error.message 
    })
  }
})

// Sauvegarder les clients sélectionnés pour le test A/B
app.post('/api/leads-test-ab/selected-clients', async (req, res) => {
  try {
    const { clientIds } = req.body

    if (!Array.isArray(clientIds)) {
      return res.status(400).json({ error: 'clientIds must be an array' })
    }

    // S'assurer que tous les IDs sont des strings
    const sanitizedClientIds = clientIds.map(id => String(id)).filter(id => id.length > 0)
    console.log('📝 Sauvegarde des clients sélectionnés dans JSON:', sanitizedClientIds)

    // Créer l'objet à sauvegarder
    const dataToSave = {
      selected_client_ids: sanitizedClientIds,
      updated_at: new Date().toISOString()
    }

    // Écrire dans le fichier JSON
    await fs.writeFile(
      SELECTED_CLIENTS_FILE,
      JSON.stringify(dataToSave, null, 2),
      'utf-8'
    )

    console.log('✅ Sauvegarde réussie dans le fichier JSON')
    res.json({ success: true, clientIds: sanitizedClientIds })
  } catch (error: any) {
    console.error('❌ Erreur lors de la sauvegarde des clients sélectionnés:', error)
    res.status(500).json({ 
      error: 'Internal server error',
      details: error.message 
    })
  }
})

// Initialiser la table daily_remark (à exécuter une seule fois)
app.get('/api/init-remarks-table', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS daily_remark (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL UNIQUE,
        remark TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_daily_remark_date ON daily_remark(date);
    `

    await dbPool.query(createTableQuery)

    res.json({ success: true, message: 'Table daily_remark créée avec succès' })
  } catch (error: any) {
    console.error('Erreur lors de la création de la table daily_remark:', error)
    res.status(500).json({
      error: 'Erreur lors de la création de la table',
      details: error.message
    })
  }
})

// Récupérer toutes les remarques depuis Supabase
app.get('/api/remarks', async (req, res) => {
  try {
    const { data, error } = await supabaseAdmin
      .from('daily_remark')
      .select('date, remark')
      .order('date', { ascending: false })

    if (error) {
      console.error('Erreur Supabase lors de la récupération des remarques:', error)
      return res.status(500).json({ error: 'Database error' })
    }

    // Convertir en format { "2024-09-14": "remarque", ... }
    const remarksMap: { [key: string]: string } = {}
    data?.forEach(row => {
      const dateStr = row.date.split('T')[0] // YYYY-MM-DD
      remarksMap[dateStr] = row.remark
    })

    res.json(remarksMap)
  } catch (error) {
    console.error('Erreur lors de la récupération des remarques:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Sauvegarder ou mettre à jour une remarque dans Supabase
app.post('/api/remarks', async (req, res) => {
  try {
    const { date, remark } = req.body

    if (!date) {
      return res.status(400).json({ error: 'Date is required' })
    }

    // Vérifier si une remarque existe déjà pour cette date
    const { data: existing, error: selectError } = await supabaseAdmin
      .from('daily_remark')
      .select('id')
      .eq('date', date)
      .single()

    if (selectError && selectError.code !== 'PGRST116') {
      // PGRST116 = pas de résultat (c'est normal si la remarque n'existe pas encore)
      console.error('Erreur lors de la recherche de la remarque:', selectError)
      return res.status(500).json({ error: 'Database error' })
    }

    let result
    if (existing?.id) {
      // Mettre à jour la remarque existante
      const { data, error } = await supabaseAdmin
        .from('daily_remark')
        .update({ 
          remark: remark || null,
          updated_at: new Date().toISOString()
        })
        .eq('id', existing.id)
        .select()

      if (error) {
        console.error('Erreur Supabase lors de la mise à jour de la remarque:', error)
        return res.status(500).json({ error: 'Database error' })
      }
      result = data
    } else {
      // Créer une nouvelle remarque
      const { data, error } = await supabaseAdmin
        .from('daily_remark')
        .insert({ 
          date, 
          remark: remark || null,
          updated_at: new Date().toISOString()
        })
        .select()

      if (error) {
        console.error('Erreur Supabase lors de la création de la remarque:', error)
        return res.status(500).json({ error: 'Database error' })
      }
      result = data
    }

    res.json({ success: true, data: result?.[0] })
  } catch (error) {
    console.error('Erreur lors de la sauvegarde de la remarque:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Proxy pour l'API Estimateur (éviter les problèmes CORS)
app.get('/api/estimateur/agencies', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any>('estimateur-agencies')
    if (cached) {
      return res.json(cached)
    }

    if (!ESTIMATEUR_API_URL || !ESTIMATEUR_API_TOKEN) {
      console.warn('ESTIMATEUR_API_URL/TOKEN manquant(s) - renvoi vide pour ne pas bloquer le front')
      return res.status(200).json({ success: false, data: [] })
    }

    const response = await fetch(`${ESTIMATEUR_API_URL}/agences/list`, {
      headers: {
        'Authorization': `Bearer ${ESTIMATEUR_API_TOKEN}`,
        'Content-Type': 'application/json'
      }
    })

    if (!response.ok) {
      const body = await response.text().catch(() => '')
      console.error('Erreur API Estimateur:', response.status, body)
      return res.status(200).json({ success: false, data: [] })
    }

    const data = await response.json()
    let result: any
    if (Array.isArray(data)) {
      result = { success: true, data }
    } else if (data && Array.isArray(data.data)) {
      result = { success: true, data: data.data }
    } else {
      result = { success: true, data: [] }
    }
    setCache('estimateur-agencies', result, CACHE_TTL_LONG_MS)
    return res.json(result)
  } catch (error) {
    console.error('Erreur lors de la récupération des agences estimateur:', error)
    // Ne pas casser le front
    res.status(200).json({ success: false, data: [] })
  }
})

// Récupérer les visiteurs quotidiens depuis GA4 V2
app.get('/api/ga4/daily-visitors-v2', async (req, res) => {
  console.log('🔍 [GA4] Appel endpoint /api/ga4/daily-visitors-v2')
  try {
    // Essayer Supabase d'abord
    const cacheKey = 'ga4-daily-visitors-v2-supabase'
    const cached = getCached<any[]>(cacheKey)
    if (cached) return res.json(cached)

    const { data: sbData, error: sbError } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, ga4_visitors')
      .eq('source', 'v2')
      .gte('stat_date', '2024-09-14')
      .gt('ga4_visitors', 0)
      .order('stat_date', { ascending: false })

    if (!sbError && sbData?.length) {
      const result = sbData.map(r => ({ date: r.stat_date, visitors: r.ga4_visitors }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback: GA4 file cache
    const data = await getDailyVisitorsV2()
    console.log(`🔍 [GA4] Données retournées: ${data.length} entrées`)
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération des visiteurs GA4 V2:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les visiteurs quotidiens V2 filtrés par clients (via agences)
app.get('/api/ga4/daily-visitors-v2-filtered', async (req, res) => {
  try {
    const { clients } = req.query
    
    if (!clients || typeof clients !== 'string') {
      return res.status(400).json({ error: 'Parameter "clients" is required (comma-separated client IDs)' })
    }

    const clientIds = clients.split(',').map(id => id.trim()).filter(id => id.length > 0)
    
    if (clientIds.length === 0) {
      return res.json([])
    }

    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    // Récupérer le rewrited_name depuis la table estimator pour les clients sélectionnés
    console.log(`🔍 [API] Recherche des rewrited_name pour les clients:`, clientIds)
    const query = `
      SELECT DISTINCT e.rewrited_name
      FROM estimator e
      WHERE e.id_client = ANY($1::uuid[])
        AND e.rewrited_name IS NOT NULL
        AND e.rewrited_name != ''
    `
    
    let result
    try {
      result = await dbPool.query(query, [clientIds])
      console.log(`✅ [API] Requête SQL réussie, ${result.rows.length} rewrited_name trouvés`)
      if (result.rows.length > 0) {
        console.log(`📊 [API] Premiers rewrited_name:`, result.rows.slice(0, 5).map((r: any) => r.rewrited_name))
      }
    } catch (error: any) {
      console.error('❌ [API] Erreur lors de la requête SQL pour récupérer rewrited_name:', error)
      return res.status(500).json({ 
        error: 'Erreur lors de la récupération des rewrited_name',
        details: error.message 
      })
    }
    
    // Extraire les rewrites des estimateurs
    const agencyRewrites: string[] = []
    result.rows.forEach((row: any) => {
      if (row.rewrited_name) {
        // Utiliser le rewrite tel quel (sans ajouter "agence-" car il est peut-être déjà dans rewrited_name)
        // Ou peut-être que l'URL dans GA4 est juste ?agence=123webimmo sans le préfixe
        agencyRewrites.push(row.rewrited_name)
      }
    })

    if (agencyRewrites.length === 0) {
      console.warn(`⚠️ [API] Aucun rewrite trouvé pour les clients sélectionnés: ${clientIds.join(', ')}`)
      return res.json([])
    }

    console.log(`🔍 [API] Récupération des visiteurs GA4 pour ${agencyRewrites.length} agences`)
    console.log(`📊 [API] Rewrites finaux:`, agencyRewrites)

    // Récupérer les visiteurs filtrés par agences
    const { getDailyVisitorsV2ByAgencies } = await import('./ga4-analytics.js')
    let data
    try {
      data = await getDailyVisitorsV2ByAgencies(agencyRewrites)
      console.log(`✅ ${data.length} jours de données GA4 filtrées récupérées pour les agences sélectionnées`)
    } catch (error: any) {
      console.error('❌ Erreur lors de la récupération des visiteurs GA4 filtrés:', error)
      return res.status(500).json({ 
        error: 'Erreur lors de la récupération des visiteurs GA4 filtrés',
        details: error.message 
      })
    }
    
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération des visiteurs GA4 V2 filtrés:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les visiteurs quotidiens depuis GA4 V1
app.get('/api/ga4/daily-visitors-v1', async (req, res) => {
  try {
    // Essayer Supabase d'abord
    const cacheKey = 'ga4-daily-visitors-v1-supabase'
    const cached = getCached<any[]>(cacheKey)
    if (cached) return res.json(cached)

    const { data: sbData, error: sbError } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, ga4_visitors')
      .eq('source', 'v1')
      .gte('stat_date', '2024-09-14')
      .gt('ga4_visitors', 0)
      .order('stat_date', { ascending: false })

    if (!sbError && sbData?.length) {
      const result = sbData.map(r => ({ date: r.stat_date, visitors: r.ga4_visitors }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback: GA4 file cache
    const data = await getDailyVisitorsV1()
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération des visiteurs GA4 V1:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les visiteurs quotidiens depuis GA4 ES (valorar-vivienda.es)
app.get('/api/ga4/daily-visitors-es', async (req, res) => {
  try {
    const cacheKey = 'ga4-daily-visitors-es-supabase'
    const cached = getCached<any[]>(cacheKey)
    if (cached) return res.json(cached)

    const { data: sbData, error: sbError } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, ga4_visitors')
      .eq('source', 'es')
      .gte('stat_date', '2024-09-14')
      .gt('ga4_visitors', 0)
      .order('stat_date', { ascending: false })

    if (!sbError && sbData?.length) {
      const result = sbData.map(r => ({ date: r.stat_date, visitors: r.ga4_visitors }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback: GA4 direct
    const data = await getDailyVisitorsEs()
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération des visiteurs GA4 ES:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Visiteurs temps réel du jour (runRealtimeReport + runReport today)
app.get('/api/ga4/today-visitors', async (req, res) => {
  try {
    const cacheKey = 'ga4-today-visitors'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const data = await getTodayVisitors()
    setCache(cacheKey, data, 2 * 60 * 1000) // Cache 2 minutes
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération des visiteurs temps réel:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Série quotidienne HP → typologie (pour mesurer l'impact de la validation d'adresse)
app.get('/api/ga4/daily-hp-to-typology', async (req, res) => {
  try {
    const cacheKey = 'ga4-daily-hp-to-typology'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const data = await getDailyFunnelHpToTypology()
    setCache(cacheKey, data)
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération HP→typologie:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Tunnel de conversion estimateur (FR ou ES)
app.get('/api/ga4/conversion-funnel', async (req, res) => {
  try {
    const country = (req.query.country as string) === 'es' ? 'es' : 'fr'
    const startDate = req.query.startDate as string | undefined
    const endDate = req.query.endDate as string | undefined
    const data = await getConversionFunnel(country, startDate, endDate)
    res.json(data)
  } catch (error) {
    console.error('Erreur funnel:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Statistiques des leads contactés par agence (BDD V3)
// Un lead contacté = property avec phone != null ET reminders_info_done_all > 0
// Retourne les stats par nom d'agence (normalisé) pour pouvoir mapper avec l'API Territory
app.get('/api/agency-contact-stats', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any[]>('agency-contact-stats')
    if (cached) {
      return res.json(cached)
    }

    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    // Récupérer les stats par nom de client (pour mapper avec l'API Territory via le nom)
    const query = `
      SELECT
        LOWER(TRIM(c.name)) as agency_name_normalized,
        c.name as agency_name,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone IS NOT NULL AND (
          p.reminders_info_done_all > 0
          OR p.status IN ('contacted', 'signed', 'refused', 'sold')
          OR (p.signed_status IS NOT NULL AND p.signed_status != 'no' AND p.signed_status != '')
          OR (p.refusal IS NOT NULL AND p.refusal != '' AND p.refusal != 'no')
        ) THEN 1 END)::integer as leads_contacted,
        COUNT(CASE WHEN p.phone IS NOT NULL AND p.reminders_info_to_do_all > 0 THEN 1 END)::integer as leads_with_reminder,
        COALESCE(AVG(CASE WHEN p.phone IS NOT NULL THEN p.reminders_info_done_all END), 0) as avg_reminders_done,
        COUNT(CASE WHEN p.signed_status != 'no' AND p.signed_status != '' AND p.origin = 'estimator' THEN 1 END)::integer as mandats_signed
      FROM client c
      LEFT JOIN agency a ON a.id_client = c.id_client
      LEFT JOIN property p ON p.id_agency = a.id
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
      GROUP BY c.name
    `

    const result = await dbPool.query(query)

    // Calculer le pourcentage pour chaque agence
    const stats = result.rows.map((row: any) => ({
      agency_name: row.agency_name,
      agency_name_normalized: row.agency_name_normalized,
      leads_with_phone: row.leads_with_phone || 0,
      leads_contacted: row.leads_contacted || 0,
      leads_with_reminder: row.leads_with_reminder || 0,
      avg_reminders_done: row.avg_reminders_done ? Math.round(parseFloat(row.avg_reminders_done) * 10) / 10 : 0,
      mandats_signed: row.mandats_signed || 0,
      contact_rate: row.leads_with_phone > 0
        ? Math.round((row.leads_contacted / row.leads_with_phone) * 100 * 10) / 10
        : 0,
      reminder_rate: row.leads_with_phone > 0
        ? Math.round((row.leads_with_reminder / row.leads_with_phone) * 100 * 10) / 10
        : 0
    }))

    setCache('agency-contact-stats', stats, CACHE_TTL_MS)
    res.json(stats)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats de contact:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Proxy pour l'API Territory V2 (éviter les problèmes CORS)
app.get('/api/v2/agencies', async (req, res) => {
  try {
    // Vérifier le cache (données stables, TTL long)
    const cached = getCached<any>('v2-agencies')
    if (cached) {
      return res.json(cached)
    }

    const response = await fetch('https://back-api.maline-immobilier.fr/territory/api/agences', {
      headers: {
        'x-api-key': '70c51af056cccd8a1fa1434be9fddfa4a0e86929e5b65055db844f38ba4b3fce'
      }
    })

    if (!response.ok) {
      throw new Error('Erreur API V2')
    }

    const data = await response.json()
    setCache('v2-agencies', data, CACHE_TTL_LONG_MS)
    res.json(data)
  } catch (error) {
    console.error('Erreur lors de la récupération des agences V2:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Agences ignorées (persistance Supabase)
app.get('/api/ignored-agencies', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any[]>('ignored-agencies')
    if (cached) {
      return res.json(cached)
    }

    const { data, error } = await supabaseAdmin
      .from('ignored_agency')
      .select('agency_id')

    if (error) {
      console.error('Erreur Supabase (get ignored):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    setCache('ignored-agencies', data || [], CACHE_TTL_LONG_MS)
    res.json(data || [])
  } catch (error) {
    console.error('Erreur lors de GET /ignored-agencies:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.post('/api/ignored-agencies', async (req, res) => {
  try {
    const { agency_ids } = req.body || {}
    if (!Array.isArray(agency_ids) || agency_ids.length === 0) {
      return res.status(400).json({ error: 'agency_ids array is required' })
    }

    const rows = agency_ids.map((id: any) => ({ agency_id: String(id) }))

    const { data, error } = await supabaseAdmin
      .from('ignored_agency')
      .upsert(rows, { onConflict: 'agency_id' })
      .select('agency_id')

    if (error) {
      console.error('Erreur Supabase (post ignored):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('ignored-agencies')
    res.json({ success: true, data: data || [] })
  } catch (error) {
    console.error('Erreur lors de POST /ignored-agencies:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.delete('/api/ignored-agencies', async (req, res) => {
  try {
    const { agency_ids } = req.body || {}
    if (!Array.isArray(agency_ids) || agency_ids.length === 0) {
      return res.status(400).json({ error: 'agency_ids array is required' })
    }

    const { error } = await supabaseAdmin
      .from('ignored_agency')
      .delete()
      .in('agency_id', agency_ids.map((id: any) => String(id)))

    if (error) {
      console.error('Erreur Supabase (delete ignored):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('ignored-agencies')
    res.json({ success: true })
  } catch (error) {
    console.error('Erreur lors de DELETE /ignored-agencies:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Overrides manuels de matching pub → client
app.get('/api/ad-matching-overrides', async (req, res) => {
  try {
    const cached = getCached<any[]>('ad-matching-overrides')
    if (cached) {
      return res.json(cached)
    }

    const { data, error } = await supabaseAdmin
      .from('ad_matching_overrides')
      .select('normalized_name, id_client')

    if (error) {
      console.error('Erreur Supabase (get ad-matching-overrides):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    setCache('ad-matching-overrides', data || [], CACHE_TTL_LONG_MS)
    res.json(data || [])
  } catch (error) {
    console.error('Erreur lors de GET /ad-matching-overrides:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.post('/api/ad-matching-overrides', async (req, res) => {
  try {
    const { normalized_name, id_client } = req.body || {}
    if (!normalized_name || !id_client) {
      return res.status(400).json({ error: 'normalized_name and id_client are required' })
    }

    const { data, error } = await supabaseAdmin
      .from('ad_matching_overrides')
      .upsert({ normalized_name: String(normalized_name), id_client: String(id_client) }, { onConflict: 'normalized_name' })
      .select('normalized_name, id_client')

    if (error) {
      console.error('Erreur Supabase (post ad-matching-overrides):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('ad-matching-overrides')
    res.json({ success: true, data: data || [] })
  } catch (error) {
    console.error('Erreur lors de POST /ad-matching-overrides:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.delete('/api/ad-matching-overrides', async (req, res) => {
  try {
    const { normalized_name } = req.body || {}
    if (!normalized_name) {
      return res.status(400).json({ error: 'normalized_name is required' })
    }

    const { error } = await supabaseAdmin
      .from('ad_matching_overrides')
      .delete()
      .eq('normalized_name', String(normalized_name))

    if (error) {
      console.error('Erreur Supabase (delete ad-matching-overrides):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('ad-matching-overrides')
    res.json({ success: true })
  } catch (error) {
    console.error('Erreur lors de DELETE /ad-matching-overrides:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Endpoints pour les agences suspendues
app.get('/api/suspended-agencies', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any[]>('suspended-agencies')
    if (cached) {
      return res.json(cached)
    }

    const { data, error } = await supabaseAdmin
      .from('suspended_agency')
      .select('agency_id')

    if (error) {
      console.error('Erreur Supabase (get suspended):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    setCache('suspended-agencies', data || [], CACHE_TTL_LONG_MS)
    res.json(data || [])
  } catch (error) {
    console.error('Erreur lors de GET /suspended-agencies:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.post('/api/suspended-agencies', async (req, res) => {
  try {
    const { agency_ids } = req.body || {}
    if (!Array.isArray(agency_ids) || agency_ids.length === 0) {
      return res.status(400).json({ error: 'agency_ids array is required' })
    }

    const rows = agency_ids.map((id: any) => ({ agency_id: String(id) }))

    const { data, error } = await supabaseAdmin
      .from('suspended_agency')
      .upsert(rows, { onConflict: 'agency_id' })
      .select('agency_id')

    if (error) {
      console.error('Erreur Supabase (post suspended):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('suspended-agencies')
    res.json({ success: true, data: data || [] })
  } catch (error) {
    console.error('Erreur lors de POST /suspended-agencies:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.delete('/api/suspended-agencies', async (req, res) => {
  try {
    const { agency_ids } = req.body || {}
    if (!Array.isArray(agency_ids) || agency_ids.length === 0) {
      return res.status(400).json({ error: 'agency_ids array is required' })
    }

    const { error } = await supabaseAdmin
      .from('suspended_agency')
      .delete()
      .in('agency_id', agency_ids.map((id: any) => String(id)))

    if (error) {
      console.error('Erreur Supabase (delete suspended):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('suspended-agencies')
    res.json({ success: true })
  } catch (error) {
    console.error('Erreur lors de DELETE /suspended-agencies:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Endpoints pour les dates de fin manuelles des agences
app.get('/api/agency-end-dates', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any[]>('agency-end-dates')
    if (cached) {
      return res.json(cached)
    }

    const { data, error } = await supabaseAdmin
      .from('agency_end_date')
      .select('agency_id,end_date')

    if (error) {
      console.error('Erreur Supabase (get agency-end-dates):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    setCache('agency-end-dates', data || [], CACHE_TTL_LONG_MS)
    res.json(data || [])
  } catch (error) {
    console.error('Erreur lors de GET /agency-end-dates:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.post('/api/agency-end-dates', async (req, res) => {
  try {
    const { items } = req.body || {}
    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'items array is required' })
    }

    const rows = items.map((it: any) => ({
      agency_id: String(it.agency_id),
      end_date: it.end_date, // attendu: YYYY-MM-DD
    }))

    const { data, error } = await supabaseAdmin
      .from('agency_end_date')
      .upsert(rows, { onConflict: 'agency_id' })
      .select('agency_id,end_date')

    if (error) {
      console.error('Erreur Supabase (post agency-end-dates):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('agency-end-dates')
    res.json({ success: true, data: data || [] })
  } catch (error) {
    console.error('Erreur lors de POST /agency-end-dates:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

app.delete('/api/agency-end-dates', async (req, res) => {
  try {
    const { agency_ids } = req.body || {}
    if (!Array.isArray(agency_ids) || agency_ids.length === 0) {
      return res.status(400).json({ error: 'agency_ids array is required' })
    }

    const { error } = await supabaseAdmin
      .from('agency_end_date')
      .delete()
      .in('agency_id', agency_ids.map((id: any) => String(id)))

    if (error) {
      console.error('Erreur Supabase (delete agency-end-dates):', error)
      return res.status(500).json({ error: 'Database error' })
    }

    invalidateCache('agency-end-dates')
    res.json({ success: true })
  } catch (error) {
    console.error('Erreur lors de DELETE /agency-end-dates:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Récupérer les statistiques téléphone quotidiennes pour les clients V1
app.get('/api/leads/v1-daily-phone', async (req, res) => {
  try {
    const cacheKey = 'leads-v1-daily-phone'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const { data: sbData, error: sbErr } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('stat_date, leads_with_phone, leads_with_validated_phone')
      .eq('source', 'v1')
      .gte('stat_date', '2024-09-14')
      .order('stat_date', { ascending: false })

    if (!sbErr && sbData && sbData.length > 0) {
      const result = sbData.map(r => ({ date: r.stat_date, leads_with_phone: r.leads_with_phone, leads_with_validated_phone: r.leads_with_validated_phone }))
      setCache(cacheKey, result)
      return res.json(result)
    }

    // Fallback PostgreSQL
    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })
    const v2ClientIds = await fetchEstimateurAgencies()
    const v2ClientIdsArray = Array.from(v2ClientIds)
    let query; let params: any[]
    if (v2ClientIdsArray.length > 0) {
      const placeholders = v2ClientIdsArray.map((_, index) => `$${index + 1}`).join(',')
      query = `SELECT DATE(p.created_date) as date,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client NOT IN (${placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) DESC`
      params = v2ClientIdsArray
    } else {
      query = `SELECT DATE(p.created_date) as date,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date) ORDER BY DATE(p.created_date) DESC`
      params = []
    }
    const result = await dbPool.query(query, params)
    setCache(cacheKey, result.rows)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats téléphone V1:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Endpoint temporaire pour lister les locales distinctes des clients
app.get('/api/debug/client-locales', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }
    const query = `SELECT DISTINCT locale, COUNT(*) as count FROM client GROUP BY locale ORDER BY count DESC`
    const result = await dbPool.query(query)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Mapping id_gocardless → locale pour enrichir les données V2
app.get('/api/client-locales', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }
    const cached = getCached<any[]>('client-locales')
    if (cached) {
      return res.json(cached)
    }
    const result = await dbPool.query(`
      SELECT
        CASE
          WHEN id_gocardless IS NOT NULL AND jsonb_typeof(id_gocardless::jsonb) = 'array'
          THEN id_gocardless::jsonb->>0
          ELSE id_gocardless::text
        END as id_gocardless,
        name,
        locale
      FROM client
      WHERE id_gocardless IS NOT NULL
    `)
    const rows = result.rows.map((r: any) => ({
      id_gocardless: r.id_gocardless?.replace(/"/g, '') || null,
      name: r.name,
      locale: r.locale || 'fr_FR'
    }))
    setCache('client-locales', rows, CACHE_TTL_LONG_MS)
    res.json(rows)
  } catch (error) {
    console.error('Erreur lors de GET /client-locales:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Endpoint pour lister les origines des leads
app.get('/api/debug/property-origins', async (req, res) => {
  try {
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }
    const query = `SELECT DISTINCT origin, COUNT(*) as count FROM property GROUP BY origin ORDER BY count DESC`
    const result = await dbPool.query(query)
    res.json(result.rows)
  } catch (error) {
    console.error('Erreur:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// ============== API PUBLIQUE STATS AGENCE ==============
// Endpoint accessible pour récupérer les stats d'une agence
// Paramètres de recherche (par ordre de priorité) : num_gcl, email_cu, nom_agence
app.get('/api/public/agency-stats', async (req, res) => {
  try {
    const { num_gcl, email_cu, nom_agence } = req.query

    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    if (!num_gcl && !email_cu && !nom_agence) {
      return res.status(400).json({
        error: 'Au moins un paramètre de recherche est requis',
        params: {
          num_gcl: 'Numéro GoCardLess du client',
          email_cu: 'Email de l\'agence (compte utilisateur)',
          nom_agence: 'Nom de l\'agence ou du client'
        }
      })
    }

    let clientId: string | null = null
    let clientGocardless: string | null = null
    let agencyInfo: { name: string; email?: string } | null = null

    // 1. Recherche par numéro GoCardLess (priorité 1)
    if (num_gcl && !clientId) {
      const gclQuery = `
        SELECT id_client, name, id_gocardless
        FROM client
        WHERE id_gocardless IS NOT NULL
          AND id_gocardless::text LIKE $1
      `
      const gclResult = await dbPool.query(gclQuery, [`%${num_gcl}%`])
      if (gclResult.rows.length > 0) {
        clientId = gclResult.rows[0].id_client
        clientGocardless = gclResult.rows[0].id_gocardless
        agencyInfo = { name: gclResult.rows[0].name }
        console.log(`📍 Agence trouvée par num_gcl: ${agencyInfo.name}`)
      }
    }

    // 2. Recherche par email CU (priorité 2)
    if (email_cu && !clientId) {
      const emailQuery = `
        SELECT a.id_client, c.name, a.email, c.id_gocardless
        FROM agency a
        JOIN client c ON c.id_client = a.id_client
        WHERE LOWER(a.email) = LOWER($1)
      `
      const emailResult = await dbPool.query(emailQuery, [email_cu])
      if (emailResult.rows.length > 0) {
        clientId = emailResult.rows[0].id_client
        clientGocardless = emailResult.rows[0].id_gocardless
        agencyInfo = { name: emailResult.rows[0].name, email: emailResult.rows[0].email }
        console.log(`📍 Agence trouvée par email_cu: ${agencyInfo.name}`)
      }
    }

    // 3. Recherche par nom d'agence (priorité 3)
    if (nom_agence && !clientId) {
      const nameQuery = `
        SELECT id_client, name, id_gocardless
        FROM client
        WHERE LOWER(name) LIKE LOWER($1)
      `
      const nameResult = await dbPool.query(nameQuery, [`%${nom_agence}%`])
      if (nameResult.rows.length > 0) {
        clientId = nameResult.rows[0].id_client
        clientGocardless = nameResult.rows[0].id_gocardless
        agencyInfo = { name: nameResult.rows[0].name }
        console.log(`📍 Agence trouvée par nom_agence: ${agencyInfo.name}`)
      }
    }

    // Si aucune agence trouvée
    if (!clientId) {
      return res.status(404).json({
        error: 'Agence non trouvée',
        recherche: {
          num_gcl: num_gcl || null,
          email_cu: email_cu || null,
          nom_agence: nom_agence || null
        }
      })
    }

    // Récupérer les stats pour cette agence
    const statsQuery = `
      SELECT
        COUNT(p.id_property)::integer as nb_leads_all,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as nb_leads_total,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as nb_leads_tel_valide,
        COUNT(CASE WHEN p.phone IS NOT NULL AND (
          p.reminders_info_done_all > 0
          OR p.status IN ('contacted', 'signed', 'refused', 'sold')
          OR (p.signed_status IS NOT NULL AND p.signed_status != 'no' AND p.signed_status != '')
          OR (p.refusal IS NOT NULL AND p.refusal != '' AND p.refusal != 'no')
        ) THEN 1 END)::integer as leads_contacted,
        COUNT(CASE WHEN p.phone IS NOT NULL AND p.reminders_info_to_do_all > 0 THEN 1 END)::integer as leads_with_reminder,
        COALESCE(AVG(CASE WHEN p.phone IS NOT NULL THEN p.reminders_info_done_all END), 0) as avg_reminders_done,
        COUNT(CASE WHEN p.signed_status != 'no' AND p.signed_status != '' AND p.origin = 'estimator' THEN 1 END)::integer as mandats_signed
      FROM agency a
      JOIN property p ON p.id_agency = a.id
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
      WHERE a.id_client = $1
    `
    const statsResult = await dbPool.query(statsQuery, [clientId])
    const stats = statsResult.rows[0]

    // Calculer les pourcentages
    const nbLeadsAll = stats.nb_leads_all || 0
    const nbLeadsTotal = stats.nb_leads_total || 0
    const nbLeadsTelValide = stats.nb_leads_tel_valide || 0
    const leadsContacted = stats.leads_contacted || 0
    const leadsWithReminder = stats.leads_with_reminder || 0
    const avgRemindersDone = stats.avg_reminders_done ? Math.round(parseFloat(stats.avg_reminders_done) * 10) / 10 : 0
    const mandatsSigned = stats.mandats_signed || 0

    const pctLeadContacte = nbLeadsTelValide > 0 ? Math.round((leadsContacted / nbLeadsTelValide) * 100 * 10) / 10 : 0
    const pctRelancePrevu = nbLeadsTelValide > 0 ? Math.round((leadsWithReminder / nbLeadsTelValide) * 100 * 10) / 10 : 0

    // Récupérer les codes postaux de la zone via V2
    let nbLeadsTelValideZone = 0
    let nbLeadsZoneTel = 0
    let nbLeadsZoneAll = 0
    let zonePostalCodes: string[] = []
    let nombreLogements: number | null = null
    try {
      const v2Data = await fetchV2AgenciesData()
      const gclStr = clientGocardless ? String(clientGocardless).replace(/"/g, '').trim() : undefined
      const v2Agency = findV2Agency(
        { id_gocardless: gclStr, client_name: agencyInfo?.name },
        v2Data
      )
      nombreLogements = v2Agency?.nombre_logements ?? null
      if (v2Agency?.tarifs) {
        const codes: string[] = []
        for (const tarif of v2Agency.tarifs) {
          if (tarif.code_postal) {
            codes.push(...tarif.code_postal.split(',').map((c: string) => c.trim()).filter((c: string) => /^\d{5}$/.test(c) && c !== '00000'))
          }
        }
        zonePostalCodes = [...new Set(codes)]
      }
      if (zonePostalCodes.length > 0) {
        const zoneQuery = `
          SELECT
            COUNT(p.id_property)::integer as nb_leads_zone_all,
            COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as nb_leads_zone_tel,
            COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as nb_tel_valide_zone
          FROM agency a
          JOIN property p ON p.id_agency = a.id
          WHERE a.id_client = $1
            AND p.postal_code = ANY($2)
            AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
            AND p.origin = 'estimator'
        `
        const zoneResult = await dbPool.query(zoneQuery, [clientId, zonePostalCodes])
        nbLeadsTelValideZone = zoneResult.rows[0]?.nb_tel_valide_zone || 0
        nbLeadsZoneTel = zoneResult.rows[0]?.nb_leads_zone_tel || 0
        nbLeadsZoneAll = zoneResult.rows[0]?.nb_leads_zone_all || 0
      }
    } catch (e) {
      console.error('Erreur récupération zone V2:', e)
    }

    res.json({
      agence: agencyInfo?.name || 'Inconnu',
      stats: {
        nb_leads_all: nbLeadsAll,
        nb_leads_total: nbLeadsTotal,
        nb_leads_tel_valide: nbLeadsTelValide,
        nb_leads_tel_valide_zone: nbLeadsTelValideZone,
        nb_leads_zone_tel: nbLeadsZoneTel,
        nb_leads_zone_all: nbLeadsZoneAll,
        pct_lead_contacte: pctLeadContacte,
        pct_relance_prevu: pctRelancePrevu,
        nb_relance_moy: avgRemindersDone,
        mandats_signes: mandatsSigned
      },
      nombre_logements_zone: nombreLogements,
      zone_postal_codes: zonePostalCodes,
      recherche: {
        methode: num_gcl && clientId ? 'num_gcl' : email_cu && clientId ? 'email_cu' : 'nom_agence',
        num_gcl: num_gcl || null,
        email_cu: email_cu || null,
        nom_agence: nom_agence || null
      }
    })
  } catch (error) {
    console.error('Erreur lors de la récupération des stats agence:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// ============== STATISTIQUES DE RÉACTIVITÉ ==============

// Helper: calculer la date de Pâques pour une année donnée (algorithme de Meeus/Jones/Butcher)
function getEasterDate(year: number): Date {
  const a = year % 19
  const b = Math.floor(year / 100)
  const c = year % 100
  const d = Math.floor(b / 4)
  const e = b % 4
  const f = Math.floor((b + 8) / 25)
  const g = Math.floor((b - f + 1) / 3)
  const h = (19 * a + b - d - g + 15) % 30
  const i = Math.floor(c / 4)
  const k = c % 4
  const l = (32 + 2 * e + 2 * i - h - k) % 7
  const m = Math.floor((a + 11 * h + 22 * l) / 451)
  const month = Math.floor((h + l - 7 * m + 114) / 31)
  const day = ((h + l - 7 * m + 114) % 31) + 1
  return new Date(year, month - 1, day)
}

// Helper: obtenir les jours fériés français pour une année donnée
function getFrenchHolidays(year: number): Set<string> {
  const holidays = new Set<string>()

  // Jours fériés fixes
  holidays.add(`${year}-01-01`) // Jour de l'An
  holidays.add(`${year}-05-01`) // Fête du Travail
  holidays.add(`${year}-05-08`) // Victoire 1945
  holidays.add(`${year}-07-14`) // Fête Nationale
  holidays.add(`${year}-08-15`) // Assomption
  holidays.add(`${year}-11-01`) // Toussaint
  holidays.add(`${year}-11-11`) // Armistice
  holidays.add(`${year}-12-25`) // Noël

  // Jours fériés mobiles (basés sur Pâques)
  const easter = getEasterDate(year)

  // Lundi de Pâques (lendemain de Pâques)
  const easterMonday = new Date(easter)
  easterMonday.setDate(easter.getDate() + 1)
  holidays.add(easterMonday.toISOString().split('T')[0])

  // Ascension (39 jours après Pâques)
  const ascension = new Date(easter)
  ascension.setDate(easter.getDate() + 39)
  holidays.add(ascension.toISOString().split('T')[0])

  // Lundi de Pentecôte (50 jours après Pâques)
  const pentecostMonday = new Date(easter)
  pentecostMonday.setDate(easter.getDate() + 50)
  holidays.add(pentecostMonday.toISOString().split('T')[0])

  return holidays
}

// Cache des jours fériés par année
const holidaysCache = new Map<number, Set<string>>()

function isHoliday(date: Date): boolean {
  const year = date.getFullYear()
  if (!holidaysCache.has(year)) {
    holidaysCache.set(year, getFrenchHolidays(year))
  }
  const dateStr = date.toISOString().split('T')[0]
  return holidaysCache.get(year)!.has(dateStr)
}

function isBusinessDay(date: Date): boolean {
  const dayOfWeek = date.getDay()
  // 0 = dimanche, 6 = samedi
  if (dayOfWeek === 0 || dayOfWeek === 6) return false
  if (isHoliday(date)) return false
  return true
}

// Calculer les heures ouvrées entre deux dates (exclut weekends et jours fériés)
function calculateBusinessHours(startDate: Date, endDate: Date): number {
  if (endDate <= startDate) return 0

  let totalHours = 0
  const currentDate = new Date(startDate)

  // Parcourir chaque jour
  while (currentDate < endDate) {
    const nextDay = new Date(currentDate)
    nextDay.setDate(nextDay.getDate() + 1)
    nextDay.setHours(0, 0, 0, 0)

    if (isBusinessDay(currentDate)) {
      // Calculer les heures pour ce jour ouvré
      const dayEnd = nextDay < endDate ? nextDay : endDate
      const hoursThisDay = (dayEnd.getTime() - currentDate.getTime()) / (1000 * 60 * 60)
      totalHours += hoursThisDay
    }

    // Passer au jour suivant à minuit
    currentDate.setTime(nextDay.getTime())
  }

  return totalHours
}

// Helper: récupérer les clients actifs (non résiliés) avec leurs dates de début depuis l'API Territory V2
interface ActiveClientInfo {
  gcl: string
  startDate: Date | null
  name: string
}

async function getActiveClientsWithStartDates(): Promise<Map<string, ActiveClientInfo>> {
  const startKeys = [
    'date_start', 'startDate', 'start_at', 'startAt', 'startedAt',
    'subscriptionStart', 'subscription.startDate', 'subscription.start_at',
    'activationDate', 'activatedAt', 'activated_at',
    'createdAt', 'created_at', 'dateStart', 'dateDebut', 'date_debut', 'debut'
  ]

  const endKeys = [
    'endDate', 'end_at', 'endAt', 'endedAt',
    'subscriptionEnd', 'subscription.endDate', 'subscription.end_at',
    'deactivationDate', 'deactivatedAt', 'canceledAt', 'cancelledAt',
    'closedAt', 'dateEnd', 'dateFin', 'date_fin', 'resiliationDate', 'fin'
  ]

  try {
    const agencies = await fetchAgenciesV2Raw()
    const activeClients = new Map<string, ActiveClientInfo>()

    const now = new Date()
    for (const agency of agencies) {
      // Vérifier si l'agence a une date de fin
      const endDate = pickDate(agency, endKeys)
      if (!endDate || endDate > now) {
        // Pas de date de fin ou date de fin dans le futur = client actif
        const gcl = agency.id_gocardless?.replace(/[^\x20-\x7E]/g, '').trim()
        if (gcl) {
          const startDate = pickDate(agency, startKeys)
          // Exclure les clients dont la date de démarrage est dans le futur
          if (startDate && startDate > now) continue
          const name = agency.nom || agency.name || ''
          activeClients.set(gcl, { gcl, startDate, name })
        }
      }
    }

    console.log(`📊 Réactivité: ${activeClients.size} clients actifs trouvés`)
    return activeClients
  } catch (error) {
    console.error('Erreur récupération clients actifs:', error)
    return new Map()
  }
}

// Statistiques de réactivité du traitement des leads par agence (clients actifs uniquement, heures ouvrées)
app.get('/api/reactivity-stats', async (req, res) => {
  try {
    // Vérifier le cache
    const cached = getCached<any>('reactivity-stats')
    if (cached) {
      return res.json(cached)
    }

    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    // Récupérer les clients actifs avec leurs dates de début
    const activeClients = await getActiveClientsWithStartDates()

    if (activeClients.size === 0) {
      return res.json({
        global: { total_leads_with_action: 0 },
        per_agency: [],
        distribution: [],
        by_action_type: [],
        monthly_trend: [],
        active_clients_count: 0,
        updated_at: new Date().toISOString()
      })
    }

    // Convertir en tableau pour la requête SQL
    const activeGclArray = Array.from(activeClients.keys())

    // Récupérer toutes les paires (created_date, first_action_date) pour calcul heures ouvrées
    console.log(`📊 Réactivité: Requête SQL avec ${activeGclArray.length} clients actifs`)
    const rawDataQuery = `
      WITH first_actions AS (
        SELECT
          p.id_property,
          c.id_client,
          c.name as client_name,
          CASE
            WHEN c.id_gocardless IS NOT NULL AND c.id_gocardless::text ~ '^\\['
            THEN TRIM(BOTH '"' FROM (c.id_gocardless::text::jsonb->>0))
            ELSE REPLACE(REPLACE(c.id_gocardless::text, '"', ''), '''', '')
          END as id_gocardless,
          p.created_date,
          MIN(r.processed_date) as first_action_date,
          MIN(r.reminder_type) as first_action_type
        FROM property p
        JOIN reminder r ON r.property_id = p.id_property
        JOIN client c ON c.id_client = p.id_client
        WHERE p.phone IS NOT NULL
          AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
          AND p.archived = false
          AND r.processed_date IS NOT NULL
          AND c.id_gocardless IS NOT NULL
          AND (
            REPLACE(REPLACE(c.id_gocardless::text, '"', ''), '''', '') = ANY($1::text[])
            OR (c.id_gocardless::text ~ '^\\[' AND TRIM(BOTH '"' FROM (c.id_gocardless::text::jsonb->>0)) = ANY($1::text[]))
          )
        GROUP BY p.id_property, c.id_client, c.name, c.id_gocardless::text, p.created_date
      )
      SELECT * FROM first_actions
    `

    let rawDataResult
    try {
      rawDataResult = await dbPool.query(rawDataQuery, [activeGclArray])
      console.log(`📊 Réactivité: ${rawDataResult.rows.length} leads trouvés`)
    } catch (sqlError) {
      console.error('❌ Erreur SQL réactivité:', sqlError)
      throw sqlError
    }

    // Calculer les heures ouvrées pour chaque lead
    interface LeadData {
      id_property: string
      id_client: string
      client_name: string
      id_gocardless: string
      created_date: Date
      first_action_date: Date
      first_action_type: string
      business_hours: number
    }

    const leadsWithBusinessHours: LeadData[] = rawDataResult.rows.map((row: any) => {
      const createdDate = new Date(row.created_date)
      const firstActionDate = new Date(row.first_action_date)
      const businessHours = calculateBusinessHours(createdDate, firstActionDate)
      return {
        id_property: row.id_property,
        id_client: row.id_client,
        client_name: row.client_name,
        id_gocardless: row.id_gocardless || '',
        created_date: createdDate,
        first_action_date: firstActionDate,
        first_action_type: row.first_action_type,
        business_hours: businessHours
      }
    })

    // 1. Stats globales
    const totalLeads = leadsWithBusinessHours.length
    const allHours = leadsWithBusinessHours.map(l => l.business_hours)
    const avgHours = totalLeads > 0 ? allHours.reduce((a, b) => a + b, 0) / totalLeads : 0

    // Médiane
    const sortedHours = [...allHours].sort((a, b) => a - b)
    const medianHours = totalLeads > 0
      ? (totalLeads % 2 === 0
        ? (sortedHours[totalLeads / 2 - 1] + sortedHours[totalLeads / 2]) / 2
        : sortedHours[Math.floor(totalLeads / 2)])
      : 0

    // Comptages par seuils (en heures ouvrées)
    const within1h = leadsWithBusinessHours.filter(l => l.business_hours <= 1).length
    const within2h = leadsWithBusinessHours.filter(l => l.business_hours <= 2).length
    const within24h = leadsWithBusinessHours.filter(l => l.business_hours <= 24).length
    const within48h = leadsWithBusinessHours.filter(l => l.business_hours <= 48).length
    const within72h = leadsWithBusinessHours.filter(l => l.business_hours <= 72).length

    // 2. Stats par agence
    const agencyMap = new Map<string, { name: string; hours: number[]; id_gocardless: string }>()
    for (const lead of leadsWithBusinessHours) {
      if (!agencyMap.has(lead.id_client)) {
        agencyMap.set(lead.id_client, { name: lead.client_name, hours: [], id_gocardless: lead.id_gocardless })
      }
      agencyMap.get(lead.id_client)!.hours.push(lead.business_hours)
    }

    // Créer un index inversé pour rechercher par nom de client (fallback)
    const activeClientsByName = new Map<string, ActiveClientInfo>()
    for (const [gcl, info] of activeClients.entries()) {
      if (info.name) {
        const normalizedName = info.name.toLowerCase().trim()
        activeClientsByName.set(normalizedName, info)
      }
    }

    const perAgencyStats = Array.from(agencyMap.entries())
      .filter(([_, data]) => data.hours.length >= 5)
      .map(([id_client, data]) => {
        const hours = data.hours
        const count = hours.length
        const avg = hours.reduce((a, b) => a + b, 0) / count

        // Récupérer la date de début d'abonnement depuis les infos V2
        // Essayer d'abord par id_gocardless, puis par nom
        let clientInfo = activeClients.get(data.id_gocardless)
        if (!clientInfo && data.name) {
          const normalizedName = data.name.toLowerCase().trim()
          clientInfo = activeClientsByName.get(normalizedName)
        }
        const subscriptionStartDate = clientInfo?.startDate?.toISOString() || null

        return {
          id_client,
          client_name: data.name,
          subscription_start_date: subscriptionStartDate,
          leads_with_action: count,
          avg_hours_to_first_action: Math.round(avg * 100) / 100,
          contacted_within_1h: hours.filter(h => h <= 1).length,
          contacted_within_2h: hours.filter(h => h <= 2).length,
          contacted_within_24h: hours.filter(h => h <= 24).length,
          contacted_within_48h: hours.filter(h => h <= 48).length,
          contacted_within_72h: hours.filter(h => h <= 72).length,
          pct_within_24h: Math.round((hours.filter(h => h <= 24).length / count) * 100 * 10) / 10,
          pct_within_48h: Math.round((hours.filter(h => h <= 48).length / count) * 100 * 10) / 10
        }
      })
      .sort((a, b) => a.avg_hours_to_first_action - b.avg_hours_to_first_action)

    // 3. Distribution des temps de réponse (heures ouvrées)
    const buckets = [
      { label: '< 1h', min: 0, max: 1, order: 1 },
      { label: '1-2h', min: 1, max: 2, order: 2 },
      { label: '2-4h', min: 2, max: 4, order: 3 },
      { label: '4-8h', min: 4, max: 8, order: 4 },
      { label: '8-24h', min: 8, max: 24, order: 5 },
      { label: '24-48h', min: 24, max: 48, order: 6 },
      { label: '48-72h', min: 48, max: 72, order: 7 },
      { label: '> 72h', min: 72, max: Infinity, order: 8 }
    ]

    const distribution = buckets.map(bucket => ({
      bucket: bucket.label,
      count: leadsWithBusinessHours.filter(l =>
        l.business_hours >= bucket.min && l.business_hours < bucket.max
      ).length
    }))

    // 4. Stats par type d'action
    const actionTypeMap = new Map<string, number[]>()
    for (const lead of leadsWithBusinessHours) {
      const type = lead.first_action_type || 'unknown'
      if (!actionTypeMap.has(type)) {
        actionTypeMap.set(type, [])
      }
      actionTypeMap.get(type)!.push(lead.business_hours)
    }

    const byActionType = Array.from(actionTypeMap.entries())
      .map(([type, hours]) => ({
        type,
        leads_count: hours.length,
        avg_hours: Math.round((hours.reduce((a, b) => a + b, 0) / hours.length) * 100) / 100
      }))
      .sort((a, b) => b.leads_count - a.leads_count)

    // 5. Évolution mensuelle (12 derniers mois)
    const twelveMonthsAgo = new Date()
    twelveMonthsAgo.setMonth(twelveMonthsAgo.getMonth() - 12)

    const recentLeads = leadsWithBusinessHours.filter(l => l.created_date >= twelveMonthsAgo)
    const monthlyMap = new Map<string, number[]>()

    for (const lead of recentLeads) {
      const month = lead.created_date.toISOString().substring(0, 7) // YYYY-MM
      if (!monthlyMap.has(month)) {
        monthlyMap.set(month, [])
      }
      monthlyMap.get(month)!.push(lead.business_hours)
    }

    const monthlyTrend = Array.from(monthlyMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([month, hours]) => {
        const count = hours.length
        const avg = hours.reduce((a, b) => a + b, 0) / count
        const within24 = hours.filter(h => h <= 24).length
        return {
          month,
          total_leads: count,
          avg_hours: Math.round(avg * 100) / 100,
          within_24h: within24,
          pct_within_24h: Math.round((within24 / count) * 100 * 10) / 10
        }
      })

    const result = {
      global: {
        total_leads_with_action: totalLeads,
        avg_hours_to_first_action: Math.round(avgHours * 100) / 100,
        median_hours_to_first_action: Math.round(medianHours * 100) / 100,
        contacted_within_1h: within1h,
        contacted_within_2h: within2h,
        contacted_within_24h: within24h,
        contacted_within_48h: within48h,
        contacted_within_72h: within72h,
        pct_within_1h: totalLeads > 0 ? Math.round((within1h / totalLeads) * 100 * 10) / 10 : 0,
        pct_within_24h: totalLeads > 0 ? Math.round((within24h / totalLeads) * 100 * 10) / 10 : 0,
        pct_within_48h: totalLeads > 0 ? Math.round((within48h / totalLeads) * 100 * 10) / 10 : 0,
        pct_within_72h: totalLeads > 0 ? Math.round((within72h / totalLeads) * 100 * 10) / 10 : 0
      },
      per_agency: perAgencyStats,
      distribution,
      by_action_type: byActionType,
      monthly_trend: monthlyTrend,
      active_clients_count: activeClients.size,
      updated_at: new Date().toISOString()
    }

    setCache('reactivity-stats', result, CACHE_TTL_MS)
    res.json(result)
  } catch (error: any) {
    console.error('❌ Erreur stats réactivité:', error?.message || error)
    if (error?.stack) console.error(error.stack)
    res.status(500).json({ error: 'Internal server error', details: error?.message })
  }
})

// ============== GESTION DES FACTURES (Gmail) ==============

// Vérifier si Gmail est authentifié
app.get('/api/invoices/auth/status', async (req, res) => {
  try {
    const authenticated = await isAuthenticated()
    res.json({ authenticated })
  } catch (error) {
    console.error('Erreur vérification auth Gmail:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Obtenir l'URL d'authentification Gmail
app.get('/api/invoices/auth/url', async (req, res) => {
  try {
    const authUrl = await getAuthUrl()
    if (!authUrl) {
      return res.status(500).json({
        error: 'Impossible de générer l\'URL d\'authentification. Vérifiez que le fichier gmail-credentials.json existe.'
      })
    }
    res.json({ authUrl })
  } catch (error) {
    console.error('Erreur génération URL auth:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Callback OAuth2 - échanger le code contre un token
app.get('/api/invoices/auth/callback', async (req, res) => {
  try {
    const { code, scope } = req.query
    if (!code || typeof code !== 'string') {
      return res.status(400).json({ error: 'Code d\'autorisation manquant' })
    }

    // Si le scope contient adwords, c'est un code Google Ads → rediriger vers l'exchange dédié
    if (typeof scope === 'string' && scope.includes('adwords')) {
      return res.redirect(`/api/google-ads/exchange?code=${encodeURIComponent(code)}`)
    }

    const success = await exchangeCodeForToken(code)

    // En développement, rediriger vers le port Vite (5173)
    // En production, utiliser le même host
    const isDev = process.env.NODE_ENV === 'development' || (!process.env.NODE_ENV && process.env.npm_lifecycle_event === 'dev')
    const baseUrl = isDev ? 'http://localhost:5173' : ''

    if (success) {
      res.redirect(`${baseUrl}/factures?auth=success`)
    } else {
      res.redirect(`${baseUrl}/factures?auth=error`)
    }
  } catch (error) {
    console.error('Erreur callback OAuth:', error)
    const isDev2 = process.env.NODE_ENV === 'development' || (!process.env.NODE_ENV && process.env.npm_lifecycle_event === 'dev')
    const baseUrl = isDev2 ? 'http://localhost:5173' : ''
    res.redirect(`${baseUrl}/factures?auth=error`)
  }
})

// Récupérer la liste des factures groupées
app.get('/api/invoices', async (req, res) => {
  try {
    const authenticated = await isAuthenticated()
    if (!authenticated) {
      return res.status(401).json({
        error: 'Gmail non authentifié',
        needsAuth: true
      })
    }

    const forceRefresh = req.query.refresh === 'true'
    const invoices = await fetchInvoices(forceRefresh)
    const grouped = groupInvoices(invoices)

    // Calculer quelques stats
    const stats = {
      totalInvoices: invoices.length,
      totalAttachments: invoices.reduce((sum, inv) => sum + inv.attachments.length, 0),
      providers: [...new Set(invoices.map(inv => inv.senderName))].length,
      years: [...new Set(invoices.map(inv => inv.year))].sort((a, b) => b - a)
    }

    res.json({ invoices, grouped, stats })
  } catch (error: any) {
    console.error('Erreur récupération factures:', error)
    if (error.message === 'Gmail non authentifié') {
      return res.status(401).json({ error: error.message, needsAuth: true })
    }
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Télécharger une pièce jointe spécifique
// ?firstPageOnly=true pour ne garder que la première page
app.get('/api/invoices/download/:messageId/:attachmentId', async (req, res) => {
  try {
    const { messageId, attachmentId } = req.params
    const firstPageOnly = req.query.firstPageOnly === 'true'

    const result = await downloadAttachment(messageId, attachmentId)
    if (!result) {
      return res.status(404).json({ error: 'Pièce jointe non trouvée' })
    }

    let pdfData = result.data
    let filename = result.filename

    // Extraire seulement la première page si demandé
    if (firstPageOnly) {
      pdfData = await extractFirstPage(result.data)
      // Ajouter un suffixe au nom du fichier
      const ext = path.extname(filename)
      const baseName = path.basename(filename, ext)
      filename = `${baseName}_page1${ext}`
    }

    res.setHeader('Content-Type', 'application/pdf')
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`)
    res.send(pdfData)
  } catch (error: any) {
    console.error('Erreur téléchargement pièce jointe:', error)
    if (error.message === 'Gmail non authentifié') {
      return res.status(401).json({ error: error.message, needsAuth: true })
    }
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Télécharger toutes les factures en ZIP (filtrage par année/mois/prestataire)
app.post('/api/invoices/download-zip', async (req, res) => {
  try {
    const { year, month, provider, invoiceIds, firstPageOnly } = req.body

    const allInvoices = await fetchInvoices()
    let filteredInvoices: InvoiceData[] = allInvoices

    // Filtrer par IDs spécifiques si fournis
    if (invoiceIds && Array.isArray(invoiceIds) && invoiceIds.length > 0) {
      filteredInvoices = allInvoices.filter(inv => invoiceIds.includes(inv.id))
    } else {
      // Sinon filtrer par année/mois/prestataire
      if (year) {
        filteredInvoices = filteredInvoices.filter(inv => inv.year === parseInt(year))
      }
      if (month) {
        filteredInvoices = filteredInvoices.filter(inv => inv.month === parseInt(month))
      }
      if (provider) {
        filteredInvoices = filteredInvoices.filter(inv => inv.senderName === provider)
      }
    }

    if (filteredInvoices.length === 0) {
      return res.status(404).json({ error: 'Aucune facture correspondante' })
    }

    const zipBuffer = await downloadInvoicesAsZip(filteredInvoices, firstPageOnly === true)

    // Construire le nom du fichier
    let filename = 'factures'
    if (year) filename += `_${year}`
    if (month) filename += `_${String(month).padStart(2, '0')}`
    if (provider) filename += `_${provider.replace(/[^a-zA-Z0-9]/g, '_')}`
    filename += '.zip'

    res.setHeader('Content-Type', 'application/zip')
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`)
    res.send(zipBuffer)
  } catch (error: any) {
    console.error('Erreur création ZIP:', error)
    if (error.message === 'Gmail non authentifié') {
      return res.status(401).json({ error: error.message, needsAuth: true })
    }
    res.status(500).json({ error: 'Internal server error' })
  }
})

// ============== OUTILS PDF ==============

// Upload de PDF(s) et extraction de la première page
app.post('/api/pdf/extract-first-page', express.raw({ type: 'application/pdf', limit: '50mb' }), async (req, res) => {
  try {
    if (!req.body || req.body.length === 0) {
      return res.status(400).json({ error: 'Aucun fichier PDF fourni' })
    }

    const pdfBuffer = Buffer.from(req.body)
    const resultBuffer = await extractFirstPage(pdfBuffer)

    res.setHeader('Content-Type', 'application/pdf')
    res.setHeader('Content-Disposition', 'attachment; filename="extracted_page1.pdf"')
    res.send(resultBuffer)
  } catch (error: any) {
    console.error('Erreur extraction PDF:', error)
    res.status(500).json({ error: 'Erreur lors de l\'extraction de la première page' })
  }
})

// Upload multiple PDFs via multipart/form-data et extraction en ZIP
app.post('/api/pdf/extract-first-pages-zip', express.json(), async (req, res) => {
  try {
    const { files } = req.body // Array of { name: string, data: string (base64) }

    if (!files || !Array.isArray(files) || files.length === 0) {
      return res.status(400).json({ error: 'Aucun fichier fourni' })
    }

    // Import dynamique de archiver
    const archiver = (await import('archiver')).default

    const archive = archiver('zip', { zlib: { level: 9 } })
    const chunks: Buffer[] = []

    archive.on('data', (chunk) => chunks.push(chunk))

    const archiveFinished = new Promise<Buffer>((resolve, reject) => {
      archive.on('end', () => resolve(Buffer.concat(chunks)))
      archive.on('error', reject)
    })

    for (const file of files) {
      if (!file.name || !file.data) continue

      try {
        // Décoder le base64
        const pdfBuffer = Buffer.from(file.data, 'base64')
        const extractedBuffer = await extractFirstPage(pdfBuffer)

        // Ajouter un suffixe au nom
        const ext = path.extname(file.name)
        const baseName = path.basename(file.name, ext)
        const newName = `${baseName}_page1${ext}`

        archive.append(extractedBuffer, { name: newName })
      } catch (err) {
        console.error(`Erreur extraction ${file.name}:`, err)
      }
    }

    archive.finalize()

    const zipBuffer = await archiveFinished

    res.setHeader('Content-Type', 'application/zip')
    res.setHeader('Content-Disposition', 'attachment; filename="pdfs_page1.zip"')
    res.send(zipBuffer)
  } catch (error: any) {
    console.error('Erreur extraction PDFs:', error)
    res.status(500).json({ error: 'Erreur lors de l\'extraction' })
  }
})

// Rafraîchir le cache des factures
app.post('/api/invoices/refresh', async (req, res) => {
  try {
    invalidateInvoicesCache()
    const invoices = await fetchInvoices(true)
    const grouped = groupInvoices(invoices)

    res.json({
      success: true,
      count: invoices.length,
      grouped
    })
  } catch (error: any) {
    console.error('Erreur rafraîchissement factures:', error)
    if (error.message === 'Gmail non authentifié') {
      return res.status(401).json({ error: error.message, needsAuth: true })
    }
    res.status(500).json({ error: 'Internal server error' })
  }
})

// ============== API GESTION PUB - ANALYSE PERFORMANCE PUBLICITAIRE ==============

// Interface pour les stats pub par client
interface PubStatsRow {
  id_client: string
  client_name: string
  nb_leads: number
  nb_leads_with_phone: number
  nb_leads_validated_phone: number
  property_types: { [key: string]: number }
  postal_codes: string[]
  zone_size: number
  avg_property_type_apartment: number
  avg_property_type_house: number
}

// --- Ads Stats (Meta + Google) ---

interface AdsClientData {
  meta_spend: number
  meta_impressions: number
  meta_clicks: number
  meta_leads: number
  google_spend: number
  google_impressions: number
  google_clicks: number
  google_leads: number
}

// Normaliser le nom d'adset/adgroup pour le matching client
function normalizeAdName(name: string): string {
  let cleaned = name
    // Préfixes Google PMax : "PMax:", "Pmax :", "PMAX :", "Leads-Performance Max-"
    .replace(/^p\s*max\s*:\s*/i, '')
    .replace(/^leads-performance\s+max-\d*\s*/i, '')
    // Suffixes courants : " - Copie", " - copie 2", " - New Estim", "Ad Group", etc.
    .replace(/\s*-\s*(copie|copy)\s*\d*/gi, '')
    .replace(/\s*-\s*new estim/gi, '')
    .replace(/\s*ad\s*group$/gi, '')
    .replace(/\s*ensemble de publicités$/gi, '')
    // Suffixes Google : " (Smart)", "?"
    .replace(/\s*\(smart\)\s*$/gi, '')
    .replace(/^\?/, '')
    .trim()
  return normalizeAgencyName(cleaned)
}

interface AdMetrics { spend: number; impressions: number; clicks: number; leads: number; account_ids?: string[] }

interface DailyAdRow {
  source: 'meta' | 'google'
  ad_name_raw: string
  normalized_name: string
  stat_date: string // YYYY-MM-DD
  spend: number
  impressions: number
  clicks: number
  leads: number
  account_id: string | null
}

function formatDateYMD(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

// Renouveler automatiquement un token Meta long-lived si bientôt expiré
// --- Auto-refresh Google Ads token ---
// Google refresh tokens expirent en 7j quand l'app OAuth est non-vérifiée (External).
// On teste proactivement le token, on stocke sa date de création, et on alerte avant expiration.
async function refreshGoogleAdsTokenIfNeeded(): Promise<void> {
  reloadGoogleAdsEnv()
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
  if (!clientId || !clientSecret || !refreshToken) return

  try {
    // 1. Tester le refresh token en demandant un access token
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
    })
    const tokenData = await tokenRes.json() as any

    if (!tokenData.access_token) {
      // Token expiré ou révoqué
      const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${clientId}&redirect_uri=http://localhost:9007&response_type=code&scope=https://www.googleapis.com/auth/adwords&access_type=offline&prompt=consent`
      console.error(`🚨🚨🚨 [google-ads] REFRESH TOKEN EXPIRÉ OU INVALIDE !`)
      console.error(`🚨 [google-ads] Erreur: ${tokenData.error || JSON.stringify(tokenData)}`)
      console.error(`🚨 [google-ads] → Regénérer via: ${authUrl}`)
      return
    }

    // 2. Vérifier l'âge du token via GOOGLE_ADS_TOKEN_GENERATED_AT
    const generatedAt = process.env.GOOGLE_ADS_TOKEN_GENERATED_AT
    if (generatedAt) {
      const genDate = new Date(generatedAt)
      const now = new Date()
      const daysElapsed = (now.getTime() - genDate.getTime()) / (1000 * 60 * 60 * 24)
      const daysLeft = 7 - daysElapsed

      if (daysLeft <= 0) {
        console.warn(`⚠️ [google-ads] Token a dépassé 7 jours (${Math.round(daysElapsed)}j) — il fonctionne encore mais pourrait expirer bientôt`)
      } else if (daysLeft <= 2) {
        const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${clientId}&redirect_uri=http://localhost:9007&response_type=code&scope=https://www.googleapis.com/auth/adwords&access_type=offline&prompt=consent`
        console.warn(`🚨 [google-ads] Token expire dans ~${Math.round(daysLeft * 24)}h ! Regénérer MAINTENANT :`)
        console.warn(`🚨 [google-ads] → ${authUrl}`)
      } else {
        console.log(`✅ [google-ads] Refresh token OK, ~${Math.round(daysLeft)} jours restants`)
      }
    } else {
      // Pas de date connue → stocker maintenant comme point de départ
      console.log(`✅ [google-ads] Refresh token OK (date de génération inconnue, enregistrement…)`)
      try {
        let envContent = await fs.readFile(ENV_FILE_PATH, 'utf-8')
        const ts = new Date().toISOString()
        if (envContent.includes('GOOGLE_ADS_TOKEN_GENERATED_AT=')) {
          envContent = envContent.replace(/^GOOGLE_ADS_TOKEN_GENERATED_AT=.*/m, `GOOGLE_ADS_TOKEN_GENERATED_AT=${ts}`)
        } else {
          envContent += `\nGOOGLE_ADS_TOKEN_GENERATED_AT=${ts}\n`
        }
        await fs.writeFile(ENV_FILE_PATH, envContent, 'utf-8')
        process.env.GOOGLE_ADS_TOKEN_GENERATED_AT = ts
        console.log(`💾 [google-ads] GOOGLE_ADS_TOKEN_GENERATED_AT=${ts} sauvegardé dans .env`)
      } catch (writeErr) {
        console.warn(`⚠️ [google-ads] Impossible de sauvegarder la date dans .env:`, (writeErr as Error).message)
      }
    }
  } catch (e) {
    console.warn('⚠️ [google-ads] Erreur vérification token:', (e as Error).message)
  }
}

async function refreshMetaTokenIfNeeded(): Promise<void> {
  const token = process.env.META_ADS_TOKEN_1
  const appId = process.env.META_ADS_APP_ID
  const appSecret = process.env.META_ADS_APP_SECRET
  if (!token || !appId || !appSecret) return

  try {
    const debugRes = await fetch(`https://graph.facebook.com/v21.0/debug_token?input_token=${token}&access_token=${token}`)
    const debugData = await debugRes.json() as any
    if (!debugData.data) return

    const expiresAt = debugData.data.expires_at
    if (expiresAt === 0) return // permanent token

    const now = Math.floor(Date.now() / 1000)
    const daysLeft = (expiresAt - now) / 86400

    if (daysLeft < 0) {
      console.warn(`⚠️ [meta] Token 1 EXPIRÉ depuis ${Math.abs(Math.round(daysLeft))} jours`)
      return
    }

    if (daysLeft < 7) {
      console.log(`🔄 [meta] Token expire dans ${Math.round(daysLeft)} jours, renouvellement...`)
      const exchangeRes = await fetch(`https://graph.facebook.com/v21.0/oauth/access_token?grant_type=fb_exchange_token&client_id=${appId}&client_secret=${appSecret}&fb_exchange_token=${token}`)
      const exchangeData = await exchangeRes.json() as any
      if (exchangeData.access_token) {
        const newToken = exchangeData.access_token
        process.env.META_ADS_TOKEN_1 = newToken
        const newDays = Math.round((exchangeData.expires_in || 0) / 86400)
        console.log(`✅ [meta] Token renouvelé, valide ${newDays} jours`)

        // Persister dans .env
        try {
          const envContent = await fs.readFile(ENV_FILE_PATH, 'utf-8')
          const updated = envContent.replace(
            /^META_ADS_TOKEN_1=.*/m,
            `META_ADS_TOKEN_1=${newToken}`
          )
          await fs.writeFile(ENV_FILE_PATH, updated, 'utf-8')
          console.log('💾 [meta] Token sauvegardé dans .env')
        } catch (writeErr) {
          console.warn('⚠️ [meta] Impossible de sauvegarder dans .env:', (writeErr as Error).message)
        }
      }
    } else {
      console.log(`✅ [meta] Token 1 valide encore ${Math.round(daysLeft)} jours`)
    }
  } catch (e) {
    console.warn('⚠️ [meta] Erreur vérification token:', (e as Error).message)
  }
}

async function fetchMetaAdsInsights(period: string): Promise<Map<string, AdMetrics>> {
  const tokens = [
    process.env.META_ADS_TOKEN_1,
    process.env.META_ADS_TOKEN_2,
    process.env.META_ADS_TOKEN_3,
    process.env.META_ADS_TOKEN_4,
    process.env.META_ADS_TOKEN_5
  ].filter(Boolean) as string[]

  if (tokens.length === 0) {
    console.warn('⚠️ [ads] Aucun token Meta configuré')
    return new Map()
  }

  // Calculer time_range selon la période
  const now = new Date()
  let since: string
  if (period === 'month') {
    since = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`
  } else if (period === '30d') {
    const d = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)
    since = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
  } else {
    since = '2020-01-01'
  }
  const until = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
  const timeRange = encodeURIComponent(JSON.stringify({ since, until }))

  const seenAccountIds = new Set<string>()
  const result = new Map<string, AdMetrics>()

  for (const token of tokens) {
    try {
      // 1. Lister les comptes pub
      const accRes = await fetch(`https://graph.facebook.com/v21.0/me/adaccounts?fields=id,name,account_status&limit=100&access_token=${token}`)
      const accData = await accRes.json() as any
      if (!accData.data) continue

      const accounts = (accData.data as any[]).filter((a: any) => a.account_status === 1 && !seenAccountIds.has(a.id))
      for (const acc of accounts) seenAccountIds.add(acc.id)

      // 2. Récupérer les insights par adset pour chaque compte
      for (const acc of accounts) {
        try {
          const url = `https://graph.facebook.com/v21.0/${acc.id}/insights?fields=adset_name,spend,impressions,clicks,actions&level=adset&time_range=${timeRange}&limit=500&access_token=${token}`
          const insRes = await fetch(url)
          const insData = await insRes.json() as any
          if (!insData.data) continue

          for (const row of insData.data) {
            const normalized = normalizeAdName(row.adset_name)
            if (!normalized) continue
            const prev = result.get(normalized) || { spend: 0, impressions: 0, clicks: 0, leads: 0 }
            prev.spend += parseFloat(row.spend || '0')
            prev.impressions += parseInt(row.impressions || '0')
            prev.clicks += parseInt(row.clicks || '0')
            if (row.actions) {
              for (const action of row.actions) {
                if (action.action_type === 'lead' || action.action_type === 'onsite_web_lead') {
                  prev.leads += parseInt(action.value || '0')
                  break
                }
              }
            }
            result.set(normalized, prev)
          }
        } catch (e) {
          console.warn(`⚠️ [ads] Meta insights error for ${acc.id}:`, (e as Error).message)
        }
      }
    } catch (e) {
      console.warn('⚠️ [ads] Meta accounts error:', (e as Error).message)
    }
  }

  return result
}

async function fetchGoogleAdsInsights(period: string): Promise<Map<string, AdMetrics>> {
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
  const customerIds = process.env.GOOGLE_ADS_CUSTOMER_IDS?.split(',').map(s => s.trim()).filter(Boolean)

  if (!clientId || !clientSecret || !refreshToken || !devToken || !customerIds?.length) {
    console.warn('⚠️ [ads] Config Google Ads incomplète')
    return new Map()
  }

  // 1. Obtenir un access token
  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
  })
  const tokenData = await tokenRes.json() as any
  if (!tokenData.access_token) {
    console.warn('⚠️ [ads] Google OAuth error:', tokenData)
    return new Map()
  }
  const accessToken = tokenData.access_token

  // 2. Déterminer le filtre de date GAQL
  let dateFilter: string
  if (period === 'month') {
    dateFilter = 'DURING THIS_MONTH'
  } else if (period === '30d') {
    dateFilter = 'DURING LAST_30_DAYS'
  } else {
    dateFilter = 'DURING ALL_TIME'
  }

  const result = new Map<string, AdMetrics>()

  for (const cid of customerIds) {
    try {
      // Requêter au niveau CAMPAIGN (pas ad_group) car beaucoup de PMax n'ont pas de ad groups
      const query = `SELECT campaign.name, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions
                     FROM campaign
                     WHERE campaign.status = 'ENABLED' AND segments.date ${dateFilter} AND metrics.impressions > 0
                     ORDER BY metrics.cost_micros DESC`

      const r = await fetch(`https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'developer-token': devToken,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query })
      })
      const data = await r.json() as any
      if (data.error || !data[0]?.results) continue

      for (const row of data[0].results) {
        const normalized = normalizeAdName(row.campaign.name)
        if (!normalized) continue
        const prev = result.get(normalized) || { spend: 0, impressions: 0, clicks: 0, leads: 0 }
        prev.spend += parseInt(row.metrics.costMicros || '0') / 1e6
        prev.impressions += parseInt(row.metrics.impressions || '0')
        prev.clicks += parseInt(row.metrics.clicks || '0')
        prev.leads += parseFloat(row.metrics.conversions || '0')
        result.set(normalized, prev)
      }
    } catch (e) {
      console.warn(`⚠️ [ads] Google Ads error for ${cid}:`, (e as Error).message)
    }
  }

  return result
}

// --- Fonctions pour récupérer la date de dernière modification des pubs ---

async function fetchMetaLastEdits(): Promise<{ edits: Record<string, string>; creations: Record<string, string>; starts: Record<string, string> }> {
  const tokens = [
    process.env.META_ADS_TOKEN_1,
    process.env.META_ADS_TOKEN_2,
    process.env.META_ADS_TOKEN_3,
    process.env.META_ADS_TOKEN_4,
    process.env.META_ADS_TOKEN_5
  ].filter(Boolean) as string[]

  if (tokens.length === 0) return { edits: {}, creations: {}, starts: {} }

  const seenAccountIds = new Set<string>()
  const result: Record<string, string> = {} // normalized_name → max updated_time ISO
  const creations: Record<string, string> = {} // normalized_name → min created_time ISO
  const starts: Record<string, string> = {} // normalized_name → start_time (date de programmation)

  // Helper : mettre à jour le max updated_time par nom normalisé
  const trackUpdate = (name: string | undefined, updatedTime: string | undefined) => {
    if (!name || !updatedTime) return
    const normalized = normalizeAdName(name)
    if (!normalized) return
    if (!result[normalized] || new Date(updatedTime) > new Date(result[normalized])) {
      result[normalized] = updatedTime
    }
  }

  // Helper : garder le MAX de created_time par nom normalisé
  const trackCreation = (name: string | undefined, createdTime: string | undefined) => {
    if (!name || !createdTime) return
    const normalized = normalizeAdName(name)
    if (!normalized) return
    if (!creations[normalized] || new Date(createdTime) > new Date(creations[normalized])) {
      creations[normalized] = createdTime
    }
  }

  // Helper : garder le start_time (date de programmation de diffusion) par nom normalisé
  const trackStart = (name: string | undefined, startTime: string | undefined) => {
    if (!name || !startTime) return
    const normalized = normalizeAdName(name)
    if (!normalized) return
    // On garde la date de démarrage la plus récente (si plusieurs adsets)
    if (!starts[normalized] || new Date(startTime) > new Date(starts[normalized])) {
      starts[normalized] = startTime
    }
  }

  // Phase 1 : récupérer tous les comptes en parallèle (1 appel par token)
  const allAccounts: { id: string; token: string; name: string }[] = []
  await Promise.all(tokens.map(async (token) => {
    try {
      const accRes = await fetch(`https://graph.facebook.com/v21.0/me/adaccounts?fields=id,name,account_status&limit=100&access_token=${token}`)
      const accData = await accRes.json() as any
      if (!accData.data) return
      for (const a of (accData.data as any[])) {
        if (a.account_status === 1 && !seenAccountIds.has(a.id)) {
          seenAccountIds.add(a.id)
          allAccounts.push({ id: a.id, token, name: a.name || a.id })
        }
      }
    } catch (e) {
      console.warn('⚠️ [ads-last-edit] Meta accounts error:', (e as Error).message)
    }
  }))

  // Phase 2 : récupérer campaigns/adsets/ads de TOUS les comptes en parallèle
  const accounts: Record<string, string> = {} // normalized_name → account name
  const trackAccount = (name: string | undefined, accountName: string) => {
    if (!name) return
    const normalized = normalizeAdName(name)
    if (normalized && !accounts[normalized]) accounts[normalized] = accountName
  }
  const statusFilter = encodeURIComponent('["ACTIVE","PAUSED","ARCHIVED","CAMPAIGN_PAUSED","IN_PROCESS","WITH_ISSUES"]')
  await Promise.all(allAccounts.map(async ({ id: accId, token, name: accName }) => {
    try {
      const [campaignRes, adsetRes, adRes] = await Promise.all([
        fetch(`https://graph.facebook.com/v21.0/${accId}/campaigns?fields=name,updated_time,created_time,start_time&effective_status=${statusFilter}&limit=500&access_token=${token}`),
        fetch(`https://graph.facebook.com/v21.0/${accId}/adsets?fields=name,updated_time,created_time,start_time&effective_status=${statusFilter}&limit=500&access_token=${token}`),
        fetch(`https://graph.facebook.com/v21.0/${accId}/ads?fields=adset_name,updated_time,created_time&effective_status=${statusFilter}&limit=500&access_token=${token}`)
      ])
      const campaignData = await campaignRes.json() as any
      const adsetData = await adsetRes.json() as any
      const adData = await adRes.json() as any

      if (campaignData.data) {
        for (const c of campaignData.data) {
          trackUpdate(c.name, c.updated_time)
          trackStart(c.name, c.start_time)
          trackAccount(c.name, accName)
        }
      }
      if (adsetData.data) {
        for (const a of adsetData.data) {
          trackUpdate(a.name, a.updated_time)
          trackCreation(a.name, a.created_time)
          trackStart(a.name, a.start_time)
          trackAccount(a.name, accName)
        }
      }
      if (adData.data) {
        for (const a of adData.data) {
          trackUpdate(a.adset_name, a.updated_time)
          trackAccount(a.adset_name, accName)
        }
      }
    } catch (e) {
      console.warn(`⚠️ [ads-last-edit] Meta error for ${accId}:`, (e as Error).message)
    }
  }))

  console.log(`✅ [ads-last-edit] Meta: ${Object.keys(result).length} noms avec date de modif, ${Object.keys(creations).length} avec date de création, ${Object.keys(starts).length} avec date de démarrage, ${Object.keys(accounts).length} avec compte`)
  return { edits: result, creations, starts, accounts }
}

async function fetchGoogleLastEdits(): Promise<{ edits: Record<string, string>; creations: Record<string, string>; accounts: Record<string, string> }> {
  reloadGoogleAdsEnv()
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
  const customerIds = process.env.GOOGLE_ADS_CUSTOMER_IDS?.split(',').map(s => s.trim()).filter(Boolean)

  if (!clientId || !clientSecret || !refreshToken || !devToken || !customerIds?.length) return { edits: {}, creations: {}, accounts: {} }

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
  })
  const tokenData = await tokenRes.json() as any
  if (!tokenData.access_token) return { edits: {}, creations: {} }
  const accessToken = tokenData.access_token

  const result: Record<string, string> = {}
  const creations: Record<string, string> = {} // normalized_name → min start_date
  const accounts: Record<string, string> = {} // normalized_name → customer descriptive_name

  // Tous les customerIds en parallèle
  await Promise.all(customerIds.map(async (cid) => {
    try {
      // Fetch start_date ET change_status en parallèle pour chaque customerId
      const startDateQuery = `SELECT customer.descriptive_name, campaign.name, campaign.start_date_time FROM campaign WHERE campaign.status != 'REMOVED' LIMIT 10000`
      const changeQuery = `SELECT campaign.name, change_status.last_change_date_time
                     FROM change_status
                     WHERE change_status.resource_type IN ('CAMPAIGN', 'AD_GROUP', 'AD', 'AD_GROUP_AD', 'AD_GROUP_CRITERION', 'CAMPAIGN_CRITERION', 'ASSET', 'CAMPAIGN_BUDGET')
                     LIMIT 10000`
      const headers = {
        'Authorization': `Bearer ${accessToken}`,
        'developer-token': devToken,
        'Content-Type': 'application/json'
      }
      const apiUrl = `https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`

      const [startRes, changeRes] = await Promise.all([
        fetch(apiUrl, { method: 'POST', headers, body: JSON.stringify({ query: startDateQuery }) }),
        fetch(apiUrl, { method: 'POST', headers, body: JSON.stringify({ query: changeQuery }) })
      ])
      const [startData, changeData] = await Promise.all([startRes.json() as any, changeRes.json() as any])

      // Process start dates
      const startError = startData?.error || startData?.[0]?.error
      if (!startError && startData[0]?.results) {
        const rows = startData[0].results
        if (rows.length > 0) console.log(`🔍 [ads-last-edit] Google start_date sample for ${cid}:`, JSON.stringify(rows[0].campaign).substring(0, 200))
        for (const row of rows) {
          const normalized = normalizeAdName(row.campaign?.name)
          if (!normalized) continue
          const startDate = (row.campaign?.startDateTime || row.campaign?.start_date_time || row.campaign?.startDate || row.campaign?.start_date) as string | undefined
          if (!startDate) continue
          if (!creations[normalized] || startDate > creations[normalized]) {
            creations[normalized] = startDate
          }
          // Track account name
          const customerName = row.customer?.descriptiveName || row.customer?.descriptive_name
          if (customerName && !accounts[normalized]) accounts[normalized] = customerName
        }
        console.log(`✅ [ads-last-edit] Google start_date for ${cid}: ${Object.keys(creations).length} campagnes avec date`)
      } else if (startError) {
        console.warn(`⚠️ [ads-last-edit] Google start_date API error for ${cid}:`, JSON.stringify(startError).substring(0, 300))
      }

      // Process change status
      if (changeData.error || !changeData[0]?.results) {
        console.warn(`⚠️ [ads-last-edit] Google change_status not available for ${cid}, trying change_event fallback`)
        try {
          const now = new Date()
          const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)
          const sinceStr = `${thirtyDaysAgo.getFullYear()}-${String(thirtyDaysAgo.getMonth() + 1).padStart(2, '0')}-${String(thirtyDaysAgo.getDate()).padStart(2, '0')}`
          const untilStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
          const fallbackQuery = `SELECT campaign.name, change_event.change_date_time
                                 FROM change_event
                                 WHERE change_event.change_date_time >= '${sinceStr}' AND change_event.change_date_time <= '${untilStr}'
                                 ORDER BY change_event.change_date_time DESC
                                 LIMIT 10000`
          const r2 = await fetch(apiUrl, { method: 'POST', headers, body: JSON.stringify({ query: fallbackQuery }) })
          const data2 = await r2.json() as any
          if (!data2.error && data2[0]?.results) {
            for (const row of data2[0].results) {
              const normalized = normalizeAdName(row.campaign?.name)
              if (!normalized) continue
              const lastChange = row.changeEvent?.changeDateTime as string | undefined
              if (!lastChange) continue
              if (!result[normalized] || new Date(lastChange) > new Date(result[normalized])) {
                result[normalized] = lastChange
              }
            }
          }
        } catch (e2) {
          console.warn(`⚠️ [ads-last-edit] Google change_event fallback also failed for ${cid}:`, (e2 as Error).message)
        }
        return
      }

      for (const row of changeData[0].results) {
        const normalized = normalizeAdName(row.campaign?.name)
        if (!normalized) continue
        const lastChange = row.changeStatus?.lastChangeDateTime as string | undefined
        if (!lastChange) continue
        if (!result[normalized] || new Date(lastChange) > new Date(result[normalized])) {
          result[normalized] = lastChange
        }
      }
    } catch (e) {
      console.warn(`⚠️ [ads-last-edit] Google error for ${cid}:`, (e as Error).message)
    }
  }))

  console.log(`✅ [ads-last-edit] Google: ${Object.keys(result).length} noms avec date de modif, ${Object.keys(creations).length} avec date de création, ${Object.keys(accounts).length} avec compte`)
  return { edits: result, creations, accounts }
}

// --- Fonctions de fetch journalier pour la synchronisation Supabase ---

async function fetchMetaAdsInsightsDaily(since: string, until: string): Promise<DailyAdRow[]> {
  const tokens = [
    process.env.META_ADS_TOKEN_1,
    process.env.META_ADS_TOKEN_2,
    process.env.META_ADS_TOKEN_3,
    process.env.META_ADS_TOKEN_4,
    process.env.META_ADS_TOKEN_5
  ].filter(Boolean) as string[]

  if (tokens.length === 0) {
    console.warn('⚠️ [ads-sync] Aucun token Meta configuré')
    return []
  }

  const timeRange = encodeURIComponent(JSON.stringify({ since, until }))
  const seenAccountIds = new Set<string>()
  const rows: DailyAdRow[] = []

  for (const token of tokens) {
    try {
      const accRes = await fetch(`https://graph.facebook.com/v21.0/me/adaccounts?fields=id,name,account_status&limit=100&access_token=${token}`)
      const accData = await accRes.json() as any
      if (!accData.data) continue

      const accounts = (accData.data as any[]).filter((a: any) => a.account_status === 1 && !seenAccountIds.has(a.id))
      for (const acc of accounts) seenAccountIds.add(acc.id)

      for (const acc of accounts) {
        try {
          // time_increment=1 pour obtenir un breakdown journalier
          let url: string | null = `https://graph.facebook.com/v21.0/${acc.id}/insights?fields=adset_name,spend,impressions,clicks,actions&level=adset&time_range=${timeRange}&time_increment=1&limit=500&access_token=${token}`

          while (url) {
            const insRes = await fetch(url)
            const insData = await insRes.json() as any
            if (!insData.data) break

            for (const row of insData.data) {
              const normalized = normalizeAdName(row.adset_name)
              if (!normalized) continue

              let leads = 0
              if (row.actions) {
                for (const action of row.actions) {
                  if (action.action_type === 'lead' || action.action_type === 'onsite_web_lead') {
                    leads += parseInt(action.value || '0')
                    break
                  }
                }
              }

              rows.push({
                source: 'meta',
                ad_name_raw: row.adset_name,
                normalized_name: normalized,
                stat_date: row.date_start,
                spend: parseFloat(row.spend || '0'),
                impressions: parseInt(row.impressions || '0'),
                clicks: parseInt(row.clicks || '0'),
                leads,
                account_id: `${acc.name || acc.id}|||${acc.id}`
              })
            }

            // Pagination Meta
            url = insData.paging?.next || null
          }
        } catch (e) {
          console.warn(`⚠️ [ads-sync] Meta insights error for ${acc.id}:`, (e as Error).message)
        }
      }
    } catch (e) {
      console.warn('⚠️ [ads-sync] Meta accounts error:', (e as Error).message)
    }
  }

  return rows
}

async function fetchGoogleAdsInsightsDaily(since: string, until: string): Promise<DailyAdRow[]> {
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
  const customerIds = process.env.GOOGLE_ADS_CUSTOMER_IDS?.split(',').map(s => s.trim()).filter(Boolean)

  if (!clientId || !clientSecret || !refreshToken || !devToken || !customerIds?.length) {
    console.warn('⚠️ [ads-sync] Config Google Ads incomplète')
    return []
  }

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
  })
  const tokenData = await tokenRes.json() as any
  if (!tokenData.access_token) {
    console.warn('⚠️ [ads-sync] Google OAuth error:', tokenData)
    return []
  }
  const accessToken = tokenData.access_token

  const rows: DailyAdRow[] = []

  for (const cid of customerIds) {
    try {
      // Récupérer le nom du compte Google Ads
      let customerName = cid
      try {
        const nameQuery = `SELECT customer.descriptive_name FROM customer LIMIT 1`
        const nameRes = await fetch(`https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'developer-token': devToken,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ query: nameQuery })
        })
        const nameData = await nameRes.json() as any
        if (nameData[0]?.results?.[0]?.customer?.descriptiveName) {
          customerName = nameData[0].results[0].customer.descriptiveName
        }
      } catch (_) { /* fallback to cid */ }

      const query = `SELECT campaign.name, segments.date, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions
                     FROM campaign
                     WHERE campaign.status = 'ENABLED'
                       AND segments.date BETWEEN '${since}' AND '${until}'
                       AND metrics.impressions > 0
                     ORDER BY segments.date DESC`

      const r = await fetch(`https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'developer-token': devToken,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query })
      })
      const data = await r.json() as any
      if (data.error || !data[0]?.results) continue

      for (const row of data[0].results) {
        const normalized = normalizeAdName(row.campaign.name)
        if (!normalized) continue

        rows.push({
          source: 'google',
          ad_name_raw: row.campaign.name,
          normalized_name: normalized,
          stat_date: row.segments.date,
          spend: parseInt(row.metrics.costMicros || '0') / 1e6,
          impressions: parseInt(row.metrics.impressions || '0'),
          clicks: parseInt(row.metrics.clicks || '0'),
          leads: parseFloat(row.metrics.conversions || '0'),
          account_id: `${customerName}|||${cid}`
        })
      }
    } catch (e) {
      console.warn(`⚠️ [ads-sync] Google Ads error for ${cid}:`, (e as Error).message)
    }
  }

  return rows
}

// --- Synchronisation vers Supabase ---

async function updateSyncMetadata(
  source: 'meta' | 'google',
  status: string,
  errorMsg: string | null,
  rowsSynced: number,
  wasFullSync: boolean = false
): Promise<void> {
  const update: Record<string, any> = {
    last_sync_status: status,
    last_sync_error: errorMsg,
    rows_synced: rowsSynced,
    last_incremental_sync: new Date().toISOString()
  }
  if (wasFullSync && status === 'success') {
    update.last_full_sync = new Date().toISOString()
  }
  await supabaseAdmin
    .from('ads_stats_sync_metadata')
    .update(update)
    .eq('source', source)
}

async function syncAdsStatsToSupabase(): Promise<void> {
  console.log('🔄 [ads-sync] Démarrage de la synchronisation...')
  const startTime = Date.now()

  try {
    // Vérifier l'état de la sync précédente
    const { data: metadataRows } = await supabaseAdmin
      .from('ads_stats_sync_metadata')
      .select('*')

    const metaMeta = metadataRows?.find((r: any) => r.source === 'meta')
    const googleMeta = metadataRows?.find((r: any) => r.source === 'google')

    const now = new Date()
    const todayStr = formatDateYMD(now)

    const needsFullSyncMeta = !metaMeta?.last_full_sync
    const needsFullSyncGoogle = !googleMeta?.last_full_sync

    // Full sync = 6 derniers mois (pas depuis 2020, trop volumineux avec time_increment=1)
    const fullSyncSince = formatDateYMD(new Date(now.getTime() - 180 * 24 * 60 * 60 * 1000))
    const incrementalSince = formatDateYMD(new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000))

    const metaSince = needsFullSyncMeta ? fullSyncSince : incrementalSince
    const googleSince = needsFullSyncGoogle ? fullSyncSince : incrementalSince

    console.log(`🔄 [ads-sync] Meta: ${needsFullSyncMeta ? 'FULL' : 'incrémental'} sync depuis ${metaSince}`)
    console.log(`🔄 [ads-sync] Google: ${needsFullSyncGoogle ? 'FULL' : 'incrémental'} sync depuis ${googleSince}`)

    // Fetch Meta et Google EN PARALLÈLE
    const [metaResult, googleResult] = await Promise.allSettled([
      fetchMetaAdsInsightsDaily(metaSince, todayStr),
      fetchGoogleAdsInsightsDaily(googleSince, todayStr)
    ])

    let metaRows: DailyAdRow[] = []
    if (metaResult.status === 'fulfilled') {
      metaRows = metaResult.value
      console.log(`✅ [ads-sync] Meta: ${metaRows.length} lignes récupérées`)
    } else {
      console.error('❌ [ads-sync] Meta fetch error:', metaResult.reason)
      await updateSyncMetadata('meta', 'error', String(metaResult.reason), 0)
    }

    let googleRows: DailyAdRow[] = []
    if (googleResult.status === 'fulfilled') {
      googleRows = googleResult.value
      console.log(`✅ [ads-sync] Google: ${googleRows.length} lignes récupérées`)
    } else {
      console.error('❌ [ads-sync] Google fetch error:', googleResult.reason)
      await updateSyncMetadata('google', 'error', String(googleResult.reason), 0)
    }

    // Dédupliquer par (source, ad_name_raw, stat_date) avant upsert
    // Le même adset peut exister dans plusieurs comptes pub → agréger
    const allRowsRaw = [...metaRows, ...googleRows]
    const deduped = new Map<string, DailyAdRow>()
    for (const row of allRowsRaw) {
      const key = `${row.source}|${row.ad_name_raw}|${row.stat_date}`
      const existing = deduped.get(key)
      if (existing) {
        existing.spend += row.spend
        existing.impressions += row.impressions
        existing.clicks += row.clicks
        existing.leads += row.leads
        // Garder le premier account_id rencontré
      } else {
        deduped.set(key, { ...row })
      }
    }
    const allRows = Array.from(deduped.values())
    if (allRowsRaw.length !== allRows.length) {
      console.log(`🔀 [ads-sync] Dédupliqué: ${allRowsRaw.length} → ${allRows.length} lignes`)
    }

    if (allRows.length > 0) {
      const batchSize = 500
      let upsertedCount = 0

      for (let i = 0; i < allRows.length; i += batchSize) {
        const batch = allRows.slice(i, i + batchSize).map(row => ({
          source: row.source,
          ad_name_raw: row.ad_name_raw,
          normalized_name: row.normalized_name,
          stat_date: row.stat_date,
          spend: row.spend,
          impressions: row.impressions,
          clicks: row.clicks,
          leads: row.leads,
          account_id: row.account_id,
          synced_at: new Date().toISOString()
        }))

        const { error } = await supabaseAdmin
          .from('ads_stats_daily')
          .upsert(batch, {
            onConflict: 'source,ad_name_raw,stat_date',
            ignoreDuplicates: false
          })

        if (error) {
          console.error(`❌ [ads-sync] Upsert batch ${Math.floor(i / batchSize) + 1} error:`, error)
        } else {
          upsertedCount += batch.length
        }
      }

      console.log(`✅ [ads-sync] ${upsertedCount}/${allRows.length} lignes upsertées`)

      if (metaRows.length > 0) {
        await updateSyncMetadata('meta', 'success', null, metaRows.length, needsFullSyncMeta)
      }
      if (googleRows.length > 0) {
        await updateSyncMetadata('google', 'success', null, googleRows.length, needsFullSyncGoogle)
      }
    } else {
      console.log('⚠️ [ads-sync] Aucune donnée récupérée')
    }

    // Invalider le cache mémoire
    invalidateCache('ads-stats')

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`✅ [ads-sync] Synchronisation terminée en ${elapsed}s`)
  } catch (error) {
    console.error('❌ [ads-sync] Erreur globale:', error)
  }
}

// ===================== LEADS STATS SYNC =====================

async function updateLeadsSyncMetadata(
  source: 'v1' | 'v2' | 'es',
  status: string,
  errorMsg: string | null,
  rowsSynced: number
): Promise<void> {
  await supabaseAdmin
    .from('leads_stats_sync_metadata')
    .update({
      last_sync: new Date().toISOString(),
      last_sync_status: status,
      last_sync_error: errorMsg,
      rows_synced: rowsSynced
    })
    .eq('source', source)
}

async function syncLeadsStatsToSupabase(): Promise<void> {
  console.log('🔄 [leads-sync] Démarrage de la synchronisation...')
  const startTime = Date.now()

  try {
    if (!dbPool) {
      console.warn('⚠️ [leads-sync] Database pool non disponible, skip')
      return
    }

    const SALE_PROJECTS = `'less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months'`
    const EXCLUDED_ORIGINS = `'storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'`

    // 1. Récupérer les V2 client IDs
    const v2ClientIds = await fetchEstimateurAgencies()
    const v2Arr = Array.from(v2ClientIds)
    console.log(`📋 [leads-sync] ${v2Arr.length} clients V2 identifiés`)

    // 2. Exécuter les 6 requêtes SQL en parallèle
    const v2Placeholders = v2Arr.map((_, i) => `$${i + 1}`).join(',')

    const queries: Promise<any>[] = []

    // V2 daily
    if (v2Arr.length > 0) {
      queries.push(dbPool.query(`
        SELECT DATE(p.created_date) as date, COUNT(*) as total_leads
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client IN (${v2Placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date)
      `, v2Arr))
    } else {
      queries.push(Promise.resolve({ rows: [] }))
    }

    // V2 daily phone
    if (v2Arr.length > 0) {
      queries.push(dbPool.query(`
        SELECT DATE(p.created_date) as date,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
          COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client IN (${v2Placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date)
      `, v2Arr))
    } else {
      queries.push(Promise.resolve({ rows: [] }))
    }

    // V1 daily
    if (v2Arr.length > 0) {
      queries.push(dbPool.query(`
        SELECT DATE(p.created_date) as date, COUNT(*) as total_leads
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client NOT IN (${v2Placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date)
      `, v2Arr))
    } else {
      queries.push(dbPool.query(`
        SELECT DATE(p.created_date) as date, COUNT(*) as total_leads
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date)
      `))
    }

    // V1 daily phone
    if (v2Arr.length > 0) {
      queries.push(dbPool.query(`
        SELECT DATE(p.created_date) as date,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
          COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client NOT IN (${v2Placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date)
      `, v2Arr))
    } else {
      queries.push(dbPool.query(`
        SELECT DATE(p.created_date) as date,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
          COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p LEFT JOIN agency a ON p.id_agency = a.id LEFT JOIN client c ON a.id_client = c.id_client
        WHERE c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
        GROUP BY DATE(p.created_date)
      `))
    }

    // ES daily
    queries.push(dbPool.query(`
      SELECT DATE(p.created_date) as date, COUNT(DISTINCT p.id_property)::integer as total_leads
      FROM property p
      INNER JOIN agency a ON p.id_agency = a.id
      INNER JOIN client c ON a.id_client = c.id_client
      WHERE c.locale = 'es_ES'
        AND c.demo IS NOT TRUE
          ${slugFilter()}
        AND p.sale_project IN (${SALE_PROJECTS})
        AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
        AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date)
    `))

    // ES daily phone
    queries.push(dbPool.query(`
      SELECT DATE(p.created_date) as date,
        COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
        COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
      FROM property p
      INNER JOIN agency a ON p.id_agency = a.id
      INNER JOIN client c ON a.id_client = c.id_client
      WHERE c.locale = 'es_ES'
        AND c.demo IS NOT TRUE
          ${slugFilter()}
        AND p.sale_project IN (${SALE_PROJECTS})
        AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
        AND p.created_date >= '2024-09-14' AND DATE(p.created_date) <= CURRENT_DATE
      GROUP BY DATE(p.created_date)
    `))

    const [v2Daily, v2Phone, v1Daily, v1Phone, esDaily, esPhone] = await Promise.all(queries)

    console.log(`📊 [leads-sync] Requêtes SQL terminées: V2=${v2Daily.rows.length}, V1=${v1Daily.rows.length}, ES=${esDaily.rows.length} dates`)

    // 3. Récupérer les données GA4
    let ga4V2: Record<string, number> = {}
    let ga4V1: Record<string, number> = {}
    let ga4Es: Record<string, number> = {}
    try {
      const [visitorsV2, visitorsV1, visitorsEs] = await Promise.all([
        getDailyVisitorsV2(),
        getDailyVisitorsV1(),
        getDailyVisitorsEs()
      ])
      for (const v of visitorsV2) ga4V2[v.date] = v.visitors
      for (const v of visitorsV1) ga4V1[v.date] = v.visitors
      for (const v of visitorsEs) ga4Es[v.date] = v.visitors
    } catch (e) {
      console.warn('⚠️ [leads-sync] GA4 non disponible:', (e as Error).message)
    }

    // 4. Fusionner les données par (source, date)
    // Convertir DATE PostgreSQL en string YYYY-MM-DD en respectant le timezone local
    // (le driver pg crée un Date à minuit local, toISOString() le décale en UTC → -1 jour)
    function pgDateToString(val: any): string {
      if (typeof val === 'string') return val.slice(0, 10)
      const d = val instanceof Date ? val : new Date(val)
      return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
    }

    function mergeRows(
      source: 'v1' | 'v2' | 'es',
      dailyRows: any[],
      phoneRows: any[],
      ga4Map: Record<string, number>
    ) {
      const byDate = new Map<string, any>()
      for (const r of dailyRows) {
        const d = pgDateToString(r.date)
        byDate.set(d, {
          source,
          stat_date: d,
          total_leads: parseInt(r.total_leads) || 0,
          leads_with_phone: 0,
          leads_with_validated_phone: 0,
          ga4_visitors: ga4Map[d] || 0,
          synced_at: new Date().toISOString()
        })
      }
      for (const r of phoneRows) {
        const d = pgDateToString(r.date)
        const existing = byDate.get(d)
        if (existing) {
          existing.leads_with_phone = parseInt(r.leads_with_phone) || 0
          existing.leads_with_validated_phone = parseInt(r.leads_with_validated_phone) || 0
        } else {
          byDate.set(d, {
            source,
            stat_date: d,
            total_leads: 0,
            leads_with_phone: parseInt(r.leads_with_phone) || 0,
            leads_with_validated_phone: parseInt(r.leads_with_validated_phone) || 0,
            ga4_visitors: ga4Map[d] || 0,
            synced_at: new Date().toISOString()
          })
        }
      }
      // Ajouter les dates GA4 manquantes
      for (const [d, visitors] of Object.entries(ga4Map)) {
        if (!byDate.has(d)) {
          byDate.set(d, {
            source,
            stat_date: d,
            total_leads: 0,
            leads_with_phone: 0,
            leads_with_validated_phone: 0,
            ga4_visitors: visitors,
            synced_at: new Date().toISOString()
          })
        }
      }
      return Array.from(byDate.values())
    }

    const v2Rows = mergeRows('v2', v2Daily.rows, v2Phone.rows, ga4V2)
    const v1Rows = mergeRows('v1', v1Daily.rows, v1Phone.rows, ga4V1)
    const esRows = mergeRows('es', esDaily.rows, esPhone.rows, ga4Es)
    const allRows = [...v2Rows, ...v1Rows, ...esRows]

    console.log(`📦 [leads-sync] ${allRows.length} lignes à upsert (V2=${v2Rows.length}, V1=${v1Rows.length}, ES=${esRows.length})`)

    // 5. Purger puis upsert par batch de 500
    if (allRows.length > 0) {
      // Purger les anciennes données (les filtres demo/origin ont changé)
      const { error: delErr } = await supabaseAdmin
        .from('leads_stats_daily')
        .delete()
        .gte('stat_date', '2024-09-14')
      if (delErr) console.warn('⚠️ [leads-sync] Erreur purge:', delErr.message)

      const BATCH_SIZE = 500
      let upsertedCount = 0

      for (let i = 0; i < allRows.length; i += BATCH_SIZE) {
        const batch = allRows.slice(i, i + BATCH_SIZE)
        const { error } = await supabaseAdmin
          .from('leads_stats_daily')
          .upsert(batch, {
            onConflict: 'source,stat_date',
            ignoreDuplicates: false
          })
        if (error) {
          console.error(`❌ [leads-sync] Erreur batch ${i}-${i + batch.length}:`, error.message)
        } else {
          upsertedCount += batch.length
        }
      }

      console.log(`✅ [leads-sync] ${upsertedCount}/${allRows.length} lignes upsertées`)

      if (v2Rows.length > 0) await updateLeadsSyncMetadata('v2', 'success', null, v2Rows.length)
      if (v1Rows.length > 0) await updateLeadsSyncMetadata('v1', 'success', null, v1Rows.length)
      if (esRows.length > 0) await updateLeadsSyncMetadata('es', 'success', null, esRows.length)
    } else {
      console.log('⚠️ [leads-sync] Aucune donnée récupérée')
    }

    // Invalider le cache mémoire (leads + GA4 depuis Supabase)
    invalidateCache('leads-')
    invalidateCache('ga4-daily-visitors')

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`✅ [leads-sync] Synchronisation terminée en ${elapsed}s`)
  } catch (error) {
    console.error('❌ [leads-sync] Erreur globale:', error)
  }
}

// Mini-sync incrémental: ne recalcule que la ligne d'aujourd'hui (V1, V2, ES)
// Beaucoup plus léger que la full sync (3 lignes upsertées, requêtes SQL filtrées sur CURRENT_DATE)
// Préserve la valeur ga4_visitors existante (rafraîchie par la full sync 6h)
async function syncTodayLeadsStatsToSupabase(): Promise<void> {
  const startTime = Date.now()
  try {
    if (!dbPool) return

    const SALE_PROJECTS = `'less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months'`
    const EXCLUDED_ORIGINS = `'storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'`

    const v2ClientIds = await fetchEstimateurAgencies()
    const v2Arr = Array.from(v2ClientIds)
    if (v2Arr.length === 0) return
    const v2Placeholders = v2Arr.map((_, i) => `$${i + 1}`).join(',')

    // Une seule requête SQL agrège V2/V1/ES + total + phone + validated phone pour aujourd'hui
    const queries: Promise<any>[] = [
      // V2 today
      dbPool.query(`
        SELECT
          COUNT(*)::integer as total_leads,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
          COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p
        LEFT JOIN agency a ON p.id_agency = a.id
        LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client IN (${v2Placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND DATE(p.created_date) = CURRENT_DATE
      `, v2Arr),
      // V1 today
      dbPool.query(`
        SELECT
          COUNT(*)::integer as total_leads,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
          COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p
        LEFT JOIN agency a ON p.id_agency = a.id
        LEFT JOIN client c ON a.id_client = c.id_client
        WHERE a.id_client NOT IN (${v2Placeholders})
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND DATE(p.created_date) = CURRENT_DATE
      `, v2Arr),
      // ES today
      dbPool.query(`
        SELECT
          COUNT(DISTINCT p.id_property)::integer as total_leads,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as leads_with_phone,
          COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as leads_with_validated_phone
        FROM property p
        INNER JOIN agency a ON p.id_agency = a.id
        INNER JOIN client c ON a.id_client = c.id_client
        WHERE c.locale = 'es_ES'
          AND c.demo IS NOT TRUE
          ${slugFilter()}
          AND p.sale_project IN (${SALE_PROJECTS})
          AND (p.origin IS NULL OR p.origin NOT IN (${EXCLUDED_ORIGINS}))
          AND DATE(p.created_date) = CURRENT_DATE
      `)
    ]

    const [v2Res, v1Res, esRes] = await Promise.all(queries)

    // Date "aujourd'hui" en timezone serveur (CET en prod)
    const now = new Date()
    const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`

    // Récupérer les ga4_visitors existants pour ne pas les écraser
    const { data: existing } = await supabaseAdmin
      .from('leads_stats_daily')
      .select('source, ga4_visitors')
      .eq('stat_date', today)
      .in('source', ['v1', 'v2', 'es'])
    const ga4By: Record<string, number> = {}
    for (const r of existing || []) ga4By[r.source as string] = (r as any).ga4_visitors || 0

    const buildRow = (source: 'v1' | 'v2' | 'es', row: any) => ({
      source,
      stat_date: today,
      total_leads: parseInt(row?.total_leads) || 0,
      leads_with_phone: parseInt(row?.leads_with_phone) || 0,
      leads_with_validated_phone: parseInt(row?.leads_with_validated_phone) || 0,
      ga4_visitors: ga4By[source] || 0,
      synced_at: new Date().toISOString()
    })

    const rows = [
      buildRow('v2', v2Res.rows[0]),
      buildRow('v1', v1Res.rows[0]),
      buildRow('es', esRes.rows[0])
    ]

    const { error } = await supabaseAdmin
      .from('leads_stats_daily')
      .upsert(rows, { onConflict: 'source,stat_date', ignoreDuplicates: false })
    if (error) {
      console.error('❌ [leads-today-sync] Upsert error:', error.message)
      return
    }

    // Invalider les caches mémoire pour que l'API renvoie immédiatement les nouvelles valeurs
    invalidateCache('leads-')

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2)
    console.log(`✅ [leads-today-sync] ${today} V2=${rows[0].total_leads} V1=${rows[1].total_leads} ES=${rows[2].total_leads} (${elapsed}s)`)
  } catch (error) {
    console.error('❌ [leads-today-sync] Erreur:', error)
  }
}

// --- Endpoint force-sync leads ---
app.get('/api/leads/force-sync', async (_req, res) => {
  try {
    console.log('🔄 [leads-sync] Force sync triggered via API')
    await syncLeadsStatsToSupabase()
    // Invalider tous les caches leads
    invalidateCache('leads-')
    res.json({ success: true, message: 'Leads sync completed' })
  } catch (error) {
    console.error('❌ [leads-sync] Force sync error:', error)
    res.status(500).json({ error: 'Sync failed', details: (error as Error).message })
  }
})

// --- Endpoint ads-stats (lecture depuis Supabase) ---

app.get('/api/ads-stats', async (req, res) => {
  const period = (req.query.period as string) || 'all'
  console.log(`🔍 [ads-stats] Début (period=${period})`)
  try {
    const cacheKey = `ads-stats-${period}`
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    // Calculer la date de début selon la période
    const now = new Date()
    let sinceDate: string
    if (period === 'month') {
      sinceDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`
    } else if (period === '5d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 5 * 24 * 60 * 60 * 1000))
    } else if (period === '15d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 15 * 24 * 60 * 60 * 1000))
    } else if (period === '30d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000))
    } else if (period === '90d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000))
    } else {
      sinceDate = '2020-01-01'
    }

    // Lancer RPC Supabase ET fetch budgets en parallèle
    const budgetCacheKey = 'ads-budgets-live'
    const cachedBudgets = getCached<{ meta: Record<string, AdBudgetInfo> }>(budgetCacheKey)?.meta
    const [rpcResult, metaBudgets] = await Promise.all([
      supabaseAdmin.rpc('get_ads_stats_aggregated', { since_date: sinceDate }),
      cachedBudgets
        ? Promise.resolve(cachedBudgets)
        : fetchMetaAdsetBudgets().catch(() => ({} as Record<string, AdBudgetInfo>))
    ])

    const { data: rows, error } = rpcResult

    if (error) {
      console.error('❌ [ads-stats] Supabase RPC error:', error)
      console.log('🔄 [ads-stats] Fallback vers appel API direct...')
      const [metaData, googleData] = await Promise.all([
        fetchMetaAdsInsights(period),
        fetchGoogleAdsInsights(period)
      ])
      const rawMeta: Record<string, AdMetrics> = {}
      const rawGoogle: Record<string, AdMetrics> = {}
      for (const [k, v] of metaData) rawMeta[k] = v
      for (const [k, v] of googleData) rawGoogle[k] = v
      const fallbackData = { meta: rawMeta, google: rawGoogle }
      setCache(cacheKey, fallbackData, CACHE_TTL_MS)
      return res.json(fallbackData)
    }

    const rawMeta: Record<string, AdMetrics> = {}
    const rawGoogle: Record<string, AdMetrics> = {}

    for (const row of (rows || [])) {
      const target = row.source === 'meta' ? rawMeta : rawGoogle
      target[row.normalized_name] = {
        spend: parseFloat(row.total_spend) || 0,
        impressions: parseInt(row.total_impressions) || 0,
        clicks: parseInt(row.total_clicks) || 0,
        leads: parseFloat(row.total_leads) || 0,
        account_ids: row.account_ids || [],
        first_stat_date: row.first_stat_date || null
      }
    }

    // Ajouter les adsets Meta actifs avec budget mais 0 dépense dans la période
    try {
      let addedBudgetOnly = 0
      for (const [rawName] of Object.entries(metaBudgets)) {
        const norm = normalizeAdName(rawName)
        if (!norm || rawMeta[norm]) continue
        rawMeta[norm] = { spend: 0, impressions: 0, clicks: 0, leads: 0, account_ids: [] }
        addedBudgetOnly++
      }
      if (addedBudgetOnly > 0) {
        console.log(`📊 [ads-stats] +${addedBudgetOnly} adsets Meta budget-only ajoutés`)
      }
    } catch (e) {
      console.warn('⚠️ [ads-stats] Erreur ajout budget-only:', (e as Error).message)
    }

    const responseData = { meta: rawMeta, google: rawGoogle }
    console.log(`✅ [ads-stats] Total: ${Object.keys(rawMeta).length} Meta + ${Object.keys(rawGoogle).length} Google (depuis Supabase)`)
    setCache(cacheKey, responseData, CACHE_TTL_MS)
    res.json(responseData)
  } catch (e) {
    console.error('❌ [ads-stats] Erreur:', e)
    res.status(500).json({ error: 'Erreur lors de la récupération des stats publicitaires' })
  }
})

// Endpoint pour récupérer la date de dernière modification des campagnes Meta/Google
app.get('/api/ads-last-edit', async (req, res) => {
  console.log('🔍 [ads-last-edit] Début')
  try {
    const cacheKey = 'ads-last-edit'
    if (req.query.refresh) invalidateCache(cacheKey)
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const [metaResult, googleResult, auditRows] = await Promise.all([
      fetchMetaLastEdits(),
      fetchGoogleLastEdits(),
      // Compléter avec les modifications faites via le backoffice (ads_audit_log)
      supabaseAdmin.from('ads_audit_log')
        .select('source, entity_name, created_at')
        .gte('created_at', new Date(Date.now() - 15 * 24 * 60 * 60 * 1000).toISOString())
        .order('created_at', { ascending: false })
        .limit(500)
        .then(r => r.data || [])
        .catch(() => [] as any[])
    ])
    const data: any = {
      meta: metaResult.edits,
      google: googleResult.edits,
      metaCreations: metaResult.creations,
      googleCreations: googleResult.creations,
      metaStarts: metaResult.starts,
      googleStarts: googleResult.creations, // Google start_date_time = date de début de campagne
      metaAccounts: metaResult.accounts,
      googleAccounts: googleResult.accounts
    }
    // Enrichir les dates de modif avec l'audit log (modifications via le backoffice)
    for (const row of auditRows) {
      if (!row.entity_name || !row.source || !row.created_at) continue
      const normalized = normalizeAdName(row.entity_name)
      if (!normalized) continue
      const target = row.source === 'meta' ? data.meta : row.source === 'google' ? data.google : null
      if (!target) continue
      if (!target[normalized] || new Date(row.created_at) > new Date(target[normalized])) {
        target[normalized] = row.created_at
      }
    }
    setCache(cacheKey, data, CACHE_TTL_LONG_MS)
    res.json(data)
  } catch (e) {
    console.error('❌ [ads-last-edit] Erreur:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// Endpoint pour récupérer la dernière date de diffusion (spend > 0) par adset/campagne
app.get('/api/ads-last-delivery', async (_req, res) => {
  try {
    const cacheKey = 'ads-last-delivery'
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    const { data: rows, error } = await supabaseAdmin
      .from('ads_stats_daily')
      .select('source, normalized_name, stat_date, spend')
      .gt('spend', 0)
      .order('stat_date', { ascending: false })
      .limit(50000)

    if (error) {
      console.error('❌ [ads-last-delivery] Supabase error:', error)
      return res.json({ meta: {}, google: {} })
    }

    const meta: Record<string, string> = {}
    const google: Record<string, string> = {}
    for (const row of (rows || [])) {
      const target = row.source === 'meta' ? meta : google
      // On garde uniquement le max (les rows sont déjà triées desc)
      if (!target[row.normalized_name]) {
        target[row.normalized_name] = row.stat_date
      }
    }

    const data = { meta, google }
    setCache(cacheKey, data, CACHE_TTL_MS)
    res.json(data)
  } catch (e) {
    console.error('❌ [ads-last-delivery] Erreur:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// --- Fonctions pour récupérer les budgets quotidiens alloués ---

interface AdBudgetInfo { daily_budget: number; url: string; id: string; status?: string }

// Maps server-side pour retrouver les credentials associées à chaque adset/campagne
const metaAdsetCredentials = new Map<string, { token: string; accountId: string }>()
const googleCampaignCredentials = new Map<string, { customerId: string; budgetResourceName: string }>()
// Reverse lookup: normalized ad name → IDs (pour résoudre les adKeys vers des IDs)
const metaNormalizedToIds = new Map<string, string[]>()
const googleNormalizedToIds = new Map<string, string[]>()

async function fetchMetaAdsetBudgets(): Promise<Record<string, AdBudgetInfo>> {
  const tokens = [
    process.env.META_ADS_TOKEN_1,
    process.env.META_ADS_TOKEN_2,
    process.env.META_ADS_TOKEN_3,
    process.env.META_ADS_TOKEN_4,
    process.env.META_ADS_TOKEN_5
  ].filter(Boolean) as string[]

  if (tokens.length === 0) return {}

  const seenAccountIds = new Set<string>()
  const result: Record<string, AdBudgetInfo> = {}

  // Phase 1 : récupérer tous les comptes en parallèle
  const allAccounts: { id: string; token: string }[] = []
  await Promise.all(tokens.map(async (token) => {
    try {
      const accRes = await fetch(`https://graph.facebook.com/v21.0/me/adaccounts?fields=id,account_status&limit=100&access_token=${token}`)
      const accData = await accRes.json() as any
      if (!accData.data) return
      for (const a of (accData.data as any[])) {
        if (a.account_status === 1 && !seenAccountIds.has(a.id)) {
          seenAccountIds.add(a.id)
          allAccounts.push({ id: a.id, token })
        }
      }
    } catch (e) {
      console.warn('⚠️ [ads-budgets] Meta accounts error:', (e as Error).message)
    }
  }))

  // Phase 2 : récupérer les adsets de tous les comptes en parallèle
  await Promise.all(allAccounts.map(async ({ id: accId, token }) => {
    try {
      const url = `https://graph.facebook.com/v21.0/${accId}/adsets?fields=id,name,daily_budget,effective_status&limit=500&access_token=${token}`
      const r = await fetch(url)
      const data = await r.json() as any
      if (!data.data) return

      const actNumeric = accId.replace('act_', '')

      for (const adset of data.data) {
        if (!adset.name) continue
        const dailyBudget = adset.daily_budget ? parseFloat(adset.daily_budget) / 100 : 0
        const adsetUrl = `https://www.facebook.com/adsmanager/manage/adsets/edit?act=${actNumeric}&selected_adset_ids=${adset.id}`
        result[adset.name] = { daily_budget: dailyBudget, url: adsetUrl, id: adset.id, status: adset.effective_status || undefined }
        metaAdsetCredentials.set(adset.id, { token, accountId: accId })
        const norm = normalizeAdName(adset.name)
        if (!metaNormalizedToIds.has(norm)) metaNormalizedToIds.set(norm, [])
        if (!metaNormalizedToIds.get(norm)!.includes(adset.id)) metaNormalizedToIds.get(norm)!.push(adset.id)
      }
    } catch (e) {
      console.warn(`⚠️ [ads-budgets] Meta adsets error for ${accId}:`, (e as Error).message)
    }
  }))

  console.log(`✅ [ads-budgets] Meta: ${Object.keys(result).length} adsets avec budget/url`)
  return result
}

async function fetchGoogleCampaignBudgets(): Promise<Record<string, AdBudgetInfo>> {
  reloadGoogleAdsEnv()
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
  const customerIds = process.env.GOOGLE_ADS_CUSTOMER_IDS?.split(',').map(s => s.trim()).filter(Boolean)

  if (!clientId || !clientSecret || !refreshToken || !devToken || !customerIds?.length) {
    console.warn('⚠️ [ads-budgets] Config Google Ads incomplète, impossible de récupérer les budgets')
    return {}
  }

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
  })
  const tokenData = await tokenRes.json() as any
  if (!tokenData.access_token) {
    console.warn('⚠️ [ads-budgets] Google OAuth token refresh failed:', tokenData.error || tokenData)
    return {}
  }
  const accessToken = tokenData.access_token

  const result: Record<string, AdBudgetInfo> = {}

  // Tous les customerIds en parallèle
  await Promise.all(customerIds.map(async (cid) => {
    try {
      const query = `SELECT campaign.id, campaign.name, campaign.status, campaign_budget.amount_micros, campaign_budget.resource_name
                     FROM campaign
                     WHERE campaign.status IN ('ENABLED', 'PAUSED')`

      const r = await fetch(`https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'developer-token': devToken,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query })
      })
      if (!r.ok) {
        const errText = await r.text().catch(() => '(unreadable)')
        console.warn(`⚠️ [ads-budgets] Google HTTP ${r.status} for ${cid}: ${errText.slice(0, 300)}`)
        return
      }
      const data = await r.json() as any
      if (data.error) {
        console.warn(`⚠️ [ads-budgets] Google API error for ${cid}:`, JSON.stringify(data.error).slice(0, 500))
        return
      }
      if (!Array.isArray(data) || !data[0]?.results) {
        console.warn(`⚠️ [ads-budgets] Google API réponse inattendue pour ${cid}: type=${typeof data}, isArray=${Array.isArray(data)}, firstItem=${JSON.stringify(data?.[0] || data).slice(0, 300)}`)
        return
      }

      for (const chunk of data) {
        if (!chunk?.results) continue
        for (const row of chunk.results) {
          if (!row.campaign?.name) continue
          const dailyBudget = row.campaignBudget?.amountMicros ? parseInt(row.campaignBudget.amountMicros) / 1e6 : 0
          const campaignId = row.campaign.id
          const campaignUrl = `https://ads.google.com/aw/campaigns?campaignId=${campaignId}&ocid=${cid}`
          const campaignStatus = row.campaign.status === 'PAUSED' ? 'PAUSED' : 'ACTIVE'
          result[row.campaign.name] = { daily_budget: dailyBudget, url: campaignUrl, id: campaignId, status: campaignStatus }
          if (row.campaignBudget?.resourceName) {
            googleCampaignCredentials.set(campaignId, { customerId: cid, budgetResourceName: row.campaignBudget.resourceName })
          }
          const norm = normalizeAdName(row.campaign.name)
          if (!googleNormalizedToIds.has(norm)) googleNormalizedToIds.set(norm, [])
          if (!googleNormalizedToIds.get(norm)!.includes(campaignId)) googleNormalizedToIds.get(norm)!.push(campaignId)
        }
      }
      console.log(`  [ads-budgets] Google cid=${cid}: ${data.reduce((n: number, c: any) => n + (c?.results?.length || 0), 0)} campagnes`)
    } catch (e) {
      console.warn(`⚠️ [ads-budgets] Google error for ${cid}:`, (e as Error).message)
    }
  }))

  console.log(`✅ [ads-budgets] Google: ${Object.keys(result).length} campagnes avec budget/url${Object.keys(result).length > 0 ? ' (ex: ' + Object.keys(result).slice(0, 3).join(', ') + ')' : ''}`)
  return result
}

// --- Détail complet d'un adset Meta (ciblage + pubs avec images) ---
async function fetchMetaAdsetDetail(adsetId: string) {
  const creds = metaAdsetCredentials.get(adsetId)
  if (!creds) return null
  try {
    const [infoRes, adsRes] = await Promise.all([
      fetch(`https://graph.facebook.com/v21.0/${adsetId}?fields=name,daily_budget,targeting,effective_status&access_token=${creds.token}`),
      fetch(`https://graph.facebook.com/v21.0/${adsetId}/ads?fields=name,effective_status,creative{id,title,body,image_url,thumbnail_url,object_story_spec}&limit=50&access_token=${creds.token}`)
    ])
    const info = await infoRes.json() as any
    const adsData = await adsRes.json() as any
    if (info.error || !info.name) return null
    if (adsData.error) console.warn(`⚠️ [ads-detail] Meta ads fetch error for adset ${adsetId}:`, adsData.error?.message)

    // Parse targeting
    const t = info.targeting || {}
    const locations: string[] = []
    if (t.geo_locations?.cities) locations.push(...t.geo_locations.cities.map((c: any) => c.name))
    if (t.geo_locations?.regions) locations.push(...t.geo_locations.regions.map((r: any) => r.name))
    if (t.geo_locations?.zips) locations.push(...t.geo_locations.zips.map((z: any) => z.name || z.key))
    if (t.geo_locations?.countries) locations.push(...t.geo_locations.countries)
    const interests: string[] = []
    if (t.flexible_spec) {
      for (const spec of t.flexible_spec) {
        if (spec.interests) interests.push(...spec.interests.map((i: any) => i.name))
        if (spec.behaviors) interests.push(...spec.behaviors.map((b: any) => b.name))
      }
    }
    const audiences: string[] = []
    if (t.custom_audiences) audiences.push(...t.custom_audiences.map((a: any) => a.name))

    // Récupérer les images HD via thumbnail_url avec dimensions élevées (pas besoin de pages_read_engagement)
    const rawAds = (adsData.data || []) as any[]
    const creativeIds = rawAds.map((ad: any) => ad.creative?.id).filter(Boolean) as string[]
    const creativeData = new Map<string, any>() // creative_id → { thumbnail_url, title, body, object_story_spec, asset_feed_spec }

    if (creativeIds.length > 0) {
      try {
        // Batch fetch des creatives : image HD + textes (title, body, object_story_spec, asset_feed_spec)
        const batchIds = creativeIds.join(',')
        const crFields = 'thumbnail_url,title,body,object_story_spec,asset_feed_spec'
        const crRes = await fetch(`https://graph.facebook.com/v21.0/?ids=${batchIds}&fields=${crFields}&thumbnail_width=1080&thumbnail_height=1080&access_token=${creds.token}`)
        const crData = await crRes.json() as any
        if (!crData.error) {
          for (const [crId, crInfo] of Object.entries(crData as Record<string, any>)) {
            creativeData.set(crId, crInfo)
          }
        }
      } catch {
        // Silently fail — fallback to data from ads response
      }
    }
    // Parse ads avec images haute résolution et textes des creatives
    const ads = rawAds.map((ad: any) => {
      const c = ad.creative || {}
      const cr = c.id ? (creativeData.get(c.id) || {}) : {} // données batch du creative
      const linkData = cr.object_story_spec?.link_data || c.object_story_spec?.link_data || {}
      const videoData = cr.object_story_spec?.video_data || c.object_story_spec?.video_data || {}
      // asset_feed_spec pour les pubs Advantage+ / dynamic creatives
      const afs = cr.asset_feed_spec || {}

      // Collecter TOUS les titres, bodies, descriptions (dédupliqués)
      const collectUnique = (values: (string | undefined | null)[]) =>
        [...new Set(values.filter((v): v is string => !!v && v.trim() !== ''))]

      const titles = collectUnique([
        cr.title, c.title, linkData.name, videoData.title,
        ...(afs.titles || []).map((t: any) => t.text)
      ])
      const bodies = collectUnique([
        cr.body, c.body, linkData.message, videoData.message,
        ...(afs.bodies || []).map((b: any) => b.text)
      ])
      const descriptions = collectUnique([
        linkData.description, videoData.description,
        ...(afs.descriptions || []).map((d: any) => d.text)
      ])

      return {
        id: ad.id,
        name: ad.name,
        status: ad.effective_status,
        titles,
        bodies,
        descriptions,
        image_url: cr.thumbnail_url || linkData.image_url || c.image_url || linkData.picture || c.thumbnail_url || null
      }
    })

    return {
      id: adsetId,
      name: info.name,
      daily_budget: info.daily_budget ? parseFloat(info.daily_budget) / 100 : 0,
      status: info.effective_status,
      targeting: {
        age_min: t.age_min || null,
        age_max: t.age_max || null,
        locations,
        interests,
        custom_audiences: audiences
      },
      ads
    }
  } catch (e) {
    console.warn(`⚠️ [ads-detail] Meta adset ${adsetId} error:`, (e as Error).message)
    return null
  }
}

// --- Détail complet d'une campagne Google (ciblage + annonces + assets PMax) ---
async function fetchGoogleCampaignDetail(campaignId: string, accessToken: string) {
  const creds = googleCampaignCredentials.get(campaignId)
  if (!creds) return null
  const { customerId } = creds
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN!
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'developer-token': devToken,
    'Content-Type': 'application/json'
  }
  const searchUrl = `https://googleads.googleapis.com/v23/customers/${customerId}/googleAds:searchStream`

  try {
    const [campaignRes, adsRes, locRes, assetsRes] = await Promise.all([
      fetch(searchUrl, {
        method: 'POST', headers,
        body: JSON.stringify({ query: `SELECT campaign.name, campaign_budget.amount_micros, campaign.advertising_channel_type FROM campaign WHERE campaign.id = ${campaignId}` })
      }),
      fetch(searchUrl, {
        method: 'POST', headers,
        body: JSON.stringify({ query: `SELECT ad_group.name, ad_group_ad.ad.type, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.descriptions, ad_group_ad.ad.final_urls, ad_group_ad.status FROM ad_group_ad WHERE campaign.id = ${campaignId} AND ad_group_ad.status != 'REMOVED'` })
      }),
      fetch(searchUrl, {
        method: 'POST', headers,
        body: JSON.stringify({ query: `SELECT geo_target_constant.name FROM campaign_criterion WHERE campaign.id = ${campaignId} AND campaign_criterion.type = 'LOCATION' AND campaign_criterion.negative = false` })
      }),
      fetch(searchUrl, {
        method: 'POST', headers,
        body: JSON.stringify({ query: `SELECT asset_group.name, asset_group_asset.field_type, asset.text_asset.text, asset.image_asset.full_size.url FROM asset_group_asset WHERE campaign.id = ${campaignId}` })
      })
    ])

    const [campaignData, adsData, locData, assetsData] = await Promise.all([
      campaignRes.json() as Promise<any>,
      adsRes.json() as Promise<any>,
      locRes.json() as Promise<any>,
      assetsRes.json() as Promise<any>
    ])

    // Debug → fichier
    const fs = await import('fs')
    const dbg = [
      `campaign: ${JSON.stringify(campaignData).substring(0, 200)}`,
      `ads: ${JSON.stringify(adsData).substring(0, 300)}`,
      `loc: ${JSON.stringify(locData).substring(0, 200)}`,
      `assets: ${JSON.stringify(assetsData).substring(0, 300)}`
    ]
    fs.appendFileSync('ads-detail-debug.log', `\n--- GOOGLE ${campaignId} (${new Date().toISOString()}) ---\n${dbg.join('\n')}\n`)

    // Campaign info
    const campInfo = campaignData?.[0]?.results?.[0]
    const campaignName = campInfo?.campaign?.name || `Campaign ${campaignId}`
    const dailyBudget = campInfo?.campaignBudget?.amountMicros ? parseInt(campInfo.campaignBudget.amountMicros) / 1e6 : 0
    const channelType = campInfo?.campaign?.advertisingChannelType || ''

    // Locations
    const locations: string[] = []
    if (locData?.[0]?.results) {
      for (const row of locData[0].results) {
        if (row.geoTargetConstant?.name) locations.push(row.geoTargetConstant.name)
      }
    }

    // Ad groups with ads (Search campaigns)
    const adGroupsMap = new Map<string, { name: string; ads: any[] }>()
    if (adsData?.[0]?.results) {
      for (const row of adsData[0].results) {
        const agName = row.adGroup?.name || 'Default'
        if (!adGroupsMap.has(agName)) adGroupsMap.set(agName, { name: agName, ads: [] })
        const rsa = row.adGroupAd?.ad?.responsiveSearchAd
        if (rsa) {
          adGroupsMap.get(agName)!.ads.push({
            type: 'RESPONSIVE_SEARCH_AD',
            status: row.adGroupAd?.status,
            headlines: (rsa.headlines || []).map((h: any) => h.text),
            descriptions: (rsa.descriptions || []).map((d: any) => d.text),
            final_urls: row.adGroupAd?.ad?.finalUrls || []
          })
        }
      }
    }

    // Asset groups (PMax campaigns)
    const assetGroupsMap = new Map<string, { name: string; headlines: string[]; long_headlines: string[]; descriptions: string[]; images: string[] }>()
    if (assetsData?.[0]?.results) {
      for (const row of assetsData[0].results) {
        const agName = row.assetGroup?.name || 'Default'
        if (!assetGroupsMap.has(agName)) assetGroupsMap.set(agName, { name: agName, headlines: [], long_headlines: [], descriptions: [], images: [] })
        const group = assetGroupsMap.get(agName)!
        const fieldType = row.assetGroupAsset?.fieldType
        const text = row.asset?.textAsset?.text
        const imageUrl = row.asset?.imageAsset?.fullSize?.url
        if (fieldType === 'HEADLINE' && text) group.headlines.push(text)
        else if (fieldType === 'LONG_HEADLINE' && text) group.long_headlines.push(text)
        else if (fieldType === 'DESCRIPTION' && text) group.descriptions.push(text)
        else if ((fieldType === 'MARKETING_IMAGE' || fieldType === 'SQUARE_MARKETING_IMAGE') && imageUrl) group.images.push(imageUrl)
      }
    }

    return {
      id: campaignId,
      name: campaignName,
      daily_budget: dailyBudget,
      channel_type: channelType,
      locations,
      ad_groups: Array.from(adGroupsMap.values()),
      asset_groups: Array.from(assetGroupsMap.values())
    }
  } catch (e) {
    console.warn(`⚠️ [ads-detail] Google campaign ${campaignId} error:`, (e as Error).message)
    return null
  }
}

// --- Historique des modifications Meta (activities) ---
async function fetchMetaAccountHistory(accountId: string, token: string, filterObjectIds?: Set<string>): Promise<any[]> {
  try {
    const since = Math.floor(Date.now() / 1000) - 90 * 24 * 60 * 60 // 90 jours
    const url = `https://graph.facebook.com/v21.0/${accountId}/activities?fields=event_time,event_type,extra_data,object_id,object_name,actor_name&since=${since}&limit=100&access_token=${token}`
    const r = await fetch(url)
    const data = await r.json() as any
    if (data.error || !data.data) return []
    const events = (data.data as any[])
    // Filtrer par object_id si spécifié (ne garder que les events liés aux adsets/campagnes du client)
    const filtered = filterObjectIds && filterObjectIds.size > 0
      ? events.filter((ev: any) => !ev.object_id || filterObjectIds.has(String(ev.object_id)))
      : events
    return filtered.map((ev: any) => {
      let description = ev.event_type || 'Modification'
      let details: string | undefined
      // Traduire les event_type courants
      const typeMap: Record<string, string> = {
        'update_campaign_budget': 'Modification budget campagne',
        'update_ad_set_budget': 'Modification budget ensemble pub',
        'update_ad_set_target': 'Modification ciblage',
        'create_ad': 'Création publicité',
        'update_ad_creative': 'Modification créative',
        'update_campaign_run_status': 'Changement statut campagne',
        'update_ad_set_run_status': 'Changement statut ensemble pub',
        'update_ad_run_status': 'Changement statut publicité',
        'create_campaign': 'Création campagne',
        'create_ad_set': 'Création ensemble pub',
        'delete_campaign': 'Suppression campagne',
        'delete_ad_set': 'Suppression ensemble pub',
        'delete_ad': 'Suppression publicité',
        'update_ad_set_bid_adjustments': 'Modification enchères',
        'update_campaign_budget_remaining': 'Modification budget restant',
      }
      description = typeMap[ev.event_type] || ev.event_type?.replace(/_/g, ' ') || 'Modification'
      if (ev.extra_data) {
        try {
          const extra = typeof ev.extra_data === 'string' ? JSON.parse(ev.extra_data) : ev.extra_data
          if (extra.old_value !== undefined && extra.new_value !== undefined) {
            details = `${extra.old_value} → ${extra.new_value}`
          }
        } catch { /* ignore */ }
      }
      return {
        date: ev.event_time,
        source: 'meta' as const,
        type: ev.event_type || 'unknown',
        entity_name: ev.object_name || '',
        description,
        details,
        actor: ev.actor_name || undefined
      }
    })
  } catch (e) {
    console.warn(`⚠️ [ads-history] Meta activities error for ${accountId}:`, (e as Error).message)
    return []
  }
}

// --- Historique des modifications Google (change_event) ---
async function fetchGoogleChangeEvents(campaignIds: string[], customerId: string, accessToken: string): Promise<any[]> {
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
  if (!devToken) return []
  try {
    const campaignFilter = campaignIds.length === 1
      ? `AND campaign.id = ${campaignIds[0]}`
      : `AND campaign.id IN (${campaignIds.join(',')})`
    const query = `SELECT change_event.change_date_time, change_event.change_resource_type, change_event.changed_fields, change_event.user_email, change_event.client_type, change_event.resource_change_operation, campaign.name FROM change_event WHERE change_event.change_date_time DURING LAST_30_DAYS ${campaignFilter} ORDER BY change_event.change_date_time DESC LIMIT 100`
    const r = await fetch(`https://googleads.googleapis.com/v23/customers/${customerId}/googleAds:searchStream`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'developer-token': devToken,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ query })
    })
    const data = await r.json() as any
    if (data.error || !data[0]?.results) return []
    const resourceTypeMap: Record<string, string> = {
      'CAMPAIGN': 'Campagne',
      'AD_GROUP': 'Groupe d\'annonces',
      'AD_GROUP_AD': 'Annonce',
      'CAMPAIGN_BUDGET': 'Budget campagne',
      'CAMPAIGN_CRITERION': 'Ciblage campagne',
      'AD_GROUP_CRITERION': 'Ciblage groupe',
      'ASSET': 'Asset',
      'ASSET_GROUP': 'Groupe d\'assets',
      'ASSET_GROUP_ASSET': 'Asset de groupe',
    }
    const opMap: Record<string, string> = {
      'CREATE': 'Création',
      'UPDATE': 'Modification',
      'REMOVE': 'Suppression',
    }
    return (data[0].results as any[]).map((row: any) => {
      const ce = row.changeEvent || {}
      const resType = resourceTypeMap[ce.changeResourceType] || ce.changeResourceType || ''
      const op = opMap[ce.resourceChangeOperation] || ce.resourceChangeOperation || ''
      const changedFields = ce.changedFields?.paths?.join(', ') || ''
      return {
        date: ce.changeDateTime,
        source: 'google' as const,
        type: `${ce.resourceChangeOperation}_${ce.changeResourceType}`,
        entity_name: row.campaign?.name || '',
        description: `${op} ${resType}`,
        details: changedFields ? `Champs: ${changedFields}` : undefined,
        actor: ce.userEmail || (ce.clientType === 'GOOGLE_INTERNAL' ? 'Google' : ce.clientType) || undefined
      }
    })
  } catch (e) {
    console.warn(`⚠️ [ads-history] Google change_event error for ${customerId}:`, (e as Error).message)
    return []
  }
}

// --- Historique interne (modifications faites via le backoffice) ---
async function fetchInternalHistory(entityIds: string[]): Promise<any[]> {
  if (!entityIds.length) return []
  try {
    const { data, error } = await supabaseAdmin
      .from('ads_audit_log')
      .select('*')
      .in('entity_id', entityIds)
      .order('created_at', { ascending: false })
      .limit(100)
    if (error || !data) return []
    return data.map((row: any) => ({
      date: row.created_at,
      source: (row.source || 'internal') as 'meta' | 'google' | 'internal',
      type: row.action || 'budget_change',
      entity_name: row.entity_name || '',
      description: row.details || `Budget modifié à ${row.new_value}€`,
      details: row.old_value && row.new_value ? `${row.old_value}€ → ${row.new_value}€` : undefined,
      actor: 'Backoffice'
    }))
  } catch {
    return [] // table might not exist yet
  }
}

// Endpoint pour récupérer les détails complets (ciblage, pubs, images) + historique
app.get('/api/ads-detail', async (req, res) => {
  const metaIds = ((req.query.metaIds as string) || '').split(',').filter(Boolean)
  const googleIds = ((req.query.googleIds as string) || '').split(',').filter(Boolean)
  const adKeys = ((req.query.adKeys as string) || '').split(',').filter(Boolean)

  console.log(`🔍 [ads-detail] Request: metaIds=${metaIds.length}, googleIds=${googleIds.length}, adKeys=${adKeys.length}`)
  console.log(`🔍 [ads-detail] Maps: metaCred=${metaAdsetCredentials.size}, googleCred=${googleCampaignCredentials.size}, metaNorm=${metaNormalizedToIds.size}, googleNorm=${googleNormalizedToIds.size}`)

  // Résoudre les IDs manquants via les normalized name maps
  if (adKeys.length > 0) {
    for (const key of adKeys) {
      const mIds = metaNormalizedToIds.get(key) || []
      for (const id of mIds) { if (!metaIds.includes(id)) metaIds.push(id) }
      const gIds = googleNormalizedToIds.get(key) || []
      for (const id of gIds) { if (!googleIds.includes(id)) googleIds.push(id) }
    }
  }

  console.log(`🔍 [ads-detail] After resolve: metaIds=${metaIds.length}, googleIds=${googleIds.length}`)

  try {
    // Collect unique Meta account IDs + tokens for history
    const metaAccounts = new Map<string, string>()
    for (const id of metaIds) {
      const creds = metaAdsetCredentials.get(id)
      if (creds && !metaAccounts.has(creds.accountId)) metaAccounts.set(creds.accountId, creds.token)
    }

    // Collect unique Google customer IDs for history
    const googleByCustomer = new Map<string, string[]>()
    for (const id of googleIds) {
      const creds = googleCampaignCredentials.get(id)
      if (creds) {
        if (!googleByCustomer.has(creds.customerId)) googleByCustomer.set(creds.customerId, [])
        googleByCustomer.get(creds.customerId)!.push(id)
      }
    }

    // Get Google access token (needed for details + history)
    let googleAccessToken: string | null = null
    const gClientId = process.env.GOOGLE_ADS_CLIENT_ID
    const gClientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
    const gRefreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
    if (gClientId && gClientSecret && gRefreshToken && (googleIds.length > 0)) {
      const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `client_id=${gClientId}&client_secret=${gClientSecret}&refresh_token=${gRefreshToken}&grant_type=refresh_token`
      })
      const tokenData = await tokenRes.json() as any
      googleAccessToken = tokenData.access_token || null
    }

    // Fetch EVERYTHING in parallel: details + history
    const [
      metaDetails,
      googleDetails,
      metaHistoryArrays,
      googleHistoryArrays,
      internalHistory
    ] = await Promise.all([
      // Meta adset details
      Promise.all(metaIds.map(fetchMetaAdsetDetail)),
      // Google campaign details
      googleAccessToken
        ? Promise.all(googleIds.map(id => fetchGoogleCampaignDetail(id, googleAccessToken!)))
        : Promise.resolve([]),
      // Meta history (one call per account, filtered to client's adset IDs)
      Promise.all(Array.from(metaAccounts.entries()).map(([accId, token]) => fetchMetaAccountHistory(accId, token, new Set(metaIds)))),
      // Google history (one call per customer)
      googleAccessToken
        ? Promise.all(Array.from(googleByCustomer.entries()).map(([cid, campIds]) => fetchGoogleChangeEvents(campIds, cid, googleAccessToken!)))
        : Promise.resolve([]),
      // Internal history (our own changes)
      fetchInternalHistory([...metaIds, ...googleIds])
    ])

    // Combine and sort history by date DESC
    const history = [
      ...metaHistoryArrays.flat(),
      ...googleHistoryArrays.flat(),
      ...internalHistory
    ].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())

    res.json({
      meta: metaDetails.filter(Boolean),
      google: googleDetails.filter(Boolean),
      history
    })
  } catch (e) {
    console.error('❌ [ads-detail] Error:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// --- OAuth Google Ads : regénérer le refresh token ---
// Utilise les credentials Gmail (qui ont un redirect_uri enregistré) avec le scope Google Ads
app.get('/api/google-ads/auth', async (_req, res) => {
  try {
    const credPath = path.join(process.cwd(), 'gmail-credentials.json')
    const creds = JSON.parse(await import('fs').then(fs => fs.promises.readFile(credPath, 'utf-8')))
    const { client_id, client_secret, redirect_uris } = creds.installed || creds.web
    const redirectUri = redirect_uris.find((u: string) => u.includes('localhost')) || redirect_uris[0]
    const scope = 'https://www.googleapis.com/auth/adwords'
    const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${client_id}&redirect_uri=${encodeURIComponent(redirectUri)}&response_type=code&scope=${encodeURIComponent(scope)}&access_type=offline&prompt=consent`
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Google Ads OAuth</title><style>body{font-family:system-ui;max-width:700px;margin:40px auto;padding:0 20px}pre{background:#f0f0f0;padding:12px;border-radius:8px;word-break:break-all;font-size:13px}code{background:#e8e8e8;padding:2px 6px;border-radius:4px}.info{background:#e8f4fd;padding:12px;border-radius:8px;margin:12px 0;font-size:13px}</style></head><body>
<h2>Renouveler le token Google Ads</h2>
<p>Clique sur le lien ci-dessous pour autoriser l'acces Google Ads :</p>
<p><a href="${authUrl}" style="display:inline-block;padding:12px 24px;background:#4285f4;color:#fff;border-radius:6px;text-decoration:none;font-weight:bold">Se connecter avec Google</a></p>
<div class="info">Apres autorisation, tu seras redirige automatiquement. Le nouveau refresh token sera affiche.<br>
Il faudra ensuite mettre a jour <code>GOOGLE_ADS_REFRESH_TOKEN</code> et <code>GOOGLE_ADS_CLIENT_ID</code> / <code>GOOGLE_ADS_CLIENT_SECRET</code> dans le <code>.env</code> :<br>
<pre>GOOGLE_ADS_CLIENT_ID=${client_id}
GOOGLE_ADS_CLIENT_SECRET=${client_secret}</pre></div>
</body></html>`)
  } catch (e) {
    res.status(500).send(`Erreur: impossible de lire gmail-credentials.json: ${(e as Error).message}`)
  }
})

// Le callback arrive sur /api/invoices/auth/callback (redirect_uri enregistré dans Gmail credentials)
// On ajoute un handler qui détecte le scope adwords et redirige vers l'exchange Google Ads
app.get('/api/google-ads/exchange', async (req, res) => {
  let code = (req.query.code as string || '').trim()
  if (!code) return res.status(400).send('Code manquant')
  const codeMatch = code.match(/[?&]code=([^&]+)/)
  if (codeMatch) code = decodeURIComponent(codeMatch[1])
  try {
    const credPath = path.join(process.cwd(), 'gmail-credentials.json')
    const creds = JSON.parse(await import('fs').then(fs => fs.promises.readFile(credPath, 'utf-8')))
    const { client_id, client_secret, redirect_uris } = creds.installed || creds.web
    const redirectUri = redirect_uris.find((u: string) => u.includes('localhost')) || redirect_uris[0]
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `code=${encodeURIComponent(code)}&client_id=${client_id}&client_secret=${client_secret}&redirect_uri=${encodeURIComponent(redirectUri)}&grant_type=authorization_code`
    })
    const data = await r.json() as any
    if (data.refresh_token) {
      // Mettre à jour automatiquement le .env avec le nouveau refresh token
      try {
        const fs = await import('fs')
        const envPath = path.join(process.cwd(), '.env')
        let envContent = await fs.promises.readFile(envPath, 'utf-8')
        if (envContent.includes('GOOGLE_ADS_REFRESH_TOKEN=')) {
          envContent = envContent.replace(/GOOGLE_ADS_REFRESH_TOKEN=.*/g, `GOOGLE_ADS_REFRESH_TOKEN=${data.refresh_token}`)
        } else {
          envContent += `\nGOOGLE_ADS_REFRESH_TOKEN=${data.refresh_token}\n`
        }
        // Stocker la date de génération pour le suivi d'expiration
        const genTs = new Date().toISOString()
        if (envContent.includes('GOOGLE_ADS_TOKEN_GENERATED_AT=')) {
          envContent = envContent.replace(/^GOOGLE_ADS_TOKEN_GENERATED_AT=.*/m, `GOOGLE_ADS_TOKEN_GENERATED_AT=${genTs}`)
        } else {
          envContent += `\nGOOGLE_ADS_TOKEN_GENERATED_AT=${genTs}\n`
        }
        await fs.promises.writeFile(envPath, envContent)
        process.env.GOOGLE_ADS_REFRESH_TOKEN = data.refresh_token
        process.env.GOOGLE_ADS_TOKEN_GENERATED_AT = genTs
        // Invalider les caches pour forcer un refresh avec le nouveau token
        setCache('ads-budgets-live', null as any, 0)
        invalidateCache('ads-last-edit')
        for (const p of ['all', 'month', '5d', '15d', '30d', '90d']) {
          invalidateCache(`ads-entries-${p}`)
        }
        console.log('✅ [google-ads/exchange] .env mis à jour automatiquement avec le nouveau refresh token + caches invalidés')
      } catch (envErr) {
        console.warn('⚠️ [google-ads/exchange] Impossible de mettre à jour .env:', (envErr as Error).message)
      }
      res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Token OK</title></head><body style="font-family:system-ui;max-width:700px;margin:40px auto;padding:0 20px">
<h2>Token Google Ads renouvele avec succes</h2>
<p style="color:green;font-weight:bold">Le .env a ete mis a jour automatiquement. Les caches ont ete invalides.</p>
<p>Le nouveau token est actif immediatement, pas besoin de redemarrer le serveur.</p>
<p style="margin-top:24px"><a href="/api/debug/google-budgets">Tester les budgets Google</a> | <a href="/">Retour au backoffice</a></p>
</body></html>`)
    } else {
      res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Erreur</title></head><body style="font-family:system-ui;max-width:700px;margin:40px auto;padding:0 20px">
<h2>Pas de refresh_token</h2>
<pre style="background:#fff3f3;padding:16px;border-radius:8px">${JSON.stringify(data, null, 2)}</pre>
<p>Retourne sur <a href="/api/google-ads/auth">/api/google-ads/auth</a> et reessaie.</p>
</body></html>`)
    }
  } catch (e) {
    res.status(500).send(`Erreur: ${(e as Error).message}`)
  }
})

// Diagnostic : chercher un client dans les ads brutes
app.get('/api/debug/ads-search', async (req, res) => {
  const q = (req.query.q as string || '').toLowerCase()
  if (!q) return res.json({ error: 'Paramètre ?q= requis' })

  const { data: rows } = await supabaseAdmin
    .from('ads_stats_daily')
    .select('source, normalized_name, ad_name_raw, spend, account_id, stat_date')
    .ilike('ad_name_raw', `%${q}%`)
    .order('stat_date', { ascending: false })
    .limit(50)

  const { data: rows2 } = await supabaseAdmin
    .from('ads_stats_daily')
    .select('source, normalized_name, ad_name_raw, spend, account_id, stat_date')
    .ilike('normalized_name', `%${q}%`)
    .order('stat_date', { ascending: false })
    .limit(50)

  // Aussi chercher dans les clients
  const { data: clients } = await supabaseAdmin
    .from('pub_stats_cache')
    .select('data')
    .eq('period', 'all')
    .limit(1)

  const clientNames: string[] = []
  if (clients?.[0]?.data?.clients) {
    for (const c of clients[0].data.clients) {
      if (c.client_name?.toLowerCase().includes(q)) {
        clientNames.push(`${c.client_name} (id: ${c.id_client})`)
      }
    }
  }

  // Dédup
  const allRows = [...(rows || []), ...(rows2 || [])]
  const seen = new Set<string>()
  const unique = allRows.filter(r => {
    const key = `${r.source}:${r.ad_name_raw}:${r.account_id}:${r.stat_date}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })

  res.json({
    query: q,
    matchingClients: clientNames,
    adsFound: unique.length,
    ads: unique.map(r => ({
      source: r.source,
      normalized: r.normalized_name,
      raw: r.ad_name_raw,
      spend: parseFloat(r.spend),
      account: r.account_id,
      date: r.stat_date
    }))
  })
})

// Diagnostic : tester le fetch des budgets Google en direct
app.get('/api/debug/google-budgets', async (_req, res) => {
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
  const customerIds = process.env.GOOGLE_ADS_CUSTOMER_IDS?.split(',').map(s => s.trim()).filter(Boolean)

  const diag: any = {
    env: { hasClientId: !!clientId, hasClientSecret: !!clientSecret, hasRefreshToken: !!refreshToken, hasDevToken: !!devToken, customerIds: customerIds || [], clientIdPrefix: clientId?.slice(0, 12), refreshTokenPrefix: refreshToken?.slice(0, 15) },
    tokenRefresh: null as any,
    customers: [] as any[],
    fetchResult: null as any
  }

  if (!clientId || !clientSecret || !refreshToken || !devToken || !customerIds?.length) {
    return res.json({ ...diag, error: 'Config incomplète' })
  }

  try {
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
    })
    const tokenData = await tokenRes.json() as any
    diag.tokenRefresh = { ok: !!tokenData.access_token, error: tokenData.error || null, accessTokenPrefix: tokenData.access_token?.slice(0, 20), devTokenPrefix: devToken?.slice(0, 10), scope: tokenData.scope }
    if (!tokenData.access_token) return res.json(diag)
    const accessToken = tokenData.access_token

    for (const cid of customerIds) {
      const custDiag: any = { customerId: cid }
      try {
        const query = `SELECT campaign.id, campaign.name, campaign_budget.amount_micros, campaign_budget.resource_name FROM campaign WHERE campaign.status IN ('ENABLED', 'PAUSED')`
        const r = await fetch(`https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${accessToken}`, 'developer-token': devToken, 'Content-Type': 'application/json' },
          body: JSON.stringify({ query })
        })
        custDiag.httpStatus = r.status
        if (!r.ok) {
          custDiag.error = (await r.text().catch(() => '')).slice(0, 1500)
        } else {
          const data = await r.json() as any
          if (data.error) {
            custDiag.apiError = data.error
          } else if (Array.isArray(data)) {
            const allResults = data.flatMap((c: any) => c?.results || [])
            custDiag.campaignCount = allResults.length
            custDiag.sampleCampaigns = allResults.slice(0, 3).map((row: any) => ({
              name: row.campaign?.name,
              id: row.campaign?.id,
              budgetMicros: row.campaignBudget?.amountMicros,
              budgetEur: row.campaignBudget?.amountMicros ? parseInt(row.campaignBudget.amountMicros) / 1e6 : null,
              budgetResource: row.campaignBudget?.resourceName
            }))
          } else {
            custDiag.unexpectedFormat = typeof data
            custDiag.rawSample = JSON.stringify(data).slice(0, 500)
          }
        }
      } catch (e) {
        custDiag.exception = (e as Error).message
      }
      diag.customers.push(custDiag)
    }

    const budgets = await fetchGoogleCampaignBudgets()
    diag.fetchResult = {
      count: Object.keys(budgets).length,
      sampleKeys: Object.keys(budgets).slice(0, 5),
      sampleValues: Object.entries(budgets).slice(0, 3).map(([k, v]) => ({ name: k, ...v }))
    }
  } catch (e) {
    diag.globalError = (e as Error).message
  }

  res.json(diag)
})

// Endpoint pour le détail des adsets/campagnes individuels (noms bruts)
app.get('/api/ads-entries', async (req, res) => {
  const period = (req.query.period as string) || 'all'
  console.log(`🔍 [ads-entries] Début (period=${period})`)
  try {
    const cacheKey = `ads-entries-${period}`
    const cached = getCached<any>(cacheKey)
    if (cached) return res.json(cached)

    // Calculer la date de début selon la période
    const now = new Date()
    let sinceDate: string
    if (period === 'month') {
      sinceDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`
    } else if (period === '5d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 5 * 24 * 60 * 60 * 1000))
    } else if (period === '15d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 15 * 24 * 60 * 60 * 1000))
    } else if (period === '30d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000))
    } else if (period === '90d') {
      sinceDate = formatDateYMD(new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000))
    } else {
      sinceDate = '2020-01-01'
    }

    // Lancer RPC Supabase ET budgets en parallèle
    const budgetCacheKey = 'ads-budgets-live'
    const cachedBudgets = getCached<{ meta: Record<string, AdBudgetInfo>; google: Record<string, AdBudgetInfo> }>(budgetCacheKey)

    const [rpcResult, budgetResult] = await Promise.all([
      supabaseAdmin.rpc('get_ads_entries_aggregated', { since_date: sinceDate }),
      cachedBudgets
        ? Promise.resolve(cachedBudgets)
        : Promise.all([
            fetchMetaAdsetBudgets().catch(e => { console.error('❌ [ads-entries] Meta budgets error:', e); return {} as Record<string, AdBudgetInfo> }),
            fetchGoogleCampaignBudgets().catch(e => { console.error('❌ [ads-entries] Google budgets error:', e); return {} as Record<string, AdBudgetInfo> })
          ]).then(([mb, gb]) => {
            const b = { meta: mb, google: gb }
            if (Object.keys(mb).length > 0 && Object.keys(gb).length > 0) {
              setCache(budgetCacheKey, b, CACHE_TTL_LONG_MS)
            } else if (Object.keys(mb).length > 0) {
              setCache(budgetCacheKey, b, 60_000)
            }
            return b
          })
    ])

    let metaBudgets: Record<string, AdBudgetInfo>
    let googleBudgets: Record<string, AdBudgetInfo>
    if ('meta' in budgetResult) {
      metaBudgets = budgetResult.meta
      googleBudgets = budgetResult.google
    } else {
      metaBudgets = budgetResult.meta
      googleBudgets = budgetResult.google
    }
    console.log(`📊 [ads-entries] Budgets: Meta=${Object.keys(metaBudgets).length}, Google=${Object.keys(googleBudgets).length}${cachedBudgets ? ' (cache)' : ' (fetch)'}`)

    const { data: rpcRows, error: rpcError } = rpcResult

    const metaEntries: Record<string, Record<string, { spend: number; leads: number; impressions: number; clicks: number }>> = {}
    const googleEntries: Record<string, Record<string, { spend: number; leads: number; impressions: number; clicks: number }>> = {}

    if (!rpcError && rpcRows) {
      console.log(`📊 [ads-entries] RPC: ${rpcRows.length} lignes agrégées depuis ${sinceDate}`)
      for (const row of rpcRows) {
        const target = row.source === 'meta' ? metaEntries : googleEntries
        if (!target[row.normalized_name]) target[row.normalized_name] = {}
        const entry = target[row.normalized_name]
        if (!entry[row.ad_name_raw]) entry[row.ad_name_raw] = { spend: 0, leads: 0, impressions: 0, clicks: 0 }
        entry[row.ad_name_raw].spend += parseFloat(row.total_spend) || 0
        entry[row.ad_name_raw].leads += parseFloat(row.total_leads) || 0
        entry[row.ad_name_raw].impressions += parseInt(row.total_impressions) || 0
        entry[row.ad_name_raw].clicks += parseInt(row.total_clicks) || 0
      }
    } else {
      // Fallback : requête directe si la RPC n'existe pas encore
      console.warn('⚠️ [ads-entries] RPC error, fallback vers requête directe:', rpcError?.message)
      const { data: fallbackRows, error: fallbackError } = await supabaseAdmin
        .from('ads_stats_daily')
        .select('source, normalized_name, ad_name_raw, spend, impressions, clicks, leads')
        .gte('stat_date', sinceDate)
        .limit(50000)
      if (fallbackError) {
        console.error('❌ [ads-entries] Supabase fallback error:', fallbackError)
        return res.json({ meta: {}, google: {} })
      }
      console.log(`📊 [ads-entries] Fallback: ${fallbackRows?.length || 0} lignes depuis ${sinceDate}`)
      for (const row of (fallbackRows || [])) {
        const target = row.source === 'meta' ? metaEntries : googleEntries
        if (!target[row.normalized_name]) target[row.normalized_name] = {}
        const entry = target[row.normalized_name]
        if (!entry[row.ad_name_raw]) entry[row.ad_name_raw] = { spend: 0, leads: 0, impressions: 0, clicks: 0 }
        entry[row.ad_name_raw].spend += parseFloat(row.spend) || 0
        entry[row.ad_name_raw].leads += parseFloat(row.leads) || 0
        entry[row.ad_name_raw].impressions += parseInt(row.impressions) || 0
        entry[row.ad_name_raw].clicks += parseInt(row.clicks) || 0
      }
    }

    // Diagnostic : comparer les noms Google budget vs Google entries
    const googleBudgetKeys = Object.keys(googleBudgets)
    const googleEntryRawNames = new Set<string>()
    for (const rawMap of Object.values(googleEntries)) {
      for (const rawName of Object.keys(rawMap)) googleEntryRawNames.add(rawName)
    }
    if (googleEntryRawNames.size > 0 || googleBudgetKeys.length > 0) {
      const matched = googleBudgetKeys.filter(k => googleEntryRawNames.has(k)).length
      console.log(`🔍 [ads-entries] Google diagnostic: ${googleBudgetKeys.length} budget keys, ${googleEntryRawNames.size} entry raw names, ${matched} direct matches`)
      if (matched === 0 && googleBudgetKeys.length > 0 && googleEntryRawNames.size > 0) {
        console.log(`  Budget keys (3 premiers): ${googleBudgetKeys.slice(0, 3).map(k => JSON.stringify(k)).join(', ')}`)
        console.log(`  Entry raw names (3 premiers): ${[...googleEntryRawNames].slice(0, 3).map(k => JSON.stringify(k)).join(', ')}`)
      }
    }

    // Index inversé : nom normalisé → budget info (fallback si le raw name ne matche pas directement)
    const buildNormalizedBudgetIndex = (budgets: Record<string, AdBudgetInfo>) => {
      const index = new Map<string, AdBudgetInfo>()
      for (const [name, info] of Object.entries(budgets)) {
        const norm = normalizeAdName(name)
        if (norm && !index.has(norm)) index.set(norm, info)
      }
      return index
    }
    const googleBudgetsNormalized = buildNormalizedBudgetIndex(googleBudgets)

    // Formater en tableaux avec budgets quotidiens
    const formatEntries = (source: Record<string, Record<string, any>>, budgets: Record<string, AdBudgetInfo>, normalizedIndex?: Map<string, AdBudgetInfo>) => {
      const result: Record<string, { raw_name: string; spend: number; leads: number; impressions: number; clicks: number; daily_budget: number | null; url: string | null; id: string | null; status: string | null }[]> = {}
      for (const [normName, rawMap] of Object.entries(source)) {
        result[normName] = Object.entries(rawMap).map(([rawName, metrics]) => {
          // Match direct par nom brut
          let budget = budgets[rawName]
          // Fallback : match par nom normalisé (utile pour Google où les noms peuvent légèrement différer)
          if (!budget && normalizedIndex) {
            budget = normalizedIndex.get(normalizeAdName(rawName)) || normalizedIndex.get(normName) as AdBudgetInfo | undefined
          }
          return {
            raw_name: rawName,
            ...metrics,
            daily_budget: budget?.daily_budget ?? null,
            url: budget?.url ?? null,
            id: budget?.id ?? null,
            status: budget?.status ?? null
          }
        })
      }
      return result
    }

    const data = { meta: formatEntries(metaEntries, metaBudgets), google: formatEntries(googleEntries, googleBudgets, googleBudgetsNormalized) }

    // Ajouter les adsets Meta actifs qui ont un budget mais 0 dépense dans la période
    // (ex: adset "Ostium Immobilier" actif avec budget mais sans impressions)
    const existingMetaNorms = new Set(Object.keys(data.meta))
    let addedBudgetOnly = 0
    for (const [rawName, budget] of Object.entries(metaBudgets)) {
      const norm = normalizeAdName(rawName)
      if (!norm || existingMetaNorms.has(norm)) continue
      // Ajouter comme entry avec 0 spend
      if (!data.meta[norm]) data.meta[norm] = []
      data.meta[norm].push({
        raw_name: rawName,
        spend: 0,
        leads: 0,
        impressions: 0,
        clicks: 0,
        daily_budget: budget.daily_budget,
        url: budget.url,
        id: budget.id,
        status: budget.status ?? null
      })
      addedBudgetOnly++
    }

    // Idem pour Google : ajouter les campagnes actives avec budget mais 0 dépense dans la période
    const existingGoogleNorms = new Set(Object.keys(data.google))
    let addedGoogleBudgetOnly = 0
    for (const [rawName, budget] of Object.entries(googleBudgets)) {
      const norm = normalizeAdName(rawName)
      if (!norm || existingGoogleNorms.has(norm)) continue
      if (!data.google[norm]) data.google[norm] = []
      data.google[norm].push({
        raw_name: rawName,
        spend: 0,
        leads: 0,
        impressions: 0,
        clicks: 0,
        daily_budget: budget.daily_budget,
        url: budget.url,
        id: budget.id,
        status: budget.status ?? null
      })
      addedGoogleBudgetOnly++
    }

    console.log(`✅ [ads-entries] Meta: ${Object.keys(data.meta).length} noms, Google: ${Object.keys(data.google).length} noms (budgets: Meta=${Object.keys(metaBudgets).length}, Google=${Object.keys(googleBudgets).length}, budget-only ajoutés: Meta=${addedBudgetOnly}, Google=${addedGoogleBudgetOnly})`)
    setCache(cacheKey, data, CACHE_TTL_MS)
    res.json(data)
  } catch (e) {
    console.error('❌ [ads-entries] Erreur:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// Endpoint CPL mensuel vendeur pour un client (budget / leads vendeur par mois)
app.post('/api/ads-cpl-monthly', async (req, res) => {
  const { clientId, budgetMensuel, months = 12 } = req.body as { clientId: string; budgetMensuel: number; months?: number }
  if (!clientId || budgetMensuel == null) {
    return res.status(400).json({ error: 'clientId and budgetMensuel required' })
  }
  if (!dbPool) {
    return res.status(503).json({ error: 'Database not connected' })
  }
  try {
    const now = new Date()
    const startDate = new Date(now.getFullYear(), now.getMonth() - months + 1, 1)
    const sinceDate = `${startDate.getFullYear()}-${String(startDate.getMonth() + 1).padStart(2, '0')}-01`

    // Leads vendeur par mois (même filtre que computePubStats)
    const leadsQuery = `
      SELECT TO_CHAR(p.created_date, 'YYYY-MM') as month, COUNT(p.id_property)::integer as leads
      FROM agency a
      JOIN property p ON p.id_agency = a.id
      WHERE a.id_client = $1
        AND p.origin = 'estimator'
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND p.created_date >= $2::date
      GROUP BY TO_CHAR(p.created_date, 'YYYY-MM')
    `
    const leadsResult = await dbPool.query(leadsQuery, [clientId, sinceDate])
    const leadsMap: Record<string, number> = {}
    for (const row of leadsResult.rows) {
      leadsMap[row.month] = row.leads || 0
    }

    // Résultat ordonné avec gap-filling
    const result: { month: string; budget: number; leads: number; cpl: number | null }[] = []
    const cursor = new Date(startDate)
    while (cursor <= now) {
      const key = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, '0')}`
      const leads = leadsMap[key] || 0
      // Pour le mois en cours, proratiser le budget au nombre de jours écoulés
      let budget = budgetMensuel
      if (key === `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`) {
        const dayOfMonth = now.getDate()
        const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate()
        budget = Math.round(budgetMensuel * (dayOfMonth / daysInMonth) * 100) / 100
      }
      result.push({
        month: key,
        budget: Math.round(budget * 100) / 100,
        leads,
        cpl: leads > 0 && budget > 0 ? Math.round((budget / leads) * 100) / 100 : null
      })
      cursor.setMonth(cursor.getMonth() + 1)
    }

    res.json(result)
  } catch (e) {
    console.error('❌ [ads-cpl-monthly] Error:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// Endpoint pour modifier le budget quotidien d'un adset Meta ou d'une campagne Google
app.patch('/api/ads-budget', async (req, res) => {
  const { source, id, daily_budget, entity_name, old_budget } = req.body as { source: string; id: string; daily_budget: number; entity_name?: string; old_budget?: number }
  console.log(`🔧 [ads-budget] Modification: source=${source}, id=${id}, budget=${daily_budget}€`)

  if (!source || !id || daily_budget == null || daily_budget < 0) {
    return res.status(400).json({ error: 'Paramètres manquants ou invalides (source, id, daily_budget)' })
  }

  try {
    if (source === 'meta') {
      const creds = metaAdsetCredentials.get(id)
      if (!creds) {
        return res.status(404).json({ error: `Adset Meta ${id} introuvable. Rafraîchissez la page pour recharger les données.` })
      }
      // Meta daily_budget est en centimes
      const budgetCents = Math.round(daily_budget * 100)
      const url = `https://graph.facebook.com/v21.0/${id}?daily_budget=${budgetCents}&access_token=${creds.token}`
      const r = await fetch(url, { method: 'POST' })
      const data = await r.json() as any
      if (data.error) {
        console.error(`❌ [ads-budget] Meta error:`, data.error)
        return res.status(400).json({ error: data.error.message || 'Erreur Meta API' })
      }
      console.log(`✅ [ads-budget] Meta adset ${id} mis à jour: ${daily_budget}€/j`)
    } else if (source === 'google') {
      const creds = googleCampaignCredentials.get(id)
      if (!creds) {
        return res.status(404).json({ error: `Campagne Google ${id} introuvable. Rafraîchissez la page pour recharger les données.` })
      }

      const clientId = process.env.GOOGLE_ADS_CLIENT_ID
      const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
      const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
      const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
      if (!clientId || !clientSecret || !refreshToken || !devToken) {
        return res.status(500).json({ error: 'Google Ads credentials manquantes' })
      }

      // Obtenir un access token frais
      const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
      })
      const tokenData = await tokenRes.json() as any
      if (!tokenData.access_token) {
        return res.status(500).json({ error: 'Impossible d\'obtenir un token Google' })
      }

      // Mettre à jour le budget via campaignBudgets:mutate
      const amountMicros = Math.round(daily_budget * 1e6).toString()
      const mutateUrl = `https://googleads.googleapis.com/v23/customers/${creds.customerId}/campaignBudgets:mutate`
      const r = await fetch(mutateUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${tokenData.access_token}`,
          'developer-token': devToken,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          operations: [{
            update: {
              resourceName: creds.budgetResourceName,
              amountMicros
            },
            updateMask: 'amountMicros'
          }]
        })
      })
      const data = await r.json() as any
      if (data.error) {
        console.error(`❌ [ads-budget] Google error:`, data.error)
        return res.status(400).json({ error: data.error.message || 'Erreur Google Ads API' })
      }
      console.log(`✅ [ads-budget] Google campagne ${id} budget mis à jour: ${daily_budget}€/j`)
    } else {
      return res.status(400).json({ error: `Source inconnue: ${source}` })
    }

    // Invalider les caches pour forcer un refresh
    setCache('ads-budgets-live', null as any, 0)
    invalidateCache('ads-last-edit')
    // Invalider aussi les caches ads-entries pour toutes les périodes
    for (const p of ['all', 'month', '5d', '15d', '30d', '90d']) {
      invalidateCache(`ads-entries-${p}`)
    }

    // Log dans la table d'audit (fail silently si table inexistante)
    try {
      await supabaseAdmin.from('ads_audit_log').insert({
        source,
        action: 'budget_change',
        entity_id: id,
        entity_name: entity_name || null,
        old_value: old_budget != null ? String(old_budget) : null,
        new_value: String(daily_budget),
        details: `Budget quotidien ${old_budget != null ? `${old_budget}€ → ` : ''}${daily_budget}€`
      })
    } catch (e) {
      console.warn('⚠️ [ads-budget] Audit log error (table might not exist):', (e as Error).message)
    }

    res.json({ success: true, daily_budget })
  } catch (e) {
    console.error('❌ [ads-budget] Erreur:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// ===================== PUB STATS COMPUTE & SYNC =====================

async function computePubStats(period: string): Promise<{ clients: any[]; summary: any }> {
  if (!dbPool) {
    throw new Error('Database not connected')
  }

  // Filtre de date selon la période
  let dateFilter = ''
  if (period === 'month') {
    dateFilter = `AND p.created_date >= date_trunc('month', CURRENT_DATE)`
  } else if (period === '5d') {
    dateFilter = `AND p.created_date >= CURRENT_DATE - INTERVAL '5 days'`
  } else if (period === '15d') {
    dateFilter = `AND p.created_date >= CURRENT_DATE - INTERVAL '15 days'`
  } else if (period === '30d') {
    dateFilter = `AND p.created_date >= CURRENT_DATE - INTERVAL '30 days'`
  } else if (period === '90d') {
    dateFilter = `AND p.created_date >= CURRENT_DATE - INTERVAL '90 days'`
  }

  // 1. Récupérer les données V2 (matching par id_gocardless + nom)
  let v2Data: V2Data = { byGocardless: new Map(), byName: new Map() }
  try {
    v2Data = await fetchV2AgenciesData()
  } catch (e) {
    console.warn('⚠️ [pub-stats] Impossible de récupérer les données V2:', e)
  }

  // 1b. Récupérer les clients estimateur V2 pour déterminer la version
  const v2EstimateurClientIds = await fetchEstimateurAgencies()

  // 2. Requête SQL alignée sur sync-agency-stats
  const query = `
    SELECT
      c.id_client,
      c.name as client_name,
      c.locale,
      CASE
        WHEN c.id_gocardless IS NOT NULL AND jsonb_typeof(c.id_gocardless::jsonb) = 'array'
        THEN c.id_gocardless::jsonb->>0
        ELSE c.id_gocardless::text
      END as id_gocardless,
      COUNT(p.id_property)::integer as nb_leads_total,
      COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as nb_leads,
      COUNT(CASE WHEN p.phone IS NOT NULL AND (
        p.reminders_info_done_all > 0
        OR p.status IN ('contacted', 'signed', 'refused', 'sold')
        OR (p.signed_status IS NOT NULL AND p.signed_status != 'no' AND p.signed_status != '')
        OR (p.refusal IS NOT NULL AND p.refusal != '' AND p.refusal != 'no')
      ) THEN 1 END)::integer as leads_contacted,
      COUNT(CASE WHEN p.phone IS NOT NULL AND p.reminders_info_to_do_all > 0 THEN 1 END)::integer as leads_with_reminder,
      COALESCE(AVG(CASE WHEN p.phone IS NOT NULL THEN p.reminders_info_done_all END), 0) as avg_reminders_done,
      COUNT(CASE WHEN p.signed_status != 'no' AND p.signed_status != '' AND p.origin = 'estimator' THEN 1 END)::integer as mandats_signed,
      COUNT(CASE WHEN p.phone_valid = 'validated' THEN 1 END)::integer as nb_leads_validated_phone,
      COUNT(CASE WHEN p.property_type = 'apartment' THEN 1 END)::integer as nb_apartments,
      COUNT(CASE WHEN p.property_type = 'house' THEN 1 END)::integer as nb_houses,
      COUNT(CASE WHEN p.origin = 'estimator' THEN 1 END)::integer as nb_leads_vendeur,
      COUNT(CASE WHEN p.origin = 'estimator' AND p.phone IS NOT NULL THEN 1 END)::integer as nb_leads_vendeur_with_phone,
      MIN(p.created_date) as first_lead_date,
      MAX(p.created_date) as last_lead_date
    FROM client c
    LEFT JOIN agency a ON a.id_client = c.id_client
    LEFT JOIN property p ON p.id_agency = a.id
      AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
      AND p.origin = 'estimator'
      ${dateFilter}
    GROUP BY c.id_client, c.name, c.locale, c.id_gocardless::text
  `

  const result = await dbPool.query(query)

  // 3. Matcher chaque client avec V2 et extraire codes postaux
  const clientPostalCodes = new Map<string, string[]>()
  for (const row of result.rows) {
    const v2Agency = findV2Agency(row, v2Data)
    if (v2Agency?.tarifs) {
      const codes: string[] = []
      for (const tarif of v2Agency.tarifs) {
        if (tarif.code_postal) {
          codes.push(...tarif.code_postal.split(',').map((c: string) => c.trim()).filter((c: string) => /^\d{5}$/.test(c) && c !== '00000'))
        }
      }
      const unique = [...new Set(codes)]
      if (unique.length > 0) clientPostalCodes.set(row.id_client, unique)
    }
  }

  // 4. Stats zone par batch (codes postaux V2) — avec détail par CP
  const zoneQuery = `
    SELECT
      p.postal_code,
      COUNT(p.id_property)::integer as nb_leads_zone_total,
      COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as nb_leads_zone,
      COUNT(CASE WHEN p.phone IS NOT NULL AND p.phone_valid = 'validated' THEN 1 END)::integer as nb_leads_zone_phone_valid
    FROM agency a
    JOIN property p ON p.id_agency = a.id
    WHERE a.id_client = $1
      AND p.postal_code = ANY($2)
      AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
      AND p.origin = 'estimator'
      ${dateFilter}
    GROUP BY p.postal_code
  `

  const zoneStats = new Map<string, { nb_leads_zone_total: number; nb_leads_zone: number; nb_leads_zone_phone_valid: number }>()
  const leadsPerCpMap = new Map<string, Record<string, number>>() // id_client → { cp: nb_leads_vendeur }
  const entries = Array.from(clientPostalCodes.entries())
  const BATCH_SIZE = 10

  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE)
    const results = await Promise.all(
      batch.map(([clientId, postalCodes]) =>
        dbPool!.query(zoneQuery, [clientId, postalCodes]).then(r => ({ clientId, rows: r.rows }))
      )
    )
    for (const { clientId, rows } of results) {
      if (rows && rows.length > 0) {
        // Agréger pour les totaux zone (compatibilité)
        let totalZone = 0, totalZonePhone = 0, totalZonePhoneValid = 0
        const cpLeads: Record<string, number> = {}
        for (const row of rows) {
          totalZone += row.nb_leads_zone_total || 0
          totalZonePhone += row.nb_leads_zone || 0
          totalZonePhoneValid += row.nb_leads_zone_phone_valid || 0
          if (row.postal_code) cpLeads[row.postal_code] = row.nb_leads_zone_total || 0
        }
        zoneStats.set(clientId, { nb_leads_zone_total: totalZone, nb_leads_zone: totalZonePhone, nb_leads_zone_phone_valid: totalZonePhoneValid })
        leadsPerCpMap.set(clientId, cpLeads)
      }
    }
  }

  // 4b. Leads vendeurs sur les 12 derniers mois PAR MOIS (pour exclure le mois le plus cher si > 3 mois)
  const leads12mMonthlyQuery = `
    SELECT a.id_client,
           TO_CHAR(p.created_date, 'YYYY-MM') as month,
           COUNT(p.id_property)::integer as nb_leads
    FROM agency a
    JOIN property p ON p.id_agency = a.id
    WHERE p.origin = 'estimator'
      AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
      AND p.created_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY a.id_client, TO_CHAR(p.created_date, 'YYYY-MM')
  `
  const leads12mMonthlyResult = await dbPool.query(leads12mMonthlyQuery)
  // Regrouper : id_client → [{ month, nb_leads }]
  const leads12mMonthlyMap = new Map<string, { month: string; nb_leads: number }[]>()
  for (const row of leads12mMonthlyResult.rows) {
    if (!leads12mMonthlyMap.has(row.id_client)) leads12mMonthlyMap.set(row.id_client, [])
    leads12mMonthlyMap.get(row.id_client)!.push({ month: row.month, nb_leads: row.nb_leads || 0 })
  }

  // 5. Enrichir les résultats
  const clients = result.rows.map((row: any) => {
    const v2Agency = findV2Agency(row, v2Data)

    // Stats de base (alignées sync-agency-stats)
    const nbLeadsTotal = row.nb_leads_total || 0
    const nbLeads = row.nb_leads || 0
    const leadsContacted = row.leads_contacted || 0
    const leadsWithReminder = row.leads_with_reminder || 0
    const avgRemindersDone = row.avg_reminders_done ? Math.round(parseFloat(row.avg_reminders_done) * 10) / 10 : 0
    const mandatsSigned = row.mandats_signed || 0
    const nbLeadsVendeur = row.nb_leads_vendeur || 0

    const pctLeadContacte = nbLeads > 0 ? Math.round((leadsContacted / nbLeads) * 100 * 10) / 10 : 0
    const pctRelancePrevu = nbLeads > 0 ? Math.round((leadsWithReminder / nbLeads) * 100 * 10) / 10 : 0

    // Stats zone (codes postaux V2)
    const zone = zoneStats.get(row.id_client)
    const nbLeadsZoneTotal = zone?.nb_leads_zone_total || 0
    const nbLeadsZone = zone?.nb_leads_zone || 0
    const nbLeadsZonePhoneValid = zone?.nb_leads_zone_phone_valid || 0

    // Codes postaux et tarifs depuis V2 (comme sync)
    const postalCodesV2: string[] = []
    const tarifsV2: { code_postal: string; tarif: string; places: number }[] = []
    let budgetTotal = 0
    if (v2Agency?.tarifs) {
      for (const tarif of v2Agency.tarifs) {
        if (tarif.code_postal) {
          const codes = tarif.code_postal.split(',').map((c: string) => c.trim()).filter((c: string) => /^\d{5}$/.test(c) && c !== '00000')
          postalCodesV2.push(...codes)
          // Compter les places par CP (un CP répété = plusieurs places)
          const cpCounts = new Map<string, number>()
          for (const code of codes) {
            cpCounts.set(code, (cpCounts.get(code) || 0) + 1)
          }
          const tarifParCode = codes.length > 0 ? (parseFloat(tarif.tarif) / codes.length).toFixed(2) : tarif.tarif
          for (const [code, places] of cpCounts) {
            tarifsV2.push({ code_postal: code, tarif: tarifParCode, places })
          }
          budgetTotal += parseFloat(tarif.tarif) || 0
        }
      }
    }
    const uniquePostalCodes = [...new Set(postalCodesV2)]
    const nombreLogements = v2Agency?.nombre_logements ?? null

    // Nombre de mois d'abonnement et budget global
    const agencyStartDate = v2Agency?.startDate || null
    let nbMois = 0
    if (agencyStartDate) {
      const now = new Date()
      nbMois = Math.max(1, Math.round((now.getTime() - agencyStartDate.getTime()) / (1000 * 60 * 60 * 24 * 30.44)))
    }
    const budgetGlobal = budgetTotal > 0 && nbMois > 0 ? Math.round(budgetTotal * nbMois * 100) / 100 : 0

    // CPL selon la période choisie
    let budgetForCPL = budgetGlobal
    if (period === '30d') {
      budgetForCPL = budgetTotal
    } else if (period === '90d') {
      budgetForCPL = Math.round(budgetTotal * 3 * 100) / 100
    } else if (period === 'month') {
      const now = new Date()
      const dayOfMonth = now.getDate()
      const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate()
      budgetForCPL = Math.round(budgetTotal * (dayOfMonth / daysInMonth) * 100) / 100
    }
    const cpl = nbLeadsVendeur > 0 && budgetForCPL > 0 ? Math.round((budgetForCPL / nbLeadsVendeur) * 100) / 100 : null

    // CPL moyen sur les 12 derniers mois (en excluant le mois le plus cher si > 3 mois de diffusion)
    const monthlyLeads = leads12mMonthlyMap.get(row.id_client) || []
    const nbMonths12m = Math.min(nbMois, 12)
    let cpl12m: number | null = null
    if (budgetTotal > 0 && monthlyLeads.length > 0) {
      // Calculer le CPL par mois : budget mensuel / leads du mois
      const monthlyCpls = monthlyLeads
        .filter(m => m.nb_leads > 0)
        .map(m => ({ month: m.month, cpl: budgetTotal / m.nb_leads, leads: m.nb_leads }))

      if (monthlyCpls.length > 3) {
        // Exclure le mois avec le CPL le plus haut
        monthlyCpls.sort((a, b) => b.cpl - a.cpl)
        const withoutWorst = monthlyCpls.slice(1)
        const totalLeads = withoutWorst.reduce((s, m) => s + m.leads, 0)
        const totalBudget = budgetTotal * withoutWorst.length
        cpl12m = totalLeads > 0 ? Math.round((totalBudget / totalLeads) * 100) / 100 : null
      } else {
        // <= 3 mois : calcul classique
        const totalLeads = monthlyLeads.reduce((s, m) => s + m.nb_leads, 0)
        const budget12m = budgetTotal * nbMonths12m
        cpl12m = totalLeads > 0 ? Math.round((budget12m / totalLeads) * 100) / 100 : null
      }
    }

    // Pourcentages pub-stats
    const pctApartments = nbLeadsTotal > 0 ? Math.round((row.nb_apartments / nbLeadsTotal) * 100) : 0
    const pctHouses = nbLeadsTotal > 0 ? Math.round((row.nb_houses / nbLeadsTotal) * 100) : 0
    const pctPhone = nbLeadsTotal > 0 ? Math.round((nbLeads / nbLeadsTotal) * 100) : 0
    const pctValidatedPhone = nbLeadsTotal > 0 ? Math.round((row.nb_leads_validated_phone / nbLeadsTotal) * 100) : 0

    // Zone type (basé sur les codes postaux V2 ou DB)
    const postalCodes = uniquePostalCodes.length > 0 ? uniquePostalCodes : []
    let zoneType = 'mixte'
    if (postalCodes.length > 0) {
      const firstCode = postalCodes[0] || ''
      const prefix = firstCode.substring(0, 2)
      const urbanPrefixes = ['75', '69', '13', '31', '33', '59', '06', '34', '44', '67', '92', '93', '94']
      const ruralPrefixes = ['03', '05', '07', '09', '12', '15', '19', '23', '32', '43', '46', '48', '52', '55', '58', '70', '88', '89']
      const mountainPrefixes = ['04', '05', '38', '73', '74', '65', '66']
      const coastalPrefixes = ['06', '13', '17', '29', '30', '33', '34', '35', '40', '44', '50', '56', '64', '66', '76', '83', '85']

      if (urbanPrefixes.includes(prefix)) zoneType = 'ville'
      else if (mountainPrefixes.includes(prefix)) zoneType = 'montagne'
      else if (coastalPrefixes.includes(prefix)) zoneType = 'littoral'
      else if (ruralPrefixes.includes(prefix)) zoneType = 'campagne'
      else zoneType = 'périurbain'
    }

    // Taille de zone
    const zoneSize = uniquePostalCodes.length
    let zoneSizeCategory = 'petite'
    if (zoneSize > 15) zoneSizeCategory = 'grande'
    else if (zoneSize > 5) zoneSizeCategory = 'moyenne'

    return {
      id_client: row.id_client,
      client_name: row.client_name,
      locale: row.locale || 'fr_FR',
      id_gocardless: row.id_gocardless?.replace(/"/g, '') || null,
      estimateur_version: v2EstimateurClientIds.has(row.id_client) ? 'V2' : 'V1',
      nb_leads_total: nbLeadsTotal,
      nb_leads: nbLeads,
      nb_leads_zone_total: nbLeadsZoneTotal,
      nb_leads_zone: nbLeadsZone,
      nb_leads_zone_phone_valid: nbLeadsZonePhoneValid,
      leads_contacted: leadsContacted,
      leads_with_reminder: leadsWithReminder,
      avg_reminders_done: avgRemindersDone,
      mandats_signed: mandatsSigned,
      pct_lead_contacte: pctLeadContacte,
      pct_relance_prevu: pctRelancePrevu,
      nombre_logements: nombreLogements,
      sector_postal_codes: uniquePostalCodes,
      tarifs: tarifsV2,
      nb_leads_vendeur: nbLeadsVendeur,
      nb_leads_validated_phone: row.nb_leads_validated_phone || 0,
      pct_phone: pctPhone,
      pct_validated_phone: pctValidatedPhone,
      nb_apartments: row.nb_apartments || 0,
      nb_houses: row.nb_houses || 0,
      pct_apartments: pctApartments,
      pct_houses: pctHouses,
      zone_size: zoneSize,
      zone_size_category: zoneSizeCategory,
      zone_type: zoneType,
      postal_codes: uniquePostalCodes.slice(0, 10),
      budget_mensuel: budgetTotal,
      nb_mois: nbMois,
      budget_global: budgetGlobal,
      start_date: agencyStartDate?.toISOString() || null,
      end_date: v2Agency?.endDate?.toISOString() || null,
      cpl: cpl,
      cpl_12m: cpl12m,
      first_lead_date: row.first_lead_date,
      last_lead_date: row.last_lead_date,
      leads_per_cp: leadsPerCpMap.get(row.id_client) || {}
    }
  })

  // Filtrer : clients actifs (non résiliés, déjà démarrés), avec codes postaux V2
  const now = new Date()
  const activeClients = clients.filter((c: any) => {
    if (!c.sector_postal_codes || c.sector_postal_codes.length === 0) return false
    if (c.end_date) {
      const endDate = new Date(c.end_date)
      if (endDate < now) return false
    }
    // Exclure les clients dont la date de démarrage est dans le futur
    if (c.start_date) {
      const startDate = new Date(c.start_date)
      if (startDate > now) return false
    }
    return true
  })

  // 6. Calculer les statistiques de synthèse
  const clientsWithCPL = activeClients.filter((c: any) => c.cpl !== null && c.cpl > 0)
  const summary = {
    total_clients: activeClients.length,
    total_leads: activeClients.reduce((sum: number, c: any) => sum + c.nb_leads, 0),
    total_leads_total: activeClients.reduce((sum: number, c: any) => sum + c.nb_leads_total, 0),
    total_mandats_signed: activeClients.reduce((sum: number, c: any) => sum + c.mandats_signed, 0),
    avg_pct_lead_contacte: activeClients.length > 0
      ? Math.round((activeClients.reduce((sum: number, c: any) => sum + c.pct_lead_contacte, 0) / activeClients.length) * 10) / 10
      : 0,
    avg_pct_relance_prevu: activeClients.length > 0
      ? Math.round((activeClients.reduce((sum: number, c: any) => sum + c.pct_relance_prevu, 0) / activeClients.length) * 10) / 10
      : 0,
    avg_pct_phone: activeClients.length > 0
      ? Math.round((activeClients.reduce((sum: number, c: any) => sum + c.pct_phone, 0) / activeClients.length) * 10) / 10
      : 0,
    avg_cpl: clientsWithCPL.length > 0
      ? Math.round((clientsWithCPL.reduce((sum: number, c: any) => sum + c.cpl, 0) / clientsWithCPL.length) * 100) / 100
      : null,
    min_cpl: clientsWithCPL.length > 0
      ? Math.min(...clientsWithCPL.map((c: any) => c.cpl))
      : null,
    max_cpl: clientsWithCPL.length > 0
      ? Math.max(...clientsWithCPL.map((c: any) => c.cpl))
      : null,
    by_zone_type: {
      ville: activeClients.filter((c: any) => c.zone_type === 'ville'),
      campagne: activeClients.filter((c: any) => c.zone_type === 'campagne'),
      montagne: activeClients.filter((c: any) => c.zone_type === 'montagne'),
      littoral: activeClients.filter((c: any) => c.zone_type === 'littoral'),
      périurbain: activeClients.filter((c: any) => c.zone_type === 'périurbain'),
      mixte: activeClients.filter((c: any) => c.zone_type === 'mixte')
    },
    by_zone_size: {
      petite: activeClients.filter((c: any) => c.zone_size_category === 'petite'),
      moyenne: activeClients.filter((c: any) => c.zone_size_category === 'moyenne'),
      grande: activeClients.filter((c: any) => c.zone_size_category === 'grande')
    },
    by_property_type: {
      majority_apartments: activeClients.filter((c: any) => c.pct_apartments > 60),
      majority_houses: activeClients.filter((c: any) => c.pct_houses > 60),
      mixed: activeClients.filter((c: any) => c.pct_apartments <= 60 && c.pct_houses <= 60)
    }
  }

  return { clients: activeClients, summary }
}

async function updatePubStatsSyncMetadata(
  period: string,
  status: string,
  errorMsg: string | null,
  durationMs: number
): Promise<void> {
  await supabaseAdmin
    .from('pub_stats_sync_metadata')
    .update({
      last_sync: new Date().toISOString(),
      last_sync_status: status,
      last_sync_error: errorMsg,
      sync_duration_ms: durationMs
    })
    .eq('period', period)
}

async function syncPubStatsToSupabase(): Promise<void> {
  console.log('🔄 [pub-stats-sync] Démarrage de la synchronisation...')
  const globalStart = Date.now()

  try {
    if (!dbPool) {
      console.warn('⚠️ [pub-stats-sync] Database pool non disponible, skip')
      return
    }

    const periods = ['all', 'month', '5d', '15d', '30d', '90d']

    for (const period of periods) {
      const periodStart = Date.now()
      try {
        console.log(`🔄 [pub-stats-sync] Calcul pour period=${period}...`)
        const responseData = await computePubStats(period)

        const { error } = await supabaseAdmin
          .from('pub_stats_cache')
          .upsert({
            period,
            response_json: responseData,
            synced_at: new Date().toISOString()
          }, {
            onConflict: 'period',
            ignoreDuplicates: false
          })

        const durationMs = Date.now() - periodStart

        if (error) {
          console.error(`❌ [pub-stats-sync] Erreur upsert period=${period}:`, error)
          await updatePubStatsSyncMetadata(period, 'error', error.message, durationMs)
        } else {
          console.log(`✅ [pub-stats-sync] period=${period} synced (${responseData.clients.length} clients, ${durationMs}ms)`)
          await updatePubStatsSyncMetadata(period, 'success', null, durationMs)
        }
      } catch (e) {
        const durationMs = Date.now() - periodStart
        console.error(`❌ [pub-stats-sync] Erreur pour period=${period}:`, e)
        await updatePubStatsSyncMetadata(period, 'error', String(e), durationMs)
      }
    }

    // Invalider le cache mémoire
    invalidateCache('pub-stats')

    const elapsed = ((Date.now() - globalStart) / 1000).toFixed(1)
    console.log(`✅ [pub-stats-sync] Synchronisation terminée en ${elapsed}s`)
  } catch (error) {
    console.error('❌ [pub-stats-sync] Erreur globale:', error)
  }
}

// Endpoint pub-stats avec cache Supabase
app.get('/api/pub-stats', async (req, res) => {
  const period = (req.query.period as string) || 'all'
  console.log(`🔍 [pub-stats] Début de la requête (period=${period})`)
  try {
    // Tier 1: Cache mémoire (le plus rapide)
    const cacheKey = `pub-stats-${period}`
    const cached = getCached<any>(cacheKey)
    if (cached) {
      return res.json(cached)
    }

    // Tier 2: Cache Supabase (pré-calculé)
    try {
      const { data: cacheRow, error } = await supabaseAdmin
        .from('pub_stats_cache')
        .select('response_json, synced_at')
        .eq('period', period)
        .single()

      if (!error && cacheRow?.response_json) {
        console.log(`✅ [pub-stats] Depuis Supabase cache (synced: ${cacheRow.synced_at})`)
        const responseData = cacheRow.response_json
        setCache(cacheKey, responseData, CACHE_TTL_MS)
        return res.json(responseData)
      }
    } catch (e) {
      console.warn('⚠️ [pub-stats] Supabase cache miss, fallback calcul complet')
    }

    // Tier 3: Calcul complet (fallback)
    console.log('🔄 [pub-stats] Fallback vers calcul complet...')
    if (!dbPool) {
      return res.status(503).json({ error: 'Database not connected' })
    }

    const responseData = await computePubStats(period)
    setCache(cacheKey, responseData, CACHE_TTL_MS)
    res.json(responseData)
  } catch (error) {
    console.error('Erreur lors de la récupération des stats pub:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// ============================================================
// INTÉGRITÉ PIPELINE - Funnel complet Estimateur → Logs → V2 → V3
// ============================================================

const SSH_KEY_PATH = process.env.SSH_KEY_PATH || '/tmp/bo_integrity_key_dec'
const AWS_HOST = process.env.AWS_HOST || 'antone.maline-immobilier.fr'
const AWS_USER = process.env.AWS_USER || 'root'
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || 'Flp2VrJ0x5uR'
const SQLITE_FR_PATH = process.env.SQLITE_FR_PATH || '/home/ubuntu/www-newprod/prisma/prisma/prod.db'
const SQLITE_ES_PATH = process.env.SQLITE_ES_PATH || '/home/ubuntu/www-es/estimateur-es/prisma/prisma/prod.db'

// Helper: exécuter une commande SSH sur AWS
// Mode heredoc pour commandes complexes (grep, etc.) - utilise un marqueur pour filtrer la bannière
function sshExec(cmd: string, timeout = 30000): string {
  const marker = '___SSH_OUTPUT_START___'
  const fullCmd = `ssh -i ${SSH_KEY_PATH} -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${AWS_USER}@${AWS_HOST} 2>/dev/null << 'SSHEOF'\necho ${marker}\n${cmd}\nSSHEOF`
  const raw = execSync(fullCmd, { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024, timeout, shell: '/bin/bash' })
  const markerIdx = raw.indexOf(marker)
  if (markerIdx === -1) return raw
  return raw.substring(markerIdx + marker.length).trim()
}

// Connexion MySQL directe (plus de SSH)
const MYSQL_HOST = process.env.EXTERNAL_DB_HOST || '15.236.49.78'
const MYSQL_PORT = parseInt(process.env.EXTERNAL_DB_PORT || '3306')
const MYSQL_USER = process.env.EXTERNAL_DB_USER || 'malineim_produser'
const MYSQL_PASS = process.env.EXTERNAL_DB_PASSWORD || 'kmjhmoanzj6'
const MYSQL_DB = process.env.EXTERNAL_DB_NAME || 'malineim_prod'

let mysqlPool: mysql.Pool | null = null
function getMysqlPool(): mysql.Pool {
  if (!mysqlPool) {
    mysqlPool = mysql.createPool({
      host: MYSQL_HOST, port: MYSQL_PORT, user: MYSQL_USER,
      password: MYSQL_PASS, database: MYSQL_DB,
      waitForConnections: true, connectionLimit: 5, connectTimeout: 10000
    })
  }
  return mysqlPool
}

// Helper: exécuter une commande locale sur le serveur BO (pour SQLite)
function localExec(cmd: string, timeout = 15000): string {
  return execSync(cmd, { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024, timeout })
}

// GET /api/leads/integrity-funnel?date=2026-03-10
// Funnel complet : Estimateur FR/ES → Logs get_price → MySQL V2 → PostgreSQL V3
app.get('/api/leads/integrity-funnel', async (req, res) => {
  try {
    const date = req.query.date as string
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Parameter date required (YYYY-MM-DD)' })
    }

    if (!dbPool) return res.status(503).json({ error: 'Database not connected' })

    // ==== 1. ESTIMATEUR SQLite (FR + ES) ====
    let estimFR = { etape3: 0, etape4: 0, etape3_vendeurs: 0, etape3_non_vendeurs: 0, etape4_vendeurs: 0, etape4_non_vendeurs: 0, details: [] as any[] }
    let estimES = { etape3: 0, etape4: 0, etape3_vendeurs: 0, etape3_non_vendeurs: 0, etape4_vendeurs: 0, etape4_non_vendeurs: 0, details: [] as any[] }

    // Helper: parser le CSV SQLite en objets
    function parseSqliteCsv(raw: string, columns: string[]): any[] {
      if (!raw.trim()) return []
      return raw.trim().split('\n').map(line => {
        const values = line.split('|')
        const obj: any = {}
        columns.forEach((col, i) => {
          const v = values[i] || ''
          obj[col] = (col === 'etape' || col === 'vente') ? parseInt(v, 10) || 0 : v
        })
        return obj
      })
    }

    const sqliteCols = ['uniqueId', 'etape', 'vente', 'idAgence', 'adresse', 'codePostalApi', 'telephone', 'email', 'nom', 'prenom', 'createdAt', 'source']

    try {
      // FR - lecture SQLite locale (même serveur)
      // createdAt est un timestamp en ms, convertir en date pour filtrer
      const frQuery = `sqlite3 -separator '|' '${SQLITE_FR_PATH}' "SELECT uniqueId, etape, vente, idAgence, adresse, codePostalApi, telephone, email, nom, prenom, createdAt, source FROM v2_EstimationLog WHERE DATE(datetime(createdAt/1000, 'unixepoch')) = '${date}' AND etape IN (3, 4) ORDER BY createdAt;"`
      const frRaw = localExec(frQuery)
      const frData = parseSqliteCsv(frRaw, sqliteCols)
      if (frData.length > 0) {
        estimFR.details = frData
        estimFR.etape3 = frData.filter((r: any) => r.etape === 3).length
        estimFR.etape4 = frData.filter((r: any) => r.etape === 4).length
        estimFR.etape3_vendeurs = frData.filter((r: any) => r.etape === 3 && [2, 4, 5, 8].includes(r.vente)).length
        estimFR.etape3_non_vendeurs = frData.filter((r: any) => r.etape === 3 && ![2, 4, 5, 8].includes(r.vente)).length
        estimFR.etape4_vendeurs = frData.filter((r: any) => r.etape === 4 && [2, 4, 5, 8].includes(r.vente)).length
        estimFR.etape4_non_vendeurs = frData.filter((r: any) => r.etape === 4 && ![2, 4, 5, 8].includes(r.vente)).length
      }
    } catch (e: any) {
      console.error('[integrity-funnel] Erreur SQLite FR:', e.message)
    }

    try {
      // ES
      const esQuery = `sqlite3 -separator '|' '${SQLITE_ES_PATH}' "SELECT uniqueId, etape, vente, idAgence, adresse, codePostalApi, telephone, email, nom, prenom, createdAt, source FROM v2_EstimationLog WHERE DATE(datetime(createdAt/1000, 'unixepoch')) = '${date}' AND etape IN (3, 4) ORDER BY createdAt;"`
      const esRaw = localExec(esQuery)
      const esData = parseSqliteCsv(esRaw, sqliteCols)
      if (esData.length > 0) {
        estimES.details = esData
        estimES.etape3 = esData.filter((r: any) => r.etape === 3).length
        estimES.etape4 = esData.filter((r: any) => r.etape === 4).length
        estimES.etape3_vendeurs = esData.filter((r: any) => r.etape === 3 && [2, 4, 5, 8].includes(r.vente)).length
        estimES.etape3_non_vendeurs = esData.filter((r: any) => r.etape === 3 && ![2, 4, 5, 8].includes(r.vente)).length
        estimES.etape4_vendeurs = esData.filter((r: any) => r.etape === 4 && [2, 4, 5, 8].includes(r.vente)).length
        estimES.etape4_non_vendeurs = esData.filter((r: any) => r.etape === 4 && ![2, 4, 5, 8].includes(r.vente)).length
      }
    } catch (e: any) {
      console.error('[integrity-funnel] Erreur SQLite ES:', e.message)
    }

    // ==== 2. LOGS get_price (AWS) - dédoublonné par uniqueId, étape max, vente + adresse ====
    let logs = { etape3: 0, etape4: 0, etape3_vendeurs: 0, etape3_non_vendeurs: 0, etape4_vendeurs: 0, etape4_non_vendeurs: 0 }
    let logsDetails: { uniqueId: string; etape: number; vente: number; adresse: string; idAgence: string }[] = []
    try {
      const logFile = `/home/serviceestimerlo/log/get_price_${date}.log`
      // Script awk : pour chaque uniqueId, garde étape max, dernière vente, dernière adresse, idAgence
      // Ligne 1 = compteurs, lignes suivantes = détail par uniqueId
      const awkScript = `
grep -E '"etape":"[34]"' ${logFile} 2>/dev/null | \\
awk -F'"' '{
  uid=""; etape=""; vente=""; adr=""; ag=""
  for(i=1;i<=NF;i++) {
    if($i=="uniqueId" && $(i+1)==":") uid=$(i+2)
    if($i=="etape" && $(i+1)==":") etape=$(i+2)
    if($i=="vente" && $(i+1)==":") vente=$(i+2)
    if($i=="adresse" && $(i+1)==":") adr=$(i+2)
    if($i=="idAgence" && $(i+1)==":") ag=$(i+2)
  }
  if(uid!="" && (etape=="3"||etape=="4")) {
    if(etape+0 > max_etape[uid]+0) max_etape[uid]=etape
    if(vente!="") last_vente[uid]=vente
    if(adr!="") last_adr[uid]=adr
    if(ag!="") last_ag[uid]=ag
  }
}
END {
  e3=0; e4=0; e3v=0; e4v=0
  for(uid in max_etape) {
    v=last_vente[uid]+0
    vendeur=(v>=1 && v<=9) ? 1 : 0
    if(max_etape[uid]==3) { e3++; if(vendeur) e3v++ }
    if(max_etape[uid]==4) { e4++; if(vendeur) e4v++ }
  }
  print "COUNTS:"e3"|"e4"|"e3v"|"e4v
  for(uid in max_etape) {
    print "D:"uid"\\t"max_etape[uid]"\\t"last_vente[uid]+0"\\t"last_adr[uid]"\\t"last_ag[uid]
  }
}'`
      const raw = sshExec(awkScript, 60000)
      const lines = raw.trim().split('\n')
      for (const line of lines) {
        if (line.startsWith('COUNTS:')) {
          const parts = line.replace('COUNTS:', '').split('|')
          if (parts.length === 4) {
            logs.etape3 = parseInt(parts[0], 10) || 0
            logs.etape4 = parseInt(parts[1], 10) || 0
            logs.etape3_vendeurs = parseInt(parts[2], 10) || 0
            logs.etape3_non_vendeurs = logs.etape3 - logs.etape3_vendeurs
            logs.etape4_vendeurs = parseInt(parts[3], 10) || 0
            logs.etape4_non_vendeurs = logs.etape4 - logs.etape4_vendeurs
          }
        } else if (line.startsWith('D:')) {
          const cols = line.substring(2).split('\t')
          if (cols.length >= 4) {
            logsDetails.push({
              uniqueId: cols[0],
              etape: parseInt(cols[1], 10) || 0,
              vente: parseInt(cols[2], 10) || 0,
              adresse: cols[3] || '',
              idAgence: cols[4] || ''
            })
          }
        }
      }
    } catch (e: any) {
      console.error('[integrity-funnel] Erreur logs:', e.message)
    }

    // ==== 3. MySQL V2 (bien_immobilier) - connexion directe mysql2 ====
    let v2Data: any[] = []
    try {
      const pool = getMysqlPool()
      const [rows] = await pool.execute(
        'SELECT id, uuid_id, adresse, pourcentage_vente, date_acquisition, ville_id, estimation_id FROM bien_immobilier WHERE DATE(date_acquisition) = ? ORDER BY date_acquisition',
        [date]
      ) as any
      v2Data = rows.map((r: any) => ({
        id: r.id,
        uuid_id: r.uuid_id || '',
        adresse: r.adresse || '',
        pourcentage_vente: r.pourcentage_vente || 0,
        date_acquisition: r.date_acquisition ? String(r.date_acquisition) : '',
        ville_id: r.ville_id || 0,
        estimation_id: r.estimation_id || ''
      }))
    } catch (e: any) {
      console.error('[integrity-funnel] Erreur MySQL V2:', e.message)
    }

    // ==== 4. Détection hors zone ====
    // Croise les données estimateur (idAgence = identifier) avec agence_tarif
    // Pour chaque bien V2, on cherche le CP du bien (via ville) et le CP de l'agence (via estimateur idAgence → agence.identifier → agence_tarif)
    let horsZoneMap = new Map<number, { cp_bien: string; cp_agence: string; hors_zone: boolean }>()
    if (v2Data.length > 0) {
      try {
        const pool = getMysqlPool()

        // CP des villes des biens
        const villeIds = [...new Set(v2Data.map((r: any) => r.ville_id).filter(Boolean))]
        let villeCpMap = new Map<number, string>()
        if (villeIds.length > 0) {
          const [villeRows] = await pool.query(`SELECT id, code_postal FROM ville WHERE id IN (${villeIds.join(',')})`) as any
          villeRows.forEach((r: any) => { if (r.id && r.code_postal) villeCpMap.set(r.id, r.code_postal.trim()) })
        }

        // Construire un map uuid → idAgence (identifier) depuis les données estimateur
        const allEstimData = [...(estimFR.details || []), ...(estimES.details || [])]
        const uuidToIdentifier = new Map<string, string>()
        allEstimData.forEach((e: any) => {
          if (e.uniqueId && e.idAgence) uuidToIdentifier.set(e.uniqueId, e.idAgence)
        })

        // Récupérer les identifiers uniques et trouver les agence_id correspondants
        const identifiers = [...new Set([...uuidToIdentifier.values()])]
        let identifierToAgenceId = new Map<string, number>()
        if (identifiers.length > 0) {
          const placeholders = identifiers.map(() => '?').join(',')
          const [agRows] = await pool.execute(`SELECT id, identifier FROM agence WHERE identifier IN (${placeholders})`, identifiers) as any
          agRows.forEach((r: any) => { if (r.identifier) identifierToAgenceId.set(r.identifier, r.id) })
        }

        // CP des agences via agence_tarif
        const agenceIds = [...new Set([...identifierToAgenceId.values()])]
        let agenceCpMap = new Map<number, string>()
        if (agenceIds.length > 0) {
          const [agenceTarifRows] = await pool.query(`SELECT agence_id, code_postal FROM agence_tarif WHERE agence_id IN (${agenceIds.join(',')})`) as any
          agenceTarifRows.forEach((r: any) => {
            if (r.agence_id && r.code_postal) {
              const existing = agenceCpMap.get(r.agence_id) || ''
              agenceCpMap.set(r.agence_id, existing ? `${existing},${r.code_postal.trim()}` : r.code_postal.trim())
            }
          })
        }

        // Détection hors zone pour chaque bien
        // On essaie de retrouver l'identifier de l'agence via l'estimateur (uniqueId correspond à un log)
        for (const bien of v2Data) {
          const cpBien = villeCpMap.get(bien.ville_id) || ''

          // Chercher l'identifier de l'agence via les données estimateur
          const identifier = uuidToIdentifier.get(String(bien.id)) || uuidToIdentifier.get(bien.uuid_id) || ''
          const agenceId = identifier ? identifierToAgenceId.get(identifier) : undefined
          const cpAgence = agenceId ? (agenceCpMap.get(agenceId) || '') : ''
          const cpList = cpAgence.split(',').map((c: string) => c.trim()).filter(Boolean)
          const horsZone = cpBien !== '' && cpList.length > 0 && !cpList.includes('00000') && !cpList.includes(cpBien)
          horsZoneMap.set(bien.id, { cp_bien: cpBien, cp_agence: cpAgence, hors_zone: horsZone })
        }
      } catch (e: any) {
        console.error('[integrity-funnel] Erreur détection hors zone:', e.message)
      }
    }

    // ==== 5. PostgreSQL V3 (property) ====
    const v3Res = await dbPool.query(`
      SELECT p.id_property, p.address_entered, p.address, p.sale_project, p.created_date, p.phone, p.origin,
             p.postal_code, p.city, a.name as agency_name, c.demo
      FROM property p
      LEFT JOIN agency a ON p.id_agency = a.id
      LEFT JOIN client c ON a.id_client = c.id_client
      WHERE DATE(p.created_date AT TIME ZONE 'Europe/Paris') = $1
        AND COALESCE(p.origin, '') NOT IN ('import', 'manual')
      ORDER BY p.created_date
    `, [date])

    // ==== 6. Cross-reference V2 ↔ V3 par UUID ====
    const v2UUIDsList = v2Data.map((r: any) => r.uuid_id).filter((u: string) => u)
    const v3ExistingMap = new Map<string, { created_date: string; id_property: string }>()
    if (v2UUIDsList.length > 0) {
      const batchSize = 500
      for (let i = 0; i < v2UUIDsList.length; i += batchSize) {
        const batch = v2UUIDsList.slice(i, i + batchSize)
        const placeholders = batch.map((_: string, idx: number) => `$${idx + 1}`).join(',')
        const existRes = await dbPool.query(
          `SELECT id_property, created_date FROM property WHERE id_property IN (${placeholders})`,
          batch
        )
        existRes.rows.forEach((r: any) => v3ExistingMap.set(r.id_property, { created_date: r.created_date, id_property: r.id_property }))
      }
    }

    const v3DayUUIDs = new Set(v3Res.rows.map((r: any) => r.id_property))
    const v2UUIDs = new Set(v2Data.map((r: any) => r.uuid_id))

    // Manquants en V3 (UUID introuvable partout)
    const missingInV3 = v2Data
      .filter((r: any) => !v3ExistingMap.has(r.uuid_id))
      .map((r: any) => {
        const hz = horsZoneMap.get(r.id)
        return {
          id: r.id,
          uuid_id: r.uuid_id,
          adresse: r.adresse,
          pourcentage_vente: r.pourcentage_vente,
          date_acquisition: r.date_acquisition,
          is_vendeur: r.pourcentage_vente >= 35,
          hors_zone: hz?.hors_zone ?? false,
          cp_bien: hz?.cp_bien || '',
          cp_agence: hz?.cp_agence || ''
        }
      })

    // Date différente V3
    const differentDateInV3 = v2Data
      .filter((r: any) => v3ExistingMap.has(r.uuid_id) && !v3DayUUIDs.has(r.uuid_id))
      .map((r: any) => ({
        id: r.id,
        uuid_id: r.uuid_id,
        adresse: r.adresse,
        pourcentage_vente: r.pourcentage_vente,
        date_acquisition: r.date_acquisition,
        is_vendeur: r.pourcentage_vente >= 35,
        v3_created_date: v3ExistingMap.get(r.uuid_id)?.created_date
      }))

    // Manquants en V2
    const missingInV2 = v3Res.rows
      .filter((r: any) => !v2UUIDs.has(r.id_property))
      .map((r: any) => ({
        id_property: r.id_property,
        address: r.address_entered || r.address,
        sale_project: r.sale_project,
        created_date: r.created_date,
        origin: r.origin,
        demo: r.demo
      }))

    // Vendeurs/non-vendeurs V2
    const v2Vendeurs = v2Data.filter((r: any) => r.pourcentage_vente >= 35).length
    const v2NonVendeurs = v2Data.length - v2Vendeurs

    // Hors zone stats
    const horsZoneImportes = v2Data.filter((r: any) => horsZoneMap.get(r.id)?.hors_zone).length
    const missingHorsZone = missingInV3.filter(r => r.hors_zone).length
    const missingEnZone = missingInV3.filter(r => !r.hors_zone).length

    // ==== 7. Cross-reference Logs → V2 par estimation_id (global, pas seulement le jour) ====
    // Trouver les leads des logs (étape 3+) qui ne sont pas dans bien_immobilier (tous jours confondus)
    let v2AllEstimationIds = new Set<string>()
    if (logsDetails.length > 0) {
      try {
        const pool = getMysqlPool()
        const uniqueIds = logsDetails.map(l => l.uniqueId).filter(Boolean)
        const batchSize = 500
        for (let i = 0; i < uniqueIds.length; i += batchSize) {
          const batch = uniqueIds.slice(i, i + batchSize)
          const placeholders = batch.map(() => '?').join(',')
          const [rows] = await pool.execute(
            `SELECT estimation_id FROM bien_immobilier WHERE estimation_id IN (${placeholders})`,
            batch
          ) as any
          rows.forEach((r: any) => { if (r.estimation_id) v2AllEstimationIds.add(r.estimation_id) })
        }
      } catch (e: any) {
        console.error('[integrity-funnel] Erreur cross-ref logs→V2:', e.message)
        // Fallback : utiliser seulement les estimation_id du jour
        v2AllEstimationIds = new Set(v2Data.map((r: any) => r.estimation_id).filter(Boolean))
      }
    }
    const logsNotInV2 = logsDetails
      .filter(log => !v2AllEstimationIds.has(log.uniqueId))
      .map(log => ({
        uniqueId: log.uniqueId,
        etape: log.etape,
        vente: log.vente,
        is_vendeur: log.vente >= 1 && log.vente <= 9,
        adresse: log.adresse,
        idAgence: log.idAgence
      }))

    res.json({
      date,
      // Funnel levels
      estimateur: {
        fr: { etape3: estimFR.etape3, etape4: estimFR.etape4, etape3_vendeurs: estimFR.etape3_vendeurs, etape3_non_vendeurs: estimFR.etape3_non_vendeurs, etape4_vendeurs: estimFR.etape4_vendeurs, etape4_non_vendeurs: estimFR.etape4_non_vendeurs },
        es: { etape3: estimES.etape3, etape4: estimES.etape4, etape3_vendeurs: estimES.etape3_vendeurs, etape3_non_vendeurs: estimES.etape3_non_vendeurs, etape4_vendeurs: estimES.etape4_vendeurs, etape4_non_vendeurs: estimES.etape4_non_vendeurs }
      },
      logs: logs,
      v2: { total: v2Data.length, vendeurs: v2Vendeurs, non_vendeurs: v2NonVendeurs, hors_zone_importes: horsZoneImportes },
      v3: { total: v3Res.rows.length },
      // Détail pertes
      missing_in_v3: missingInV3,
      missing_in_v3_stats: { total: missingInV3.length, vendeurs: missingInV3.filter(r => r.is_vendeur).length, non_vendeurs: missingInV3.filter(r => !r.is_vendeur).length, hors_zone: missingHorsZone, en_zone: missingEnZone },
      different_date_in_v3: differentDateInV3,
      missing_in_v2: missingInV2,
      // Logs non importés en V2
      logs_not_in_v2: logsNotInV2,
      logs_not_in_v2_stats: {
        total: logsNotInV2.length,
        vendeurs: logsNotInV2.filter(r => r.is_vendeur).length,
        non_vendeurs: logsNotInV2.filter(r => !r.is_vendeur).length,
        etape3: logsNotInV2.filter(r => r.etape === 3).length,
        etape4: logsNotInV2.filter(r => r.etape === 4).length
      }
    })
  } catch (error) {
    console.error('Erreur integrity-funnel:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// POST /api/leads/integrity-retry  { id: 938892 }
// Appelle retryV3FromV2.php sur le serveur AWS avec l'id v2
app.post('/api/leads/integrity-retry', async (req, res) => {
  try {
    const { id } = req.body
    if (!id || isNaN(Number(id))) {
      return res.status(400).json({ error: 'Parameter id required (numeric)' })
    }

    const cmd = `cd /home/toolsmalineimmob/public_html/malineImport/scripts && php retryV3FromV2.php ${Number(id)}`
    const output = sshExec(cmd)

    console.log(`[integrity-retry] id=${id} => ${output.trim()}`)
    res.json({ success: true, id: Number(id), output: output.trim() })
  } catch (error: any) {
    console.error(`Erreur integrity-retry id=${req.body?.id}:`, error)
    res.status(500).json({
      error: 'Retry failed',
      details: error.stderr || error.message || String(error)
    })
  }
})

// POST /api/leads/integrity-retry-all  { ids: [938892, 938895, ...] }
app.post('/api/leads/integrity-retry-all', async (req, res) => {
  try {
    const { ids } = req.body
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'Parameter ids required (array of numbers)' })
    }

    const results: { id: number; success: boolean; output: string }[] = []
    for (const id of ids) {
      try {
        const cmd = `cd /home/toolsmalineimmob/public_html/malineImport/scripts && php retryV3FromV2.php ${Number(id)}`
        const output = sshExec(cmd)
        results.push({ id: Number(id), success: true, output: output.trim() })
      } catch (err: any) {
        results.push({ id: Number(id), success: false, output: err.stderr || err.message || String(err) })
      }
    }

    console.log(`[integrity-retry-all] ${results.filter(r => r.success).length}/${ids.length} OK`)
    res.json({ results })
  } catch (error) {
    console.error('Erreur integrity-retry-all:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// POST /api/leads/integrity-import-log  { uniqueId: "1766574043663", date: "2026-03-21" }
// Récupère la ligne JSON du log get_price, la copie dans recette_immo, lance import_from_ri.php
app.post('/api/leads/integrity-import-log', async (req, res) => {
  try {
    const { uniqueId, date } = req.body
    if (!uniqueId || !date) {
      return res.status(400).json({ error: 'Parameters uniqueId and date required' })
    }

    const logFile = `/home/serviceestimerlo/log/get_price_${date}.log`
    const riDir = `/home/toolsmalineimmob/public_html/malineImport/data/recette_immo`
    const riScript = `/home/toolsmalineimmob/public_html/malineImport/scripts/import_from_ri.php`

    // 1. Récupérer la dernière ligne du log contenant cet uniqueId (étape la plus avancée)
    // 2. La copier dans recette_immo
    // 3. Lancer import_from_ri.php
    const cmd = [
      `LINE=$(grep '"uniqueId":"${uniqueId}"' ${logFile} 2>/dev/null | tail -1)`,
      `if [ -z "$LINE" ]; then echo "ERROR: uniqueId ${uniqueId} not found in ${logFile}"; exit 1; fi`,
      `FILENAME="import_bo_${uniqueId}_$(date +%s).json"`,
      `echo "$LINE" > ${riDir}/$FILENAME`,
      `echo "FILE: $FILENAME"`,
      `echo "---IMPORT_OUTPUT---"`,
      `cd /home/toolsmalineimmob/public_html/malineImport/scripts && php import_from_ri.php 2>&1`,
      `echo "---END---"`
    ].join(' && ')

    const output = sshExec(cmd, 60000)
    console.log(`[integrity-import-log] uniqueId=${uniqueId} => ${output.substring(0, 200)}`)

    res.json({ success: true, uniqueId, output: output.trim() })
  } catch (error: any) {
    console.error(`Erreur integrity-import-log uniqueId=${req.body?.uniqueId}:`, error)
    res.status(500).json({
      error: 'Import failed',
      details: error.stderr || error.message || String(error)
    })
  }
})

// POST /api/leads/integrity-import-log-all  { uniqueIds: ["123", "456"], date: "2026-03-21" }
// Import batch de tous les leads non importés
app.post('/api/leads/integrity-import-log-all', async (req, res) => {
  try {
    const { uniqueIds, date } = req.body
    if (!Array.isArray(uniqueIds) || uniqueIds.length === 0 || !date) {
      return res.status(400).json({ error: 'Parameters uniqueIds (array) and date required' })
    }

    const logFile = `/home/serviceestimerlo/log/get_price_${date}.log`
    const riDir = `/home/toolsmalineimmob/public_html/malineImport/data/recette_immo`

    console.log(`[integrity-import-log-all] Importing ${uniqueIds.length} leads from ${date}...`)

    // 1. Extraire toutes les lignes nécessaires en une seule commande SSH
    // Crée un fichier par uniqueId dans recette_immo puis lance import_from_ri.php une seule fois
    const grepPatterns = uniqueIds.map(uid => `"uniqueId":"${uid}"`).join('\\|')
    const extractCmd = [
      // Grep toutes les lignes correspondantes d'un coup
      `LINES=$(grep '${grepPatterns}' ${logFile} 2>/dev/null)`,
      `if [ -z "$LINES" ]; then echo "ERROR: No matching lines found"; exit 1; fi`,
      `echo "MATCHED: $(echo "$LINES" | wc -l) lines"`,
    ].join(' && ')

    // Pour chaque uniqueId, extraire la dernière ligne et créer un fichier
    const perIdCmds = uniqueIds.map(uid => [
      `LINE=$(grep '"uniqueId":"${uid}"' ${logFile} 2>/dev/null | tail -1)`,
      `if [ -n "$LINE" ]; then echo "$LINE" > ${riDir}/import_bo_${uid}.json && echo "OK:${uid}"; else echo "MISS:${uid}"; fi`
    ].join(' && ')).join('; ')

    const fullCmd = [
      perIdCmds,
      `echo "---FILES_DONE---"`,
      `cd /home/toolsmalineimmob/public_html/malineImport/scripts && php import_from_ri.php 2>&1`,
      `echo "---IMPORT_DONE---"`
    ].join(' && ')

    const output = sshExec(fullCmd, 120000)
    console.log(`[integrity-import-log-all] Done: ${output.substring(0, 300)}`)

    // Parse les résultats
    const lines = output.split('\n')
    const results: { uniqueId: string; found: boolean }[] = []
    let importOutput = ''
    let inImport = false

    for (const line of lines) {
      if (line.startsWith('OK:')) results.push({ uniqueId: line.substring(3), found: true })
      else if (line.startsWith('MISS:')) results.push({ uniqueId: line.substring(5), found: false })
      else if (line === '---FILES_DONE---') inImport = true
      else if (line === '---IMPORT_DONE---') inImport = false
      else if (inImport) importOutput += line + '\n'
    }

    res.json({
      success: true,
      total: uniqueIds.length,
      found: results.filter(r => r.found).length,
      missed: results.filter(r => !r.found).length,
      results,
      importOutput: importOutput.trim()
    })
  } catch (error: any) {
    console.error('Erreur integrity-import-log-all:', error)
    res.status(500).json({
      error: 'Batch import failed',
      details: error.stderr || error.message || String(error)
    })
  }
})

// --- Contrôle Facturation ---

// Helper : obtenir un access token Zoho via refresh token
async function getZohoAccessToken(): Promise<string | null> {
  const clientId = process.env.ZOHO_CLIENT_ID
  const clientSecret = process.env.ZOHO_CLIENT_SECRET
  const refreshToken = process.env.ZOHO_REFRESH_TOKEN
  if (!clientId || !clientSecret || !refreshToken) {
    console.warn('⚠️ [zoho] Config Zoho incomplète')
    return null
  }
  const params = new URLSearchParams({
    refresh_token: refreshToken,
    client_id: clientId,
    client_secret: clientSecret,
    grant_type: 'refresh_token'
  })
  const res = await fetch('https://accounts.zoho.eu/oauth/v2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString()
  })
  const data = await res.json() as any
  if (!data.access_token) {
    console.warn('⚠️ [zoho] Token refresh failed:', data.error || data)
    return null
  }
  return data.access_token
}

// Récupérer toutes les factures Zoho Invoice (paginées, parallélisé par batch)
async function fetchAllZohoInvoices(accessToken: string): Promise<any[]> {
  const orgId = process.env.ZOHO_ORG_ID
  if (!orgId) return []
  const allInvoices: any[] = []
  let page = 1
  let hasMore = true
  // Première page pour connaître le total
  const t0 = Date.now()
  const firstUrl = `https://www.zohoapis.eu/invoice/v3/invoices?organization_id=${orgId}&page=1&per_page=200&sort_column=date&sort_order=D`
  const firstRes = await fetch(firstUrl, {
    headers: { 'Authorization': `Zoho-oauthtoken ${accessToken}` }
  })
  if (!firstRes.ok) return []
  const firstData = await firstRes.json() as any
  if (firstData.invoices?.length > 0) allInvoices.push(...firstData.invoices)
  hasMore = firstData.page_context?.has_more_page === true
  const totalPages = firstData.page_context?.total_pages || 1
  console.log(`📄 [zoho] Invoices: page 1/${totalPages} fetched (${firstData.invoices?.length || 0} items, ${Date.now() - t0}ms)`)

  // Fetch remaining pages in parallel batches of 5
  if (hasMore && totalPages > 1) {
    const BATCH_SIZE = 5
    for (let batchStart = 2; batchStart <= totalPages && batchStart <= 50; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, totalPages, 50)
      const batchPages = Array.from({ length: batchEnd - batchStart + 1 }, (_, i) => batchStart + i)
      const batchResults = await Promise.all(batchPages.map(async (p) => {
        const url = `https://www.zohoapis.eu/invoice/v3/invoices?organization_id=${orgId}&page=${p}&per_page=200&sort_column=date&sort_order=D`
        const res = await fetch(url, { headers: { 'Authorization': `Zoho-oauthtoken ${accessToken}` } })
        if (!res.ok) return []
        const data = await res.json() as any
        return data.invoices || []
      }))
      for (const items of batchResults) allInvoices.push(...items)
    }
  }
  console.log(`✅ [zoho] Total invoices: ${allInvoices.length} in ${Date.now() - t0}ms`)
  return allInvoices
}

// Récupérer les contacts Zoho (paginé, parallélisé par batch)
async function fetchAllZohoContacts(accessToken: string): Promise<any[]> {
  const orgId = process.env.ZOHO_ORG_ID
  if (!orgId) return []
  const allContacts: any[] = []
  const t0 = Date.now()
  const firstUrl = `https://www.zohoapis.eu/invoice/v3/contacts?organization_id=${orgId}&page=1&per_page=200`
  const firstRes = await fetch(firstUrl, {
    headers: { 'Authorization': `Zoho-oauthtoken ${accessToken}` }
  })
  if (!firstRes.ok) return []
  const firstData = await firstRes.json() as any
  if (firstData.contacts?.length > 0) allContacts.push(...firstData.contacts)
  const totalPages = firstData.page_context?.total_pages || 1

  if (firstData.page_context?.has_more_page && totalPages > 1) {
    const BATCH_SIZE = 5
    for (let batchStart = 2; batchStart <= totalPages && batchStart <= 50; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, totalPages, 50)
      const batchPages = Array.from({ length: batchEnd - batchStart + 1 }, (_, i) => batchStart + i)
      const batchResults = await Promise.all(batchPages.map(async (p) => {
        const url = `https://www.zohoapis.eu/invoice/v3/contacts?organization_id=${orgId}&page=${p}&per_page=200`
        const res = await fetch(url, { headers: { 'Authorization': `Zoho-oauthtoken ${accessToken}` } })
        if (!res.ok) return []
        const data = await res.json() as any
        return data.contacts || []
      }))
      for (const items of batchResults) allContacts.push(...items)
    }
  }
  console.log(`✅ [zoho] Total contacts: ${allContacts.length} in ${Date.now() - t0}ms`)
  return allContacts
}

// Construire le résultat Zoho à partir des factures et contacts
async function buildZohoResult(invoices: any[], contacts: any[]) {
  const contactMap = new Map<string, any>()
  const contactGclMap = new Map<string, string>()
  const contactCreatedMap = new Map<string, string>()
  for (const c of contacts) {
    contactMap.set(c.contact_id, c)
    if (c.created_time) contactCreatedMap.set(c.contact_id, c.created_time)
    if (c.custom_fields) {
      const gclField = (c.custom_fields as any[]).find((cf: any) =>
        cf.label === 'Num GoCardLess' || cf.customfield_id === 'cf_num_gocardless'
      )
      if (gclField?.value) {
        contactGclMap.set(c.contact_id, gclField.value.replace(/[^\x20-\x7E]/g, '').trim())
      }
    }
  }

  let v2Agencies: { nom: string; id_gocardless: string | null; endDate: Date | null; startDate: Date | null }[] = []
  try {
    const raw = await fetchAgenciesV2Raw()
    v2Agencies = raw.map((a: any) => ({
      nom: a.nom || '',
      id_gocardless: a.id_gocardless ? a.id_gocardless.replace(/[^\x20-\x7E]/g, '').trim() : null,
      endDate: pickDate(a, ['date_fin', 'dateFin', 'endDate', 'end_at', 'endAt', 'canceledAt', 'cancelledAt']),
      startDate: pickDate(a, [...V2_START_DATE_KEYS, 'created', 'creationDate'])
    }))
  } catch (e) {
    console.warn('⚠️ [controle] Erreur V2 agencies:', (e as Error).message)
  }

  const byCustomer: Record<string, {
    customer_name: string; customer_id: string; gcl_id: string | null;
    created_time: string | null; contact_info: any; invoices: any[];
    total_invoiced: number; last_invoice_date: string | null
  }> = {}

  for (const inv of invoices) {
    const name = inv.customer_name || 'Inconnu'
    if (!byCustomer[name]) {
      byCustomer[name] = {
        customer_name: name,
        customer_id: inv.customer_id,
        gcl_id: contactGclMap.get(inv.customer_id) || null,
        created_time: contactCreatedMap.get(inv.customer_id) || null,
        contact_info: contactMap.get(inv.customer_id) || null,
        invoices: [],
        total_invoiced: 0,
        last_invoice_date: null
      }
    }
    byCustomer[name].invoices.push({
      invoice_id: inv.invoice_id, invoice_number: inv.invoice_number,
      date: inv.date, due_date: inv.due_date, status: inv.status,
      total: parseFloat(inv.total || '0'), balance: parseFloat(inv.balance || '0'),
      currency_code: inv.currency_code, reference_number: inv.reference_number,
      country: inv.country || inv.billing_address?.country || null
    })
    byCustomer[name].total_invoiced += parseFloat(inv.total || '0')
    if (!byCustomer[name].last_invoice_date || inv.date > byCustomer[name].last_invoice_date!) {
      byCustomer[name].last_invoice_date = inv.date
    }
  }

  console.log(`✅ [controle] ${invoices.length} factures Zoho, ${contacts.length} contacts, ${Object.keys(byCustomer).length} clients, ${v2Agencies.length} agences V2`)
  return {
    customers: byCustomer,
    total_invoices: invoices.length,
    total_contacts: contacts.length,
    v2_agencies: v2Agencies.map(a => ({
      nom: a.nom,
      gcl_id: a.id_gocardless,
      end_date: a.endDate?.toISOString().slice(0, 10) || null,
      start_date: a.startDate?.toISOString().slice(0, 10) || null,
      normalized: normalizeAgencyName(a.nom)
    }))
  }
}

// Refresh en arrière-plan pour stale-while-revalidate
const zohoRefreshInProgress = new Set<string>()

async function refreshZohoInvoicesCache(cacheKey: string) {
  if (zohoRefreshInProgress.has(cacheKey)) return
  zohoRefreshInProgress.add(cacheKey)
  console.log('🔄 [controle] Background refresh Zoho invoices...')
  const t0 = Date.now()
  try {
    const accessToken = await getZohoAccessToken()
    if (!accessToken) return

    const [invoices, contacts] = await Promise.all([
      fetchAllZohoInvoices(accessToken),
      fetchAllZohoContacts(accessToken)
    ])

    const result = await buildZohoResult(invoices, contacts)
    setCache(cacheKey, result, 2 * 60 * 60 * 1000)
    console.log(`✅ [controle] Background refresh done in ${Date.now() - t0}ms`)
  } catch (e) {
    console.error('❌ [controle] Background refresh failed:', (e as Error).message)
  } finally {
    zohoRefreshInProgress.delete(cacheKey)
  }
}

// Endpoint : récupérer les factures Zoho
app.get('/api/controle/zoho-invoices', async (req, res) => {
  console.log('🔍 [controle] Récupération factures Zoho...')
  try {
    const cacheKey = 'controle-zoho-invoices'
    if (!req.query.refresh) {
      // Cache frais → retourner directement
      const cached = getCached<any>(cacheKey)
      if (cached) return res.json(cached)
      // Cache périmé → retourner immédiatement + refresh en arrière-plan
      const stale = getStaleCache<any>(cacheKey)
      if (stale) {
        refreshZohoInvoicesCache(cacheKey)
        return res.json(stale)
      }
    }

    const accessToken = await getZohoAccessToken()
    if (!accessToken) return res.status(500).json({ error: 'Impossible d\'obtenir un token Zoho' })

    const [invoices, contacts] = await Promise.all([
      fetchAllZohoInvoices(accessToken),
      fetchAllZohoContacts(accessToken)
    ])

    const result = await buildZohoResult(invoices, contacts)
    setCache(cacheKey, result, 2 * 60 * 60 * 1000) // 2h
    res.json(result)
  } catch (e) {
    console.error('❌ [controle] Erreur Zoho:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// Endpoint : récupérer toutes les campagnes actives Meta + Google avec leur statut
app.get('/api/controle/active-campaigns', async (req, res) => {
  console.log('🔍 [controle] Récupération campagnes actives...')
  try {
    const cacheKey = 'controle-active-campaigns'
    if (!req.query.refresh) {
      const cached = getCached<any>(cacheKey)
      if (cached) return res.json(cached)
    }

    // --- Meta : lister tous les adsets avec statut ---
    const metaCampaigns: any[] = []
    const tokens = [
      process.env.META_ADS_TOKEN_1,
      process.env.META_ADS_TOKEN_2,
      process.env.META_ADS_TOKEN_3,
      process.env.META_ADS_TOKEN_4,
      process.env.META_ADS_TOKEN_5
    ].filter(Boolean) as string[]

    const seenMetaAccounts = new Set<string>()
    for (const token of tokens) {
      try {
        const accRes = await fetch(`https://graph.facebook.com/v21.0/me/adaccounts?fields=id,name,account_status&limit=100&access_token=${token}`)
        const accData = await accRes.json() as any
        if (!accData.data) continue
        for (const acc of (accData.data as any[])) {
          if (seenMetaAccounts.has(acc.id)) continue
          seenMetaAccounts.add(acc.id)
          // Seuls les comptes actifs (account_status=1), exclure comptes ignorés
          if (acc.account_status !== 1) continue
          if (acc.id === 'act_2750225608564753') continue
          try {
            const url = `https://graph.facebook.com/v21.0/${acc.id}/adsets?fields=id,name,daily_budget,effective_status,created_time,campaign{name}&filtering=${encodeURIComponent(JSON.stringify([{field:'effective_status',operator:'IN',value:['ACTIVE']}]))}&limit=500&access_token=${token}`
            const r = await fetch(url)
            const data = await r.json() as any
            if (!data.data) continue
            const actNumeric = acc.id.replace('act_', '')
            for (const adset of data.data) {
              if (adset.effective_status !== 'ACTIVE') continue // double vérification
              metaCampaigns.push({
                source: 'meta',
                account_id: acc.id,
                account_name: acc.name,
                adset_id: adset.id,
                name: adset.name,
                campaign_name: adset.campaign?.name || null,
                daily_budget: adset.daily_budget ? parseFloat(adset.daily_budget) / 100 : 0,
                status: adset.effective_status,
                normalized: normalizeAdName(adset.name),
                url: `https://www.facebook.com/adsmanager/manage/adsets/edit?act=${actNumeric}&selected_adset_ids=${adset.id}`,
                created_time: adset.created_time || null
              })
            }
          } catch (e) {
            console.warn(`⚠️ [controle] Meta adsets error for ${acc.id}:`, (e as Error).message)
          }
        }
      } catch (e) {
        console.warn('⚠️ [controle] Meta accounts error:', (e as Error).message)
      }
    }

    // --- Google : lister toutes les campagnes avec statut ---
    const googleCampaigns: any[] = []
    const clientId = process.env.GOOGLE_ADS_CLIENT_ID
    const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET
    const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN
    const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN
    const customerIds = process.env.GOOGLE_ADS_CUSTOMER_IDS?.split(',').map(s => s.trim()).filter(Boolean)

    if (clientId && clientSecret && refreshToken && devToken && customerIds?.length) {
      const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `client_id=${clientId}&client_secret=${clientSecret}&refresh_token=${refreshToken}&grant_type=refresh_token`
      })
      const tokenData = await tokenRes.json() as any
      if (tokenData.access_token) {
        const accessToken = tokenData.access_token
        await Promise.all(customerIds.map(async (cid) => {
          try {
            // Récupérer le nom du compte + les campagnes en diffusion uniquement
            const query = `SELECT customer.descriptive_name, campaign.id, campaign.name, campaign.status, campaign.serving_status, campaign.start_date, campaign_budget.amount_micros
                           FROM campaign
                           WHERE campaign.status = 'ENABLED' AND campaign.serving_status = 'SERVING'`
            const r = await fetch(`https://googleads.googleapis.com/v23/customers/${cid}/googleAds:searchStream`, {
              method: 'POST',
              headers: {
                'Authorization': `Bearer ${accessToken}`,
                'developer-token': devToken,
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({ query })
            })
            if (!r.ok) return
            const data = await r.json() as any
            if (!Array.isArray(data)) return
            let accountName = ''
            for (const chunk of data) {
              if (!chunk?.results) continue
              for (const row of chunk.results) {
                if (!accountName && row.customer?.descriptiveName) accountName = row.customer.descriptiveName
                if (!row.campaign?.name) continue
                const campaignId = row.campaign.id
                googleCampaigns.push({
                  source: 'google',
                  customer_id: cid,
                  account_name: accountName,
                  campaign_id: campaignId,
                  name: row.campaign.name,
                  daily_budget: row.campaignBudget?.amountMicros ? parseInt(row.campaignBudget.amountMicros) / 1e6 : 0,
                  status: row.campaign.status,
                  normalized: normalizeAdName(row.campaign.name),
                  url: `https://ads.google.com/aw/overview?campaignId=${campaignId}&ocid=${cid}`,
                  created_time: row.campaign.startDate || null
                })
              }
            }
          } catch (e) {
            console.warn(`⚠️ [controle] Google error for ${cid}:`, (e as Error).message)
          }
        }))
      }
    }

    const result = {
      meta: metaCampaigns,
      google: googleCampaigns,
      total_meta: metaCampaigns.length,
      total_google: googleCampaigns.length
    }
    console.log(`✅ [controle] ${metaCampaigns.length} adsets Meta, ${googleCampaigns.length} campagnes Google`)
    setCache(cacheKey, result, CACHE_TTL_LONG_MS)
    res.json(result)
  } catch (e) {
    console.error('❌ [controle] Erreur campagnes:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// Endpoint : clients actifs V2 avec budget mensuel (pour contrôle facturation)
app.get('/api/controle/active-clients', async (req, res) => {
  console.log('🔍 [controle] Récupération clients actifs V2...')
  try {
    const cacheKey = 'controle-active-clients'
    if (!req.query.refresh) {
      const cached = getCached<any>(cacheKey)
      if (cached) return res.json(cached)
    }

    const agencies = await fetchAgenciesV2Raw()
    const now = new Date()
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 86400000)

    const clients: any[] = []
    for (const a of agencies) {
      const startDate = pickDate(a, [...V2_START_DATE_KEYS, 'created', 'creationDate'])
      const endDate = pickDate(a, ['date_fin', 'dateFin', 'endDate', 'end_at', 'endAt', 'canceledAt', 'cancelledAt'])

      // Uniquement les clients actifs (pas de end_date ou end_date dans le futur)
      if (endDate && endDate < now) continue

      // Uniquement les clients créés il y a plus de 30 jours
      if (startDate && startDate > thirtyDaysAgo) continue

      // Calculer le budget mensuel depuis les tarifs
      const tarifs = Array.isArray(a.tarifs) ? a.tarifs : []
      const budgetMensuel = tarifs.reduce((sum: number, t: any) => {
        const val = parseFloat(t.tarif || '0')
        return sum + (isNaN(val) ? 0 : val)
      }, 0)

      // Exclure les clients sans budget
      if (budgetMensuel <= 0) continue

      const gclId = a.id_gocardless ? a.id_gocardless.replace(/[^\x20-\x7E]/g, '').trim() : null

      clients.push({
        nom: a.nom || '',
        normalized: normalizeAgencyName(a.nom || ''),
        gcl_id: gclId,
        budget_mensuel: Math.round(budgetMensuel * 100) / 100,
        tarifs: tarifs.map((t: any) => ({ code_postal: t.code_postal, tarif: parseFloat(t.tarif || '0') })),
        start_date: startDate?.toISOString().slice(0, 10) || null,
        end_date: endDate?.toISOString().slice(0, 10) || null
      })
    }

    clients.sort((a: any, b: any) => a.nom.localeCompare(b.nom, 'fr'))
    const result = { clients, total: clients.length }
    console.log(`✅ [controle] ${clients.length} clients actifs V2`)
    setCache(cacheKey, result, CACHE_TTL_LONG_MS)
    res.json(result)
  } catch (e) {
    console.error('❌ [controle] Erreur clients actifs:', e)
    res.status(500).json({ error: (e as Error).message })
  }
})

// --- Invoice matching overrides (Supabase via ad_matching_overrides avec préfixe inv:) ---
const INV_PREFIX = 'inv:'

app.get('/api/invoice-matching-overrides', async (req, res) => {
  try {
    const cached = getCached<Record<string, string>>('invoice-matching-overrides')
    if (cached) return res.json(cached)

    const { data, error } = await supabaseAdmin
      .from('ad_matching_overrides')
      .select('normalized_name, id_client')
      .like('normalized_name', `${INV_PREFIX}%`)

    if (error) return res.status(500).json({ error: error.message })

    const overrides: Record<string, string> = {}
    for (const row of (data || [])) {
      overrides[row.normalized_name.replace(INV_PREFIX, '')] = row.id_client
    }
    setCache('invoice-matching-overrides', overrides, CACHE_TTL_LONG_MS)
    res.json(overrides)
  } catch (e) {
    res.status(500).json({ error: (e as Error).message })
  }
})

app.post('/api/invoice-matching-overrides', async (req, res) => {
  try {
    const { campaign_normalized, zoho_customer_name } = req.body || {}
    if (!campaign_normalized || !zoho_customer_name) {
      return res.status(400).json({ error: 'campaign_normalized and zoho_customer_name required' })
    }
    const { error } = await supabaseAdmin
      .from('ad_matching_overrides')
      .upsert({ normalized_name: `${INV_PREFIX}${campaign_normalized}`, id_client: zoho_customer_name }, { onConflict: 'normalized_name' })

    if (error) return res.status(500).json({ error: error.message })
    invalidateCache('invoice-matching-overrides')
    res.json({ success: true })
  } catch (e) {
    res.status(500).json({ error: (e as Error).message })
  }
})

app.delete('/api/invoice-matching-overrides', async (req, res) => {
  try {
    const { campaign_normalized } = req.body || {}
    if (!campaign_normalized) {
      return res.status(400).json({ error: 'campaign_normalized required' })
    }
    const { error } = await supabaseAdmin
      .from('ad_matching_overrides')
      .delete()
      .eq('normalized_name', `${INV_PREFIX}${campaign_normalized}`)

    if (error) return res.status(500).json({ error: error.message })
    invalidateCache('invoice-matching-overrides')
    res.json({ success: true })
  } catch (e) {
    res.status(500).json({ error: (e as Error).message })
  }
})

// Démarrage du serveur
async function startServer() {
  try {
    await initDatabase()

    // Vérifier les tokens publicitaires
    refreshGoogleAdsTokenIfNeeded().catch(() => {})
    refreshMetaTokenIfNeeded().catch(() => {})
    // Re-vérifier toutes les 12h (Google expire en 7j)
    setInterval(() => refreshGoogleAdsTokenIfNeeded().catch(() => {}), 12 * 60 * 60 * 1000)
    // Re-vérifier Meta toutes les 24h
    setInterval(() => refreshMetaTokenIfNeeded().catch(() => {}), 24 * 60 * 60 * 1000)

    // Synchronisation des stats publicitaires vers Supabase
    // Sync initiale après 10s (laisser le serveur démarrer d'abord)
    setTimeout(() => {
      console.log('🔄 [ads-sync] Lancement de la synchronisation initiale...')
      syncAdsStatsToSupabase().catch(e => {
        console.error('❌ [ads-sync] Erreur sync initiale:', e)
      })
    }, 10_000)
    // Puis toutes les 6h
    setInterval(() => {
      console.log('🔄 [ads-sync] Synchronisation périodique...')
      syncAdsStatsToSupabase().catch(e => {
        console.error('❌ [ads-sync] Erreur sync périodique:', e)
      })
    }, 6 * 60 * 60 * 1000)

    // Synchronisation des stats leads vers Supabase
    setTimeout(() => {
      console.log('🔄 [leads-sync] Lancement de la synchronisation initiale...')
      syncLeadsStatsToSupabase().catch(e => {
        console.error('❌ [leads-sync] Erreur sync initiale:', e)
      })
    }, 15_000)
    setInterval(() => {
      console.log('🔄 [leads-sync] Synchronisation périodique...')
      syncLeadsStatsToSupabase().catch(e => {
        console.error('❌ [leads-sync] Erreur sync périodique:', e)
      })
    }, 6 * 60 * 60 * 1000)

    // Mini-sync incrémental "aujourd'hui uniquement" toutes les 90s
    // Léger (3 lignes upsertées, requêtes SQL filtrées sur CURRENT_DATE)
    setInterval(() => {
      syncTodayLeadsStatsToSupabase().catch(e => {
        console.error('❌ [leads-today-sync] Erreur:', e)
      })
    }, 90 * 1000)

    // Synchronisation des stats pub vers Supabase
    setTimeout(() => {
      console.log('🔄 [pub-stats-sync] Lancement de la synchronisation initiale...')
      syncPubStatsToSupabase().catch(e => {
        console.error('❌ [pub-stats-sync] Erreur sync initiale:', e)
      })
    }, 20_000)
    setInterval(() => {
      console.log('🔄 [pub-stats-sync] Synchronisation périodique...')
      syncPubStatsToSupabase().catch(e => {
        console.error('❌ [pub-stats-sync] Erreur sync périodique:', e)
      })
    }, 2 * 60 * 60 * 1000)

    app.listen(PORT, () => {
      console.log(`🚀 Serveur API démarré sur http://localhost:${PORT}`)
      console.log(`📊 Health check: http://localhost:${PORT}/api/health`)
      console.log(`🏢 Agences: http://localhost:${PORT}/api/agencies`)
    })
  } catch (error) {
    console.error('❌ Impossible de démarrer le serveur:', error)
    process.exit(1)
  }
}

// Gestion de l'arrêt propre
process.on('SIGINT', async () => {
  console.log('\n🔄 Arrêt du serveur...')

  if (dbPool) {
    await dbPool.end()
    console.log('✅ Pool PostgreSQL fermé')
  }

  if (sshProcess) {
    closeSSHTunnel(sshProcess)
  }

  process.exit(0)
})

startServer()
