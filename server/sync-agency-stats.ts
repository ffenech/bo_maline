/**
 * Script de synchronisation des stats agences vers Supabase
 *
 * Ce script lit les statistiques des agences depuis PostgreSQL V3
 * et les enrichit avec les données de l'API Territory V2 (codes postaux, tarifs, nombre_logements).
 * Les données sont synchronisées vers Supabase pour permettre l'accès
 * depuis les Netlify Functions (sans tunnel SSH).
 *
 * Usage: npx tsx server/sync-agency-stats.ts
 *
 * À exécuter régulièrement (cron) ou manuellement.
 */

import { Pool } from 'pg'
import { createClient } from '@supabase/supabase-js'
import dotenv from 'dotenv'
import { createSSHTunnel, closeSSHTunnel } from './ssh-tunnel.js'

dotenv.config()

const supabaseUrl = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

if (!supabaseUrl || !serviceRoleKey) {
  console.error('❌ Variables Supabase manquantes')
  process.exit(1)
}

const supabase = createClient(supabaseUrl, serviceRoleKey)

// Interface pour les données de l'API Territory V2
interface V2AgencyData {
  nom: string
  identifier: string
  id_gocardless: string | null
  nombre_logements: number | null
  tarifs: { code_postal: string; tarif: string }[]
}

interface AgencyStats {
  id_client: string
  client_name: string
  id_gocardless: string | null
  nb_leads_total: number
  nb_leads: number
  nb_leads_zone_total: number
  nb_leads_zone: number
  nb_leads_zone_phone_valid: number
  sector_postal_codes: string | null
  tarifs: string | null
  leads_contacted: number
  leads_with_reminder: number
  avg_reminders_done: number
  mandats_signed: number
  pct_lead_contacte: number
  pct_relance_prevu: number
  nombre_logements: number | null
  updated_at: string
}

// Normaliser un nom d'agence pour le matching
function normalizeAgencyName(name: string): string {
  return name
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // Supprimer les accents
    .replace(/[^a-z0-9]/g, '') // Garder uniquement lettres et chiffres
    .trim()
}

// Résultat de la récupération V2
interface V2Data {
  byGocardless: Map<string, V2AgencyData>
  byName: Map<string, V2AgencyData>
}

// Récupérer les données depuis l'API Territory V2
async function fetchV2AgenciesData(): Promise<V2Data> {
  console.log('📡 Récupération des données depuis l\'API Territory V2...')

  const response = await fetch('https://back-api.maline-immobilier.fr/territory/api/agences', {
    headers: {
      'x-api-key': '70c51af056cccd8a1fa1434be9fddfa4a0e86929e5b65055db844f38ba4b3fce'
    }
  })

  if (!response.ok) {
    throw new Error(`Erreur API V2: ${response.status}`)
  }

  const data = await response.json()
  const agencies: V2AgencyData[] = Array.isArray(data) ? data : (data?.data || [])

  // Créer une map par id_gocardless (nettoyé des caractères invisibles)
  const byGocardless = new Map<string, V2AgencyData>()
  // Créer une map par nom normalisé (fallback)
  const byName = new Map<string, V2AgencyData>()

  for (const agency of agencies) {
    // Map par id_gocardless
    if (agency.id_gocardless) {
      const cleanGcl = agency.id_gocardless.replace(/[^\x20-\x7E]/g, '').trim()
      if (cleanGcl) {
        byGocardless.set(cleanGcl, agency)
      }
    }

    // Map par nom normalisé (pour fallback)
    if (agency.nom) {
      const normalizedName = normalizeAgencyName(agency.nom)
      if (normalizedName) {
        byName.set(normalizedName, agency)
      }
    }
  }

  console.log(`✅ ${byGocardless.size} agences V2 avec id_gocardless, ${byName.size} par nom`)
  return { byGocardless, byName }
}

// Helper pour attendre
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// Générer les noms de base candidats (incrémentation automatique)
function generateCandidateDbNames(initialName: string, limit = 10): string[] {
  const candidates = [initialName]
  const match = initialName.match(/^(.*?)(\d+)$/)

  if (!match) {
    return candidates
  }

  const [, prefix, suffix] = match
  let current = parseInt(suffix, 10)
  if (Number.isNaN(current)) {
    return candidates
  }

  for (let i = 1; i <= limit; i++) {
    current += 1
    candidates.push(`${prefix}${current}`)
  }

  return candidates
}

// Trouver la base de données qui fonctionne
async function findWorkingDatabase(basePool: Pool): Promise<string> {
  const initialName = process.env.DB_NAME
  if (!initialName) {
    throw new Error('DB_NAME doit être défini')
  }

  const candidates = generateCandidateDbNames(initialName)

  for (const candidate of candidates) {
    try {
      console.log(`🔄 Test de connexion sur la base "${candidate}"...`)
      const testPool = new Pool({
        host: process.env.DB_HOST,
        port: parseInt(process.env.DB_PORT || '20184', 10),
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: candidate,
        ssl: { rejectUnauthorized: false },
        max: 1,
        connectionTimeoutMillis: 5000,
      })

      const client = await testPool.connect()
      await client.query('SELECT 1')
      client.release()
      await testPool.end()

      console.log(`✅ Base "${candidate}" trouvée`)
      return candidate
    } catch (error) {
      console.log(`❌ Base "${candidate}" non disponible`)
    }
  }

  throw new Error(`Impossible de trouver une base parmi: ${candidates.join(', ')}`)
}

async function syncAgencyStats() {
  let sshProcess: any = null
  let dbPool: Pool | null = null

  try {
    console.log('🔄 Démarrage de la synchronisation...')

    // 1. Récupérer les données V2 (codes postaux, tarifs et nombre_logements) depuis l'API Territory
    const v2Data = await fetchV2AgenciesData()

    // 2. Établir le tunnel SSH
    console.log('🔐 Établissement du tunnel SSH...')
    sshProcess = await createSSHTunnel()

    // Attendre que le tunnel soit complètement établi
    console.log('⏳ Attente de stabilisation du tunnel (5s)...')
    await sleep(5000)

    // 3. Trouver et se connecter à PostgreSQL
    console.log('🔌 Recherche de la base PostgreSQL...')

    // Trouver la bonne base de données
    const workingDbName = await findWorkingDatabase(null as any)

    dbPool = new Pool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || '20184', 10),
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: workingDbName,
      ssl: { rejectUnauthorized: false },
      max: 5,
      connectionTimeoutMillis: 10000,
    })

    const client = await dbPool.connect()
    console.log(`✅ Connexion PostgreSQL établie sur "${workingDbName}"`)

    // 4. Récupérer les stats des agences (sans sector_postal_codes de V3)
    console.log('📊 Récupération des stats agences...')
    const query = `
      SELECT
        c.id_client,
        c.name as client_name,
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
        CASE
          WHEN c.id_gocardless IS NOT NULL AND jsonb_typeof(c.id_gocardless::jsonb) = 'array'
          THEN c.id_gocardless::jsonb->>0
          ELSE c.id_gocardless::text
        END as id_gocardless
      FROM client c
      LEFT JOIN agency a ON a.id_client = c.id_client
      LEFT JOIN property p ON p.id_agency = a.id
        AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
        AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
        AND p.archived = false
      GROUP BY c.id_client, c.name, c.id_gocardless::text
    `

    const result = await client.query(query)
    console.log(`📋 ${result.rows.length} clients récupérés`)

    // 5. Récupérer les stats de la zone pour chaque client
    // On utilise maintenant les codes postaux de V2 via id_gocardless ou fallback par nom
    console.log('📊 Récupération des stats zone (avec codes postaux V2)...')

    // Helper pour trouver l'agence V2 correspondante
    function findV2Agency(row: any): V2AgencyData | null {
      // Priorité 1: matching par id_gocardless
      const gcl = row.id_gocardless?.replace(/"/g, '')
      if (gcl && v2Data.byGocardless.has(gcl)) {
        return v2Data.byGocardless.get(gcl)!
      }

      // Priorité 2: fallback par nom normalisé
      if (row.client_name) {
        const normalizedName = normalizeAgencyName(row.client_name)
        if (normalizedName && v2Data.byName.has(normalizedName)) {
          return v2Data.byName.get(normalizedName)!
        }
      }

      return null
    }

    // Construire une map id_client -> codes postaux V2
    const clientPostalCodes = new Map<string, string[]>()
    let matchedByGcl = 0
    let matchedByName = 0

    for (const row of result.rows) {
      const v2Agency = findV2Agency(row)
      if (v2Agency) {
        // Compter le type de matching
        const gcl = row.id_gocardless?.replace(/"/g, '')
        if (gcl && v2Data.byGocardless.has(gcl)) {
          matchedByGcl++
        } else {
          matchedByName++
        }

        // Les codes postaux sont séparés par des virgules dans chaque tarif
        const postalCodes: string[] = []
        for (const tarif of v2Agency.tarifs) {
          if (tarif.code_postal) {
            // Splitter par virgule et nettoyer
            const codes = tarif.code_postal.split(',').map(c => c.trim()).filter(c => c && c !== '00000')
            postalCodes.push(...codes)
          }
        }
        // Dédupliquer
        const uniqueCodes = [...new Set(postalCodes)]
        if (uniqueCodes.length > 0) {
          clientPostalCodes.set(row.id_client, uniqueCodes)
        }
      }
    }

    console.log(`📋 Matching V2: ${matchedByGcl} par id_gocardless, ${matchedByName} par nom`)

    // Requête pour les stats zone avec les codes postaux V2
    // On doit faire une requête par client qui a des codes postaux
    const zoneStats = new Map<string, { nb_leads_zone_total: number; nb_leads_zone: number; nb_leads_zone_phone_valid: number }>()

    const clientsWithPostalCodes = Array.from(clientPostalCodes.entries())
    console.log(`📋 ${clientsWithPostalCodes.length} clients avec codes postaux V2`)

    for (const [clientId, postalCodes] of clientsWithPostalCodes) {
      if (postalCodes.length === 0) continue

      const zoneQuery = `
        SELECT
          COUNT(p.id_property)::integer as nb_leads_zone_total,
          COUNT(CASE WHEN p.phone IS NOT NULL THEN 1 END)::integer as nb_leads_zone,
          COUNT(CASE WHEN p.phone IS NOT NULL AND p.phone_valid = 'validated' THEN 1 END)::integer as nb_leads_zone_phone_valid
        FROM agency a
        JOIN property p ON p.id_agency = a.id
        WHERE a.id_client = $1
          AND p.postal_code = ANY($2)
          AND p.sale_project IN ('less1Year', 'between1And2Years', 'more2Years', 'onGoing', 'asSoonAsPossible', 'in3Months', 'less6Months')
          AND (p.origin IS NULL OR p.origin NOT IN ('storeFlyer', 'qrcode', 'import', 'noticePassage', 'iframe', 'manual'))
          AND p.archived = false
      `

      const zoneResult = await client.query(zoneQuery, [clientId, postalCodes])
      if (zoneResult.rows[0]) {
        zoneStats.set(clientId, zoneResult.rows[0])
      }
    }

    console.log(`📋 ${zoneStats.size} clients avec stats zone calculées`)

    client.release()

    // 6. Transformer les données
    const stats: AgencyStats[] = result.rows.map((row: any) => {
      const nbLeadsTotal = row.nb_leads_total || 0
      const nbLeads = row.nb_leads || 0
      const leadsContacted = row.leads_contacted || 0
      const leadsWithReminder = row.leads_with_reminder || 0
      const avgRemindersDone = row.avg_reminders_done ? Math.round(parseFloat(row.avg_reminders_done) * 10) / 10 : 0
      const mandatsSigned = row.mandats_signed || 0

      // Stats de la zone (calculées avec codes postaux V2)
      const zone = zoneStats.get(row.id_client)
      const nbLeadsZoneTotal = zone?.nb_leads_zone_total || 0
      const nbLeadsZone = zone?.nb_leads_zone || 0
      const nbLeadsZonePhoneValid = zone?.nb_leads_zone_phone_valid || 0

      const pctLeadContacte = nbLeads > 0 ? Math.round((leadsContacted / nbLeads) * 100 * 10) / 10 : 0
      const pctRelancePrevu = nbLeads > 0 ? Math.round((leadsWithReminder / nbLeads) * 100 * 10) / 10 : 0

      // Récupérer les données V2 (codes postaux, tarifs et nombre_logements) via id_gocardless ou fallback par nom
      const gcl = row.id_gocardless?.replace(/"/g, '')
      const v2Agency = findV2Agency(row)

      // Codes postaux depuis V2 (extraits et splittés des tarifs)
      const postalCodesV2: string[] = []
      if (v2Agency?.tarifs) {
        for (const tarif of v2Agency.tarifs) {
          if (tarif.code_postal) {
            const codes = tarif.code_postal.split(',').map(c => c.trim()).filter(c => c && c !== '00000')
            postalCodesV2.push(...codes)
          }
        }
      }
      const uniquePostalCodes = [...new Set(postalCodesV2)]

      // Tarifs depuis V2 (avec codes postaux splittés)
      const tarifsV2: { code_postal: string; tarif: string }[] = []
      if (v2Agency?.tarifs) {
        for (const tarif of v2Agency.tarifs) {
          if (tarif.code_postal) {
            const codes = tarif.code_postal.split(',').map(c => c.trim()).filter(c => c && c !== '00000')
            // Répartir le tarif sur chaque code postal
            const tarifParCode = codes.length > 0 ? (parseFloat(tarif.tarif) / codes.length).toFixed(2) : tarif.tarif
            for (const code of codes) {
              tarifsV2.push({ code_postal: code, tarif: tarifParCode })
            }
          }
        }
      }

      // Nombre de logements depuis V2
      const nombreLogements = v2Agency?.nombre_logements ?? null

      return {
        id_client: row.id_client,
        client_name: row.client_name || 'Inconnu',
        id_gocardless: gcl || null,
        nb_leads_total: nbLeadsTotal,
        nb_leads: nbLeads,
        nb_leads_zone_total: nbLeadsZoneTotal,
        nb_leads_zone: nbLeadsZone,
        nb_leads_zone_phone_valid: nbLeadsZonePhoneValid,
        sector_postal_codes: uniquePostalCodes.length > 0 ? JSON.stringify(uniquePostalCodes) : null,
        tarifs: tarifsV2.length > 0 ? JSON.stringify(tarifsV2) : null,
        leads_contacted: leadsContacted,
        leads_with_reminder: leadsWithReminder,
        avg_reminders_done: avgRemindersDone,
        mandats_signed: mandatsSigned,
        pct_lead_contacte: pctLeadContacte,
        pct_relance_prevu: pctRelancePrevu,
        nombre_logements: nombreLogements,
        updated_at: new Date().toISOString()
      }
    })

    // 7. Synchroniser vers Supabase (upsert)
    console.log('📤 Synchronisation vers Supabase...')

    // Supprimer les anciennes données et insérer les nouvelles
    const { error: deleteError } = await supabase
      .from('agency_stats')
      .delete()
      .neq('id_client', '00000000-0000-0000-0000-000000000000') // Supprime tout

    if (deleteError) {
      console.error('⚠️ Erreur lors de la suppression:', deleteError)
    }

    // Insérer par lots de 100
    const batchSize = 100
    for (let i = 0; i < stats.length; i += batchSize) {
      const batch = stats.slice(i, i + batchSize)
      const { error: insertError } = await supabase
        .from('agency_stats')
        .insert(batch)

      if (insertError) {
        console.error(`❌ Erreur lors de l'insertion du lot ${i / batchSize + 1}:`, insertError)
      } else {
        console.log(`✅ Lot ${i / batchSize + 1}/${Math.ceil(stats.length / batchSize)} inséré`)
      }
    }

    console.log('✅ Synchronisation terminée avec succès!')

  } catch (error) {
    console.error('❌ Erreur lors de la synchronisation:', error)
    throw error
  } finally {
    // Nettoyage
    if (dbPool) {
      await dbPool.end()
      console.log('🔌 Pool PostgreSQL fermé')
    }
    if (sshProcess) {
      closeSSHTunnel(sshProcess)
      console.log('🔐 Tunnel SSH fermé')
    }
  }
}

// Exécution
syncAgencyStats()
  .then(() => {
    console.log('🎉 Script terminé')
    process.exit(0)
  })
  .catch((error) => {
    console.error('💥 Erreur fatale:', error)
    process.exit(1)
  })
