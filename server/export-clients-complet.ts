import { Pool } from 'pg'
import { createClient } from '@supabase/supabase-js'
import { createSSHTunnel, closeSSHTunnel } from './ssh-tunnel.js'
import dotenv from 'dotenv'
import fs from 'fs'
import path from 'path'

dotenv.config()

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

async function main() {
  console.log('📡 Récupération des données API Territory V2...')

  const response = await fetch('https://back-api.maline-immobilier.fr/territory/api/agences', {
    headers: { 'x-api-key': '70c51af056cccd8a1fa1434be9fddfa4a0e86929e5b65055db844f38ba4b3fce' }
  })

  const territoryData = await response.json()
  const territoryAgencies = Array.isArray(territoryData) ? territoryData : (territoryData?.data || [])
  console.log('API Territory V2:', territoryAgencies.length, 'agences')

  // Connexion PostgreSQL V3
  console.log('🔐 Connexion PostgreSQL V3...')
  const ssh = await createSSHTunnel()
  await sleep(5000)

  const pool = new Pool({
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || '20184'),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME || 'dbmsmaline_main_137',
    ssl: { rejectUnauthorized: false },
    max: 5,
    connectionTimeoutMillis: 10000,
  })

  const client = await pool.connect()
  console.log('✅ Connecté à PostgreSQL')

  // Récupérer les infos des agences (adresse, tel, email, contact principal)
  console.log('📊 Récupération des infos agences V3...')
  const agencyQuery = `
    SELECT
      c.id_client,
      c.name as client_name,
      c.id_gocardless,
      a.name as agency_name,
      a.address,
      a.postal_code,
      a.city,
      a.phone,
      a.email,
      ag.first_name as contact_first_name,
      ag.last_name as contact_last_name,
      ag.email as contact_email
    FROM client c
    LEFT JOIN agency a ON a.id_client = c.id_client
    LEFT JOIN LATERAL (
      SELECT first_name, last_name, email
      FROM agent
      WHERE id_agency = a.id
      ORDER BY id ASC
      LIMIT 1
    ) ag ON true
  `
  const agencyResult = await client.query(agencyQuery)
  console.log('PostgreSQL V3:', agencyResult.rows.length, 'lignes')

  client.release()
  await pool.end()
  closeSSHTunnel(ssh)

  // Créer des maps V3 par id_gocardless et par nom
  function normalizeName(name: string): string {
    return name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]/g, '').trim()
  }

  const v3ByGcl = new Map<string, any>()
  const v3ByName = new Map<string, any>()

  for (const row of agencyResult.rows) {
    // Nettoyer id_gocardless
    let gcl = row.id_gocardless
    if (gcl) {
      // Convertir en string si nécessaire
      if (typeof gcl !== 'string') {
        gcl = JSON.stringify(gcl)
      }
      // Gérer le cas où c'est un array JSON
      if (gcl.startsWith('[')) {
        try {
          const arr = JSON.parse(gcl)
          gcl = arr[0] || null
        } catch {}
      }
      if (gcl && typeof gcl === 'string') {
        gcl = gcl.replace(/"/g, '').replace(/[^\x20-\x7E]/g, '').trim()
      }
    }

    if (gcl && !v3ByGcl.has(gcl)) {
      v3ByGcl.set(gcl, row)
    }
    if (row.client_name) {
      const normalized = normalizeName(row.client_name)
      if (normalized && !v3ByName.has(normalized)) {
        v3ByName.set(normalized, row)
      }
    }
  }

  console.log('Maps V3:', v3ByGcl.size, 'par GCL,', v3ByName.size, 'par nom')

  // Construire la liste complète depuis V2
  const clients = territoryAgencies.map((agency: any) => {
    const gcl = agency.id_gocardless ? agency.id_gocardless.replace(/[^\x20-\x7E]/g, '').trim() : null

    // Trouver le client V3 correspondant
    let v3 = gcl ? v3ByGcl.get(gcl) : null
    if (!v3 && agency.nom) {
      v3 = v3ByName.get(normalizeName(agency.nom))
    }

    // Statut résiliation
    let statut = 'Actif'
    if (agency.date_fin) {
      const fin = new Date(agency.date_fin)
      statut = fin <= new Date() ? 'Résilié' : 'Actif'
    }

    // Extraire tarif total
    let tarifTotal = 0
    if (agency.tarifs && Array.isArray(agency.tarifs)) {
      tarifTotal = agency.tarifs.reduce((sum: number, t: any) => sum + parseFloat(t.tarif || 0), 0)
    }

    // Extraire codes postaux uniques
    let codesPostaux: string[] = []
    if (agency.tarifs && Array.isArray(agency.tarifs)) {
      for (const t of agency.tarifs) {
        if (t.code_postal) {
          const codes = t.code_postal.split(',').map((c: string) => c.trim()).filter((c: string) => c && c !== '00000')
          codesPostaux.push(...codes)
        }
      }
    }
    codesPostaux = [...new Set(codesPostaux)]

    return {
      nom: agency.nom || '',
      nom_agence: v3?.agency_name || agency.nom || '',
      prenom_contact: v3?.contact_first_name || '',
      nom_contact: v3?.contact_last_name || '',
      email_contact: v3?.contact_email || '',
      statut: statut,
      date_debut: agency.date_start || '',
      date_fin: agency.date_fin || '',
      // Infos contact depuis V3
      adresse: v3?.address || '',
      code_postal: v3?.postal_code || '',
      ville: v3?.city || '',
      telephone: v3?.phone || '',
      email_agence: v3?.email || '',
    }
  })

  // Stats
  const actifs = clients.filter((c: any) => c.statut === 'Actif').length
  const resilies = clients.filter((c: any) => c.statut === 'Résilié').length
  const avecContact = clients.filter((c: any) => c.email || c.telephone).length

  console.log('\n📊 Statistiques:')
  console.log('- Total:', clients.length)
  console.log('- Actifs:', actifs)
  console.log('- Résiliés:', resilies)
  console.log('- Avec contact (email/tel):', avecContact)

  // Export
  const docPath = path.resolve(process.cwd(), '../doc')
  if (!fs.existsSync(docPath)) fs.mkdirSync(docPath, { recursive: true })

  const jsonPath = path.join(docPath, 'clients_liste_complete.json')
  fs.writeFileSync(jsonPath, JSON.stringify(clients, null, 2), 'utf8')
  console.log('\n✅ JSON:', jsonPath)

  const csvPath = path.join(docPath, 'clients_liste_complete.csv')
  const headers = Object.keys(clients[0] || {})
  const csvContent = [
    headers.join(';'),
    ...clients.map((row: any) => headers.map(h => {
      let val = row[h]
      if (val === null || val === undefined) return ''
      if (typeof val === 'string' && (val.includes(';') || val.includes('"') || val.includes('\n'))) {
        return '"' + val.replace(/"/g, '""') + '"'
      }
      return val
    }).join(';'))
  ].join('\n')
  fs.writeFileSync(csvPath, csvContent, 'utf8')
  console.log('✅ CSV:', csvPath)

  process.exit(0)
}

main().catch(e => { console.error(e); process.exit(1) })
