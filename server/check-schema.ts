import { Pool } from 'pg'
import { createSSHTunnel, closeSSHTunnel } from './ssh-tunnel.js'
import dotenv from 'dotenv'
dotenv.config()

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

async function main() {
  console.log('🔐 Établissement du tunnel SSH...')
  const ssh = await createSSHTunnel()
  await sleep(5000)

  const pool = new Pool({
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || '20184'),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: 'dbmsmaline_main_132',
    ssl: { rejectUnauthorized: false },
    max: 1,
    connectionTimeoutMillis: 10000,
  })

  const client = await pool.connect()
  console.log('✅ Connecté à PostgreSQL')

  // Colonnes de la table client
  console.log('\n📋 Colonnes table CLIENT:')
  const clientCols = await client.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'client' ORDER BY ordinal_position")
  console.log(clientCols.rows.map((r: any) => r.column_name).join(', '))

  // Colonnes de la table agency
  console.log('\n📋 Colonnes table AGENCY:')
  const agencyCols = await client.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'agency' ORDER BY ordinal_position")
  console.log(agencyCols.rows.map((r: any) => r.column_name).join(', '))

  // Exemple d'un client avec toutes les colonnes
  console.log('\n📋 Exemple CLIENT (1 ligne):')
  const ex = await client.query('SELECT * FROM client LIMIT 1')
  if (ex.rows[0]) {
    for (const [key, value] of Object.entries(ex.rows[0])) {
      if (value !== null && value !== '') {
        console.log(`  ${key}: ${JSON.stringify(value).substring(0, 100)}`)
      }
    }
  }

  // Exemple d'une agency avec toutes les colonnes
  console.log('\n📋 Exemple AGENCY (1 ligne):')
  const ex2 = await client.query('SELECT * FROM agency LIMIT 1')
  if (ex2.rows[0]) {
    for (const [key, value] of Object.entries(ex2.rows[0])) {
      if (value !== null && value !== '') {
        console.log(`  ${key}: ${JSON.stringify(value).substring(0, 100)}`)
      }
    }
  }

  client.release()
  await pool.end()
  closeSSHTunnel(ssh)
  console.log('\n✅ Terminé')
  process.exit(0)
}

main().catch(e => { console.error(e); process.exit(1) })
