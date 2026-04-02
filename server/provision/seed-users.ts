import 'dotenv/config'
import { supabaseAdmin } from '../supabase-admin.js'

const allowed = [
  'ffenech@maline-immobilier.fr',
  'ffrezzato@maline-immobilier.fr',
]

async function main() {
  const baseUrl = process.env.APP_BASE_URL || 'http://localhost:5173'
  const redirectTo = `${baseUrl.replace(/\/$/, '')}/reset-password`

  console.log('Inviting allowed users…')
  for (const email of allowed) {
    try {
      const { data, error } = await supabaseAdmin.auth.admin.inviteUserByEmail(email, {
        redirectTo,
      })
      if (error) {
        const msg = String(error.message || '').toLowerCase()
        if (msg.includes('already registered') || msg.includes('already exists')) {
          console.log(`• ${email} déjà existant — ok`)
          continue
        }
        console.error(`• ${email} erreur:`, error.message)
        continue
      }
      console.log(`• ${email} invité (id: ${data.user?.id})`)
    } catch (e: any) {
      console.error(`• ${email} exception:`, e?.message || e)
    }
  }

  console.log('\nAssurez-vous de désactiver les inscriptions libres dans Supabase:')
  console.log('Auth > Providers > Email > "Allow email signups" = OFF')
  console.log(`Redirect URL autorisée: ${redirectTo}`)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})

