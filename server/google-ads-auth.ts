/**
 * Script standalone pour générer un refresh token Google Ads
 * Utilise le flow OOB (out-of-band) : Google affiche le code directement
 *
 * Usage: npx tsx server/google-ads-auth.ts
 */

import http from 'http'
import readline from 'readline'
import fs from 'fs'

const CLIENT_ID = '13008734323-kkls10u3brtulqeirqjn0hbbljucu94u.apps.googleusercontent.com'
const CLIENT_SECRET = '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'
const SCOPE = 'https://www.googleapis.com/auth/adwords'
const REDIRECT_URI = 'http://localhost:9007'

async function exchangeCode(code: string) {
  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `code=${encodeURIComponent(code)}&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}&grant_type=authorization_code`
  })
  const data = await tokenRes.json() as any
  if (data.refresh_token) {
    const output = [
      '\n✅ Refresh token obtenu !\n',
      `GOOGLE_ADS_CLIENT_ID=${CLIENT_ID}`,
      `GOOGLE_ADS_CLIENT_SECRET=${CLIENT_SECRET}`,
      `GOOGLE_ADS_REFRESH_TOKEN=${data.refresh_token}`
    ].join('\n')
    process.stdout.write(output + '\n')
    fs.writeFileSync('google-ads-token.txt', output)
    process.stdout.write('✅ Token écrit dans google-ads-token.txt\n')
  } else {
    const err = '\n❌ Erreur: ' + JSON.stringify(data, null, 2)
    process.stdout.write(err + '\n')
    fs.writeFileSync('google-ads-token.txt', err)
  }
}

async function main() {
  const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}&response_type=code&scope=${encodeURIComponent(SCOPE)}&access_type=offline&prompt=consent`

  console.log('\n=== Google Ads OAuth - Génération du refresh token ===\n')
  console.log('1) Ouvre cette URL dans ton navigateur :\n')
  console.log(authUrl)
  console.log('\n2) Connecte-toi avec le compte Google Ads')
  console.log('3) Autorise l\'accès')
  console.log('4) Google affichera un code — copie-le et colle-le ci-dessous\n')

  // Also start a web server as fallback for pasting the code
  const PORT = 9007
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '', `http://localhost:${PORT}`)
    const code = url.searchParams.get('code')
    if (code) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
      res.end('<h2>Code recu, regarde le terminal...</h2>')
      await exchangeCode(code)
      server.close()
      process.exit(0)
    }
    // Show a form for pasting the code
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
    res.end(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Google Ads OAuth</title></head>
<body style="font-family:system-ui;max-width:600px;margin:40px auto;padding:0 20px">
<h2>Google Ads - Coller le code OAuth</h2>
<p>Apres avoir autorise l'acces sur Google, colle le code ici :</p>
<form method="GET">
<input type="text" name="code" placeholder="Colle le code ici" style="width:100%;padding:12px;font-size:16px;border:2px solid #4285f4;border-radius:8px;margin:8px 0" autofocus required />
<button type="submit" style="padding:12px 24px;background:#4285f4;color:#fff;border:none;border-radius:8px;font-size:16px;cursor:pointer;margin-top:8px">Echanger le code</button>
</form>
</body></html>`)
  })
  server.listen(PORT, () => {
    console.log(`💡 Alternative: va sur http://localhost:${PORT} pour coller le code dans un formulaire web\n`)
  })

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  rl.question('Code: ', async (code) => {
    rl.close()
    if (code.trim()) {
      await exchangeCode(code.trim())
    }
    server.close()
    process.exit(0)
  })
}

main().catch(console.error)
