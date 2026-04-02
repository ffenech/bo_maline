import { google } from 'googleapis'
import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { PDFDocument } from 'pdf-lib'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

// Chemins des fichiers de configuration
const CREDENTIALS_PATH = path.join(__dirname, '../gmail-credentials.json')
const TOKEN_PATH = path.join(__dirname, '../gmail-token.json')

// Scopes nécessaires pour lire les emails et pièces jointes
const SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

// Email cible
const TARGET_EMAIL = 'invoicesmaline@gmail.com'

// Choisir le bon redirect_uri selon l'environnement
function pickRedirectUri(redirect_uris: string[]): string {
  const isDev = process.env.NODE_ENV === 'development' || (!process.env.NODE_ENV && process.env.npm_lifecycle_event === 'dev')
  if (isDev) return redirect_uris[0] // localhost
  // En prod, prendre l'URI non-localhost s'il existe
  return redirect_uris.find(u => !u.includes('localhost')) || redirect_uris[0]
}

// Cache des factures (évite de recalculer à chaque appel)
let invoicesCache: { data: InvoiceData[]; timestamp: number } | null = null
const CACHE_TTL_MS = 10 * 60 * 1000 // 10 minutes

export interface InvoiceAttachment {
  filename: string
  mimeType: string
  attachmentId: string
  size: number
}

export interface InvoiceData {
  id: string
  messageId: string
  subject: string
  from: string
  senderName: string // Nom du prestataire extrait
  date: string
  year: number
  month: number
  monthName: string
  attachments: InvoiceAttachment[]
}

export interface InvoicesGrouped {
  [year: string]: {
    [month: string]: {
      [provider: string]: InvoiceData[]
    }
  }
}

// Créer le client OAuth2
async function getOAuth2Client() {
  try {
    const credentialsContent = await fs.readFile(CREDENTIALS_PATH, 'utf-8')
    const credentials = JSON.parse(credentialsContent)

    const { client_id, client_secret, redirect_uris } = credentials.installed || credentials.web
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, pickRedirectUri(redirect_uris))

    // Essayer de charger le token existant
    try {
      const tokenContent = await fs.readFile(TOKEN_PATH, 'utf-8')
      const token = JSON.parse(tokenContent)
      oAuth2Client.setCredentials(token)

      // Vérifier si le token doit être rafraîchi
      if (token.expiry_date && token.expiry_date < Date.now()) {
        console.log('🔄 Token Gmail expiré, rafraîchissement...')
        const { credentials: newCredentials } = await oAuth2Client.refreshAccessToken()
        oAuth2Client.setCredentials(newCredentials)
        await fs.writeFile(TOKEN_PATH, JSON.stringify(newCredentials), 'utf-8')
        console.log('✅ Token Gmail rafraîchi et sauvegardé')
      }

      return oAuth2Client
    } catch {
      console.log('⚠️ Aucun token Gmail trouvé, authentification requise')
      return null
    }
  } catch (error) {
    console.error('❌ Erreur lors du chargement des credentials Gmail:', error)
    return null
  }
}

// Générer l'URL d'authentification
export async function getAuthUrl(): Promise<string | null> {
  try {
    const credentialsContent = await fs.readFile(CREDENTIALS_PATH, 'utf-8')
    const credentials = JSON.parse(credentialsContent)

    const { client_id, client_secret, redirect_uris } = credentials.installed || credentials.web
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, pickRedirectUri(redirect_uris))

    const authUrl = oAuth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES,
      prompt: 'consent' // Force le refresh token
    })

    return authUrl
  } catch (error) {
    console.error('❌ Erreur lors de la génération de l\'URL d\'auth:', error)
    return null
  }
}

// Échanger le code d'autorisation contre un token
export async function exchangeCodeForToken(code: string): Promise<boolean> {
  try {
    const credentialsContent = await fs.readFile(CREDENTIALS_PATH, 'utf-8')
    const credentials = JSON.parse(credentialsContent)

    const { client_id, client_secret, redirect_uris } = credentials.installed || credentials.web
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, pickRedirectUri(redirect_uris))

    const { tokens } = await oAuth2Client.getToken(code)
    oAuth2Client.setCredentials(tokens)

    // Sauvegarder le token
    await fs.writeFile(TOKEN_PATH, JSON.stringify(tokens), 'utf-8')
    console.log('✅ Token Gmail sauvegardé')

    return true
  } catch (error) {
    console.error('❌ Erreur lors de l\'échange du code:', error)
    return false
  }
}

// Vérifier si l'authentification est configurée
export async function isAuthenticated(): Promise<boolean> {
  const client = await getOAuth2Client()
  return client !== null
}

// Extraire le nom du prestataire depuis l'email "from"
function extractSenderName(from: string): string {
  // Format typique: "Nom <email@domain.com>" ou juste "email@domain.com"
  const match = from.match(/^"?([^"<]+)"?\s*</)
  if (match) {
    return match[1].trim()
  }

  // Si pas de nom, extraire le domaine
  const emailMatch = from.match(/@([^.]+)/)
  if (emailMatch) {
    return emailMatch[1].charAt(0).toUpperCase() + emailMatch[1].slice(1)
  }

  return from.split('@')[0] || 'Inconnu'
}

// Obtenir le nom du mois en français
function getMonthName(month: number): string {
  const months = [
    'Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin',
    'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'
  ]
  return months[month - 1] || 'Inconnu'
}

// Récupérer les factures depuis Gmail
export async function fetchInvoices(forceRefresh = false): Promise<InvoiceData[]> {
  // Vérifier le cache
  if (!forceRefresh && invoicesCache && Date.now() - invoicesCache.timestamp < CACHE_TTL_MS) {
    console.log('📦 Cache HIT: gmail-invoices')
    return invoicesCache.data
  }

  const oAuth2Client = await getOAuth2Client()
  if (!oAuth2Client) {
    throw new Error('Gmail non authentifié')
  }

  const gmail = google.gmail({ version: 'v1', auth: oAuth2Client })

  console.log(`📧 Récupération des emails avec factures pour ${TARGET_EMAIL}...`)

  // Rechercher les emails avec pièces jointes (typiquement des factures PDF)
  // On recherche les emails ayant des pièces jointes PDF ou qui contiennent "facture" ou "invoice"
  const searchQuery = 'has:attachment (filename:pdf OR filename:PDF) (facture OR invoice OR receipt OR reçu OR billing)'

  const invoices: InvoiceData[] = []
  let pageToken: string | undefined = undefined

  do {
    const response = await gmail.users.messages.list({
      userId: 'me',
      q: searchQuery,
      maxResults: 100,
      pageToken
    })

    const messages = response.data.messages || []
    console.log(`📨 ${messages.length} messages trouvés dans cette page`)

    for (const message of messages) {
      if (!message.id) continue

      try {
        const fullMessage = await gmail.users.messages.get({
          userId: 'me',
          id: message.id,
          format: 'full'
        })

        const headers = fullMessage.data.payload?.headers || []
        const subject = headers.find(h => h.name?.toLowerCase() === 'subject')?.value || 'Sans sujet'
        const from = headers.find(h => h.name?.toLowerCase() === 'from')?.value || 'Inconnu'
        const dateHeader = headers.find(h => h.name?.toLowerCase() === 'date')?.value

        // Parser la date
        const date = dateHeader ? new Date(dateHeader) : new Date()
        const year = date.getFullYear()
        const month = date.getMonth() + 1

        // Extraire les pièces jointes PDF
        const attachments: InvoiceAttachment[] = []

        function findAttachments(parts: any[]) {
          for (const part of parts) {
            if (part.filename && part.body?.attachmentId) {
              const mimeType = part.mimeType?.toLowerCase() || ''
              // Ne garder que les PDF
              if (mimeType === 'application/pdf' || part.filename.toLowerCase().endsWith('.pdf')) {
                attachments.push({
                  filename: part.filename,
                  mimeType: part.mimeType || 'application/pdf',
                  attachmentId: part.body.attachmentId,
                  size: part.body.size || 0
                })
              }
            }
            if (part.parts) {
              findAttachments(part.parts)
            }
          }
        }

        if (fullMessage.data.payload?.parts) {
          findAttachments(fullMessage.data.payload.parts)
        }

        // Ne garder que les emails avec des pièces jointes PDF
        if (attachments.length > 0) {
          invoices.push({
            id: message.id,
            messageId: message.id,
            subject,
            from,
            senderName: extractSenderName(from),
            date: date.toISOString(),
            year,
            month,
            monthName: getMonthName(month),
            attachments
          })
        }
      } catch (err) {
        console.error(`⚠️ Erreur lors de la lecture du message ${message.id}:`, err)
      }
    }

    pageToken = response.data.nextPageToken || undefined
  } while (pageToken)

  console.log(`✅ ${invoices.length} factures trouvées au total`)

  // Trier par date décroissante
  invoices.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())

  // Mettre en cache
  invoicesCache = { data: invoices, timestamp: Date.now() }

  return invoices
}

// Grouper les factures par année > mois > prestataire
export function groupInvoices(invoices: InvoiceData[]): InvoicesGrouped {
  const grouped: InvoicesGrouped = {}

  for (const invoice of invoices) {
    const yearKey = String(invoice.year)
    const monthKey = `${String(invoice.month).padStart(2, '0')} - ${invoice.monthName}`
    const provider = invoice.senderName

    if (!grouped[yearKey]) {
      grouped[yearKey] = {}
    }
    if (!grouped[yearKey][monthKey]) {
      grouped[yearKey][monthKey] = {}
    }
    if (!grouped[yearKey][monthKey][provider]) {
      grouped[yearKey][monthKey][provider] = []
    }

    grouped[yearKey][monthKey][provider].push(invoice)
  }

  return grouped
}

// Télécharger une pièce jointe
export async function downloadAttachment(messageId: string, attachmentId: string): Promise<{ data: Buffer; filename: string } | null> {
  const oAuth2Client = await getOAuth2Client()
  if (!oAuth2Client) {
    throw new Error('Gmail non authentifié')
  }

  const gmail = google.gmail({ version: 'v1', auth: oAuth2Client })

  try {
    // D'abord, récupérer les infos du message pour avoir le nom du fichier
    const message = await gmail.users.messages.get({
      userId: 'me',
      id: messageId,
      format: 'full'
    })

    let filename = 'facture.pdf'

    function findFilename(parts: any[]) {
      for (const part of parts) {
        if (part.body?.attachmentId === attachmentId && part.filename) {
          filename = part.filename
          return true
        }
        if (part.parts && findFilename(part.parts)) {
          return true
        }
      }
      return false
    }

    if (message.data.payload?.parts) {
      findFilename(message.data.payload.parts)
    }

    // Télécharger l'attachement
    const attachment = await gmail.users.messages.attachments.get({
      userId: 'me',
      messageId,
      id: attachmentId
    })

    if (!attachment.data.data) {
      return null
    }

    // Décoder le base64 URL-safe
    const data = Buffer.from(attachment.data.data, 'base64')

    return { data, filename }
  } catch (error) {
    console.error('❌ Erreur lors du téléchargement de la pièce jointe:', error)
    return null
  }
}

// Nettoyer un nom pour l'utiliser dans un chemin de fichier
function sanitizeName(name: string): string {
  return name.replace(/[<>:"/\\|?*]/g, '_').replace(/\s+/g, ' ').trim()
}

// Télécharger toutes les factures d'un groupe (année/mois/prestataire) en ZIP
// Structure: Année/Mois/Prestataire/fichier.pdf
// Si firstPageOnly est true, ne garde que la première page de chaque PDF
export async function downloadInvoicesAsZip(invoices: InvoiceData[], firstPageOnly = false): Promise<Buffer> {
  const oAuth2Client = await getOAuth2Client()
  if (!oAuth2Client) {
    throw new Error('Gmail non authentifié')
  }

  // Import dynamique de archiver
  const archiver = (await import('archiver')).default

  return new Promise(async (resolve, reject) => {
    const archive = archiver('zip', { zlib: { level: 9 } })
    const chunks: Buffer[] = []

    archive.on('data', (chunk) => chunks.push(chunk))
    archive.on('end', () => resolve(Buffer.concat(chunks)))
    archive.on('error', reject)

    for (const invoice of invoices) {
      for (const attachment of invoice.attachments) {
        try {
          const result = await downloadAttachment(invoice.messageId, attachment.attachmentId)
          if (result) {
            // Construire le chemin: Année/Mois/Prestataire/fichier.pdf
            const year = String(invoice.year)
            const month = `${String(invoice.month).padStart(2, '0')}_${invoice.monthName}`
            const provider = sanitizeName(invoice.senderName)

            // Ajouter la date au nom du fichier pour éviter les doublons
            const dateStr = new Date(invoice.date).toISOString().split('T')[0]
            const safeFilename = `${dateStr}_${result.filename.replace(/[^a-zA-Z0-9._-]/g, '_')}`

            // Chemin complet dans le ZIP
            const fullPath = `${year}/${month}/${provider}/${safeFilename}`

            // Extraire seulement la première page si demandé
            let pdfData = result.data
            if (firstPageOnly) {
              pdfData = await extractFirstPage(result.data)
            }

            archive.append(pdfData, { name: fullPath })
          }
        } catch (err) {
          console.error(`⚠️ Erreur téléchargement ${attachment.filename}:`, err)
        }
      }
    }

    archive.finalize()
  })
}

// Invalider le cache
export function invalidateInvoicesCache() {
  invoicesCache = null
  console.log('🗑️ Cache factures invalidé')
}

// Extraire uniquement la première page d'un PDF
export async function extractFirstPage(pdfBuffer: Buffer): Promise<Buffer> {
  try {
    const pdfDoc = await PDFDocument.load(pdfBuffer)
    const pageCount = pdfDoc.getPageCount()

    if (pageCount <= 1) {
      // Le PDF n'a qu'une page, retourner tel quel
      return pdfBuffer
    }

    // Créer un nouveau PDF avec uniquement la première page
    const newPdfDoc = await PDFDocument.create()
    const [firstPage] = await newPdfDoc.copyPages(pdfDoc, [0])
    newPdfDoc.addPage(firstPage)

    const newPdfBytes = await newPdfDoc.save()
    return Buffer.from(newPdfBytes)
  } catch (error) {
    console.error('❌ Erreur lors de l\'extraction de la première page:', error)
    // En cas d'erreur, retourner le PDF original
    return pdfBuffer
  }
}
