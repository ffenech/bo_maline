import { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { FileText, Download, RefreshCw, ChevronDown, ChevronRight, Calendar, Building, FolderOpen, Search, AlertCircle, ExternalLink, Scissors, Upload, X, FileUp } from 'lucide-react'

interface InvoiceAttachment {
  filename: string
  mimeType: string
  attachmentId: string
  size: number
}

interface InvoiceData {
  id: string
  messageId: string
  subject: string
  from: string
  senderName: string
  date: string
  year: number
  month: number
  monthName: string
  attachments: InvoiceAttachment[]
}

interface InvoicesGrouped {
  [year: string]: {
    [month: string]: {
      [provider: string]: InvoiceData[]
    }
  }
}

interface InvoicesResponse {
  invoices: InvoiceData[]
  grouped: InvoicesGrouped
  stats: {
    totalInvoices: number
    totalAttachments: number
    providers: number
    years: number[]
  }
}

function FacturesPage() {
  const [invoicesData, setInvoicesData] = useState<InvoicesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [needsAuth, setNeedsAuth] = useState(false)
  const [authUrl, setAuthUrl] = useState<string | null>(null)

  // États d'expansion de l'arborescence
  const [expandedYears, setExpandedYears] = useState<Set<string>>(new Set())
  const [expandedMonths, setExpandedMonths] = useState<Set<string>>(new Set())
  const [expandedProviders, setExpandedProviders] = useState<Set<string>>(new Set())

  // Filtres
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedYear, setSelectedYear] = useState<string>('all')

  // Option première page uniquement
  const [firstPageOnly, setFirstPageOnly] = useState(false)

  // Téléchargements en cours
  const [downloading, setDownloading] = useState<Set<string>>(new Set())

  // Upload PDF
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([])
  const [isProcessingUpload, setIsProcessingUpload] = useState(false)
  const [isDragging, setIsDragging] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const API_URL = import.meta.env.VITE_API_URL || '/api'

  // Vérifier le statut d'authentification au chargement
  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search)
    const authStatus = urlParams.get('auth')

    if (authStatus === 'success') {
      // Nettoyer l'URL
      window.history.replaceState({}, '', '/factures')
    } else if (authStatus === 'error') {
      setError('Erreur lors de l\'authentification Gmail')
      window.history.replaceState({}, '', '/factures')
    }

    checkAuthAndFetch()
  }, [])

  const checkAuthAndFetch = async () => {
    setLoading(true)
    setError(null)

    try {
      // Vérifier l'authentification
      const authResponse = await fetch(`${API_URL}/invoices/auth/status`)
      const authData = await authResponse.json()

      if (!authData.authenticated) {
        setNeedsAuth(true)
        // Récupérer l'URL d'auth
        const urlResponse = await fetch(`${API_URL}/invoices/auth/url`)
        const urlData = await urlResponse.json()
        if (urlData.authUrl) {
          setAuthUrl(urlData.authUrl)
        }
        setLoading(false)
        return
      }

      // Récupérer les factures
      await fetchInvoices()
    } catch (err) {
      console.error('Erreur:', err)
      setError('Erreur de connexion au serveur')
    } finally {
      setLoading(false)
    }
  }

  const fetchInvoices = async (forceRefresh = false) => {
    try {
      const url = forceRefresh ? `${API_URL}/invoices?refresh=true` : `${API_URL}/invoices`
      const response = await fetch(url)

      if (response.status === 401) {
        const data = await response.json()
        if (data.needsAuth) {
          setNeedsAuth(true)
          const urlResponse = await fetch(`${API_URL}/invoices/auth/url`)
          const urlData = await urlResponse.json()
          if (urlData.authUrl) {
            setAuthUrl(urlData.authUrl)
          }
        }
        return
      }

      if (!response.ok) {
        throw new Error('Erreur lors de la récupération des factures')
      }

      const data: InvoicesResponse = await response.json()
      setInvoicesData(data)
      setNeedsAuth(false)

      // Ouvrir automatiquement la première année
      if (data.stats.years.length > 0) {
        setExpandedYears(new Set([String(data.stats.years[0])]))
      }
    } catch (err) {
      console.error('Erreur:', err)
      setError('Erreur lors de la récupération des factures')
    }
  }

  const handleRefresh = async () => {
    setRefreshing(true)
    await fetchInvoices(true)
    setRefreshing(false)
  }

  const toggleYear = (year: string) => {
    const newExpanded = new Set(expandedYears)
    if (newExpanded.has(year)) {
      newExpanded.delete(year)
    } else {
      newExpanded.add(year)
    }
    setExpandedYears(newExpanded)
  }

  const toggleMonth = (key: string) => {
    const newExpanded = new Set(expandedMonths)
    if (newExpanded.has(key)) {
      newExpanded.delete(key)
    } else {
      newExpanded.add(key)
    }
    setExpandedMonths(newExpanded)
  }

  const toggleProvider = (key: string) => {
    const newExpanded = new Set(expandedProviders)
    if (newExpanded.has(key)) {
      newExpanded.delete(key)
    } else {
      newExpanded.add(key)
    }
    setExpandedProviders(newExpanded)
  }

  const downloadAttachment = async (messageId: string, attachmentId: string, filename: string) => {
    const key = `${messageId}-${attachmentId}`
    setDownloading(prev => new Set(prev).add(key))

    try {
      const url = firstPageOnly
        ? `${API_URL}/invoices/download/${messageId}/${attachmentId}?firstPageOnly=true`
        : `${API_URL}/invoices/download/${messageId}/${attachmentId}`
      const response = await fetch(url)
      if (!response.ok) throw new Error('Erreur téléchargement')

      const blob = await response.blob()
      const blobUrl = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = blobUrl

      // Ajouter suffixe si première page uniquement
      let downloadFilename = filename
      if (firstPageOnly) {
        const ext = filename.substring(filename.lastIndexOf('.'))
        const baseName = filename.substring(0, filename.lastIndexOf('.'))
        downloadFilename = `${baseName}_page1${ext}`
      }

      a.download = downloadFilename
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(blobUrl)
      document.body.removeChild(a)
    } catch (err) {
      console.error('Erreur téléchargement:', err)
      alert('Erreur lors du téléchargement')
    } finally {
      setDownloading(prev => {
        const next = new Set(prev)
        next.delete(key)
        return next
      })
    }
  }

  const downloadZip = async (year?: number, month?: number, provider?: string) => {
    const key = `zip-${year || 'all'}-${month || 'all'}-${provider || 'all'}`
    setDownloading(prev => new Set(prev).add(key))

    try {
      const response = await fetch(`${API_URL}/invoices/download-zip`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ year, month, provider, firstPageOnly })
      })

      if (!response.ok) throw new Error('Erreur création ZIP')

      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url

      let filename = 'factures'
      if (year) filename += `_${year}`
      if (month) filename += `_${String(month).padStart(2, '0')}`
      if (provider) filename += `_${provider.replace(/[^a-zA-Z0-9]/g, '_')}`
      if (firstPageOnly) filename += '_page1'
      filename += '.zip'

      a.download = filename
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
    } catch (err) {
      console.error('Erreur création ZIP:', err)
      alert('Erreur lors de la création du ZIP')
    } finally {
      setDownloading(prev => {
        const next = new Set(prev)
        next.delete(key)
        return next
      })
    }
  }

  // Filtrer les factures par recherche et année
  const filteredGrouped = useMemo(() => {
    if (!invoicesData) return null

    const { grouped, invoices } = invoicesData

    // Si pas de filtre, retourner tel quel
    if (!searchTerm && selectedYear === 'all') {
      return grouped
    }

    // Filtrer les factures
    let filteredInvoices = invoices

    if (searchTerm) {
      const term = searchTerm.toLowerCase()
      filteredInvoices = filteredInvoices.filter(inv =>
        inv.senderName.toLowerCase().includes(term) ||
        inv.subject.toLowerCase().includes(term) ||
        inv.from.toLowerCase().includes(term)
      )
    }

    if (selectedYear !== 'all') {
      filteredInvoices = filteredInvoices.filter(inv => inv.year === parseInt(selectedYear))
    }

    // Regrouper
    const result: InvoicesGrouped = {}
    for (const invoice of filteredInvoices) {
      const yearKey = String(invoice.year)
      const monthKey = `${String(invoice.month).padStart(2, '0')} - ${invoice.monthName}`
      const provider = invoice.senderName

      if (!result[yearKey]) result[yearKey] = {}
      if (!result[yearKey][monthKey]) result[yearKey][monthKey] = {}
      if (!result[yearKey][monthKey][provider]) result[yearKey][monthKey][provider] = []

      result[yearKey][monthKey][provider].push(invoice)
    }

    return result
  }, [invoicesData, searchTerm, selectedYear])

  // Formater la taille du fichier
  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

  // Gestion de l'upload de fichiers
  const handleFileSelect = useCallback((files: FileList | null) => {
    if (!files) return
    const pdfFiles = Array.from(files).filter(f => f.type === 'application/pdf' || f.name.toLowerCase().endsWith('.pdf'))
    setUploadedFiles(prev => [...prev, ...pdfFiles])
  }, [])

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(true)
  }, [])

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)
  }, [])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)
    handleFileSelect(e.dataTransfer.files)
  }, [handleFileSelect])

  const removeUploadedFile = (index: number) => {
    setUploadedFiles(prev => prev.filter((_, i) => i !== index))
  }

  const clearUploadedFiles = () => {
    setUploadedFiles([])
  }

  const processUploadedPdfs = async () => {
    if (uploadedFiles.length === 0) return

    setIsProcessingUpload(true)

    try {
      if (uploadedFiles.length === 1) {
        // Un seul fichier : télécharger directement le PDF
        const file = uploadedFiles[0]
        const arrayBuffer = await file.arrayBuffer()

        const response = await fetch(`${API_URL}/pdf/extract-first-page`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/pdf'
          },
          body: arrayBuffer
        })

        if (!response.ok) throw new Error('Erreur extraction')

        const blob = await response.blob()
        const url = window.URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url

        // Nom du fichier
        const ext = file.name.substring(file.name.lastIndexOf('.'))
        const baseName = file.name.substring(0, file.name.lastIndexOf('.'))
        a.download = `${baseName}_page1${ext}`

        document.body.appendChild(a)
        a.click()
        window.URL.revokeObjectURL(url)
        document.body.removeChild(a)
      } else {
        // Plusieurs fichiers : créer un ZIP
        const filesData = await Promise.all(
          uploadedFiles.map(async (file) => {
            const arrayBuffer = await file.arrayBuffer()
            const base64 = btoa(
              new Uint8Array(arrayBuffer).reduce((data, byte) => data + String.fromCharCode(byte), '')
            )
            return { name: file.name, data: base64 }
          })
        )

        const response = await fetch(`${API_URL}/pdf/extract-first-pages-zip`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ files: filesData })
        })

        if (!response.ok) throw new Error('Erreur extraction')

        const blob = await response.blob()
        const url = window.URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = 'pdfs_page1.zip'
        document.body.appendChild(a)
        a.click()
        window.URL.revokeObjectURL(url)
        document.body.removeChild(a)
      }

      // Vider les fichiers après succès
      setUploadedFiles([])
    } catch (err) {
      console.error('Erreur traitement PDFs:', err)
      alert('Erreur lors du traitement des PDFs')
    } finally {
      setIsProcessingUpload(false)
    }
  }

  // Formater la date
  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('fr-FR', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric'
    })
  }

  // Écran d'authentification
  if (needsAuth && !loading) {
    return (
      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8">
          <div className="text-center max-w-md mx-auto">
            <div className="w-16 h-16 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <AlertCircle className="w-8 h-8 text-blue-600" />
            </div>
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Connexion Gmail requise</h2>
            <p className="text-gray-600 mb-6">
              Pour accéder aux factures, vous devez autoriser l'application à lire les emails de
              <span className="font-medium"> invoicesmaline@gmail.com</span>
            </p>
            {authUrl ? (
              <a
                href={authUrl}
                className="inline-flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg font-medium transition-colors"
              >
                <ExternalLink className="w-5 h-5" />
                Se connecter avec Google
              </a>
            ) : (
              <div className="text-red-600">
                <p>Impossible de générer le lien d'authentification.</p>
                <p className="text-sm mt-2">Vérifiez que le fichier <code>gmail-credentials.json</code> est présent.</p>
              </div>
            )}
          </div>
        </div>
      </div>
    )
  }

  // Écran de chargement
  if (loading) {
    return (
      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
            <p className="text-gray-600 mt-4">Chargement des factures...</p>
          </div>
        </div>
      </div>
    )
  }

  // Écran d'erreur
  if (error) {
    return (
      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-8">
          <div className="text-center">
            <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <AlertCircle className="w-8 h-8 text-red-600" />
            </div>
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Erreur</h2>
            <p className="text-gray-600 mb-6">{error}</p>
            <button
              onClick={checkAuthAndFetch}
              className="inline-flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg font-medium transition-colors"
            >
              <RefreshCw className="w-5 h-5" />
              Réessayer
            </button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Stats */}
      {invoicesData && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-blue-100 rounded-lg">
                <FileText className="w-6 h-6 text-blue-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Total factures</p>
                <p className="text-2xl font-bold text-gray-900">{invoicesData.stats.totalInvoices}</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-green-100 rounded-lg">
                <Download className="w-6 h-6 text-green-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Fichiers PDF</p>
                <p className="text-2xl font-bold text-gray-900">{invoicesData.stats.totalAttachments}</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-purple-100 rounded-lg">
                <Building className="w-6 h-6 text-purple-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Prestataires</p>
                <p className="text-2xl font-bold text-gray-900">{invoicesData.stats.providers}</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-orange-100 rounded-lg">
                <Calendar className="w-6 h-6 text-orange-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Années</p>
                <p className="text-2xl font-bold text-gray-900">{invoicesData.stats.years.length}</p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Filtres et actions */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex flex-wrap items-center gap-4">
          {/* Recherche */}
          <div className="relative flex-1 min-w-[200px]">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
            <input
              type="text"
              placeholder="Rechercher par prestataire, sujet..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>

          {/* Filtre année */}
          <select
            value={selectedYear}
            onChange={(e) => setSelectedYear(e.target.value)}
            className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="all">Toutes les années</option>
            {invoicesData?.stats.years.map(year => (
              <option key={year} value={year}>{year}</option>
            ))}
          </select>

          {/* Option première page uniquement */}
          <label className="inline-flex items-center gap-2 px-4 py-2 bg-orange-50 border border-orange-200 rounded-lg cursor-pointer hover:bg-orange-100 transition-colors">
            <input
              type="checkbox"
              checked={firstPageOnly}
              onChange={(e) => setFirstPageOnly(e.target.checked)}
              className="w-4 h-4 text-orange-600 border-gray-300 rounded focus:ring-orange-500"
            />
            <Scissors className="w-4 h-4 text-orange-600" />
            <span className="text-sm text-orange-800">1ère page uniquement</span>
          </label>

          {/* Bouton rafraîchir */}
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="inline-flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`w-5 h-5 ${refreshing ? 'animate-spin' : ''}`} />
            Rafraîchir
          </button>

          {/* Télécharger tout */}
          <button
            onClick={() => downloadZip()}
            disabled={downloading.has('zip-all-all-all')}
            className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download className={`w-5 h-5 ${downloading.has('zip-all-all-all') ? 'animate-pulse' : ''}`} />
            Tout télécharger
          </button>
        </div>
      </div>

      {/* Section Upload PDF */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-orange-50 to-amber-50">
          <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
            <Scissors className="w-5 h-5 text-orange-600" />
            Extraire la 1ère page de PDFs
          </h3>
          <p className="text-sm text-gray-600 mt-1">
            Déposez vos fichiers PDF pour extraire uniquement la première page
          </p>
        </div>

        <div className="p-6">
          {/* Zone de drop */}
          <div
            className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
              isDragging
                ? 'border-orange-500 bg-orange-50'
                : 'border-gray-300 hover:border-gray-400 bg-gray-50'
            }`}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".pdf,application/pdf"
              multiple
              className="hidden"
              onChange={(e) => handleFileSelect(e.target.files)}
            />

            <FileUp className={`w-12 h-12 mx-auto mb-4 ${isDragging ? 'text-orange-500' : 'text-gray-400'}`} />

            <p className="text-gray-600 mb-2">
              Glissez-déposez vos fichiers PDF ici
            </p>
            <p className="text-sm text-gray-500 mb-4">ou</p>
            <button
              onClick={() => fileInputRef.current?.click()}
              className="inline-flex items-center gap-2 px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white rounded-lg transition-colors"
            >
              <Upload className="w-4 h-4" />
              Sélectionner des fichiers
            </button>
          </div>

          {/* Liste des fichiers uploadés */}
          {uploadedFiles.length > 0 && (
            <div className="mt-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-gray-700">
                  {uploadedFiles.length} fichier{uploadedFiles.length > 1 ? 's' : ''} sélectionné{uploadedFiles.length > 1 ? 's' : ''}
                </span>
                <button
                  onClick={clearUploadedFiles}
                  className="text-sm text-red-600 hover:text-red-800"
                >
                  Tout supprimer
                </button>
              </div>

              <div className="space-y-2 max-h-48 overflow-y-auto">
                {uploadedFiles.map((file, index) => (
                  <div
                    key={`${file.name}-${index}`}
                    className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2"
                  >
                    <div className="flex items-center gap-2 min-w-0">
                      <FileText className="w-4 h-4 text-red-500 flex-shrink-0" />
                      <span className="text-sm text-gray-700 truncate">{file.name}</span>
                      <span className="text-xs text-gray-400 flex-shrink-0">
                        ({formatSize(file.size)})
                      </span>
                    </div>
                    <button
                      onClick={() => removeUploadedFile(index)}
                      className="p-1 hover:bg-gray-200 rounded"
                    >
                      <X className="w-4 h-4 text-gray-500" />
                    </button>
                  </div>
                ))}
              </div>

              <button
                onClick={processUploadedPdfs}
                disabled={isProcessingUpload}
                className="mt-4 w-full inline-flex items-center justify-center gap-2 px-4 py-3 bg-orange-600 hover:bg-orange-700 text-white rounded-lg transition-colors disabled:opacity-50"
              >
                {isProcessingUpload ? (
                  <>
                    <RefreshCw className="w-5 h-5 animate-spin" />
                    Extraction en cours...
                  </>
                ) : (
                  <>
                    <Scissors className="w-5 h-5" />
                    Extraire la 1ère page {uploadedFiles.length > 1 ? '(ZIP)' : ''}
                  </>
                )}
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Arborescence des factures */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
          <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
            <FolderOpen className="w-5 h-5 text-yellow-600" />
            Factures par Année / Mois / Prestataire
          </h3>
        </div>

        <div className="divide-y divide-gray-100">
          {filteredGrouped && Object.keys(filteredGrouped).length > 0 ? (
            Object.entries(filteredGrouped)
              .sort(([a], [b]) => parseInt(b) - parseInt(a))
              .map(([year, months]) => (
                <div key={year} className="bg-white">
                  {/* Niveau Année */}
                  <div
                    className="flex items-center gap-3 px-6 py-3 hover:bg-gray-50 cursor-pointer group"
                    onClick={() => toggleYear(year)}
                  >
                    {expandedYears.has(year) ? (
                      <ChevronDown className="w-5 h-5 text-gray-500" />
                    ) : (
                      <ChevronRight className="w-5 h-5 text-gray-500" />
                    )}
                    <Calendar className="w-5 h-5 text-blue-600" />
                    <span className="font-semibold text-gray-900">{year}</span>
                    <span className="text-sm text-gray-500">
                      ({Object.keys(months).length} mois)
                    </span>
                    <button
                      onClick={(e) => {
                        e.stopPropagation()
                        downloadZip(parseInt(year))
                      }}
                      disabled={downloading.has(`zip-${year}-all-all`)}
                      className="ml-auto opacity-0 group-hover:opacity-100 inline-flex items-center gap-1 px-3 py-1 text-sm bg-blue-100 hover:bg-blue-200 text-blue-700 rounded-lg transition-all"
                    >
                      <Download className={`w-4 h-4 ${downloading.has(`zip-${year}-all-all`) ? 'animate-pulse' : ''}`} />
                      ZIP
                    </button>
                  </div>

                  {/* Niveau Mois */}
                  {expandedYears.has(year) && (
                    <div className="pl-8">
                      {Object.entries(months)
                        .sort(([a], [b]) => b.localeCompare(a))
                        .map(([month, providers]) => {
                          const monthKey = `${year}-${month}`
                          const monthNum = parseInt(month.split(' ')[0])

                          return (
                            <div key={monthKey} className="border-l-2 border-gray-200">
                              <div
                                className="flex items-center gap-3 px-6 py-2 hover:bg-gray-50 cursor-pointer group"
                                onClick={() => toggleMonth(monthKey)}
                              >
                                {expandedMonths.has(monthKey) ? (
                                  <ChevronDown className="w-4 h-4 text-gray-500" />
                                ) : (
                                  <ChevronRight className="w-4 h-4 text-gray-500" />
                                )}
                                <FolderOpen className="w-4 h-4 text-yellow-500" />
                                <span className="font-medium text-gray-800">{month}</span>
                                <span className="text-sm text-gray-500">
                                  ({Object.keys(providers).length} prestataires)
                                </span>
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    downloadZip(parseInt(year), monthNum)
                                  }}
                                  disabled={downloading.has(`zip-${year}-${monthNum}-all`)}
                                  className="ml-auto inline-flex items-center gap-1 px-2 py-1 text-xs bg-green-100 hover:bg-green-200 text-green-700 rounded transition-all"
                                >
                                  <Download className={`w-3 h-3 ${downloading.has(`zip-${year}-${monthNum}-all`) ? 'animate-pulse' : ''}`} />
                                  Télécharger ce mois
                                </button>
                              </div>

                              {/* Niveau Prestataire */}
                              {expandedMonths.has(monthKey) && (
                                <div className="pl-8">
                                  {Object.entries(providers)
                                    .sort(([a], [b]) => a.localeCompare(b))
                                    .map(([provider, invoices]) => {
                                      const providerKey = `${monthKey}-${provider}`

                                      return (
                                        <div key={providerKey} className="border-l-2 border-gray-100">
                                          <div
                                            className="flex items-center gap-3 px-6 py-2 hover:bg-gray-50 cursor-pointer group"
                                            onClick={() => toggleProvider(providerKey)}
                                          >
                                            {expandedProviders.has(providerKey) ? (
                                              <ChevronDown className="w-4 h-4 text-gray-400" />
                                            ) : (
                                              <ChevronRight className="w-4 h-4 text-gray-400" />
                                            )}
                                            <Building className="w-4 h-4 text-purple-500" />
                                            <span className="text-gray-700">{provider}</span>
                                            <span className="text-sm text-gray-400">
                                              ({invoices.length} facture{invoices.length > 1 ? 's' : ''})
                                            </span>
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation()
                                                downloadZip(parseInt(year), monthNum, provider)
                                              }}
                                              disabled={downloading.has(`zip-${year}-${monthNum}-${provider}`)}
                                              className="ml-auto opacity-0 group-hover:opacity-100 inline-flex items-center gap-1 px-2 py-1 text-xs bg-purple-100 hover:bg-purple-200 text-purple-700 rounded transition-all"
                                            >
                                              <Download className={`w-3 h-3 ${downloading.has(`zip-${year}-${monthNum}-${provider}`) ? 'animate-pulse' : ''}`} />
                                              ZIP
                                            </button>
                                          </div>

                                          {/* Niveau Factures */}
                                          {expandedProviders.has(providerKey) && (
                                            <div className="pl-12 pb-2">
                                              {invoices.map((invoice) => (
                                                <div key={invoice.id} className="py-2 border-b border-gray-50 last:border-0">
                                                  <div className="flex items-start gap-3">
                                                    <FileText className="w-4 h-4 text-gray-400 mt-0.5" />
                                                    <div className="flex-1 min-w-0">
                                                      <p className="text-sm text-gray-700 truncate" title={invoice.subject}>
                                                        {invoice.subject}
                                                      </p>
                                                      <p className="text-xs text-gray-400">{formatDate(invoice.date)}</p>
                                                    </div>
                                                  </div>
                                                  {/* Pièces jointes */}
                                                  <div className="mt-2 pl-7 space-y-1">
                                                    {invoice.attachments.map((attachment) => {
                                                      const dlKey = `${invoice.messageId}-${attachment.attachmentId}`
                                                      return (
                                                        <button
                                                          key={attachment.attachmentId}
                                                          onClick={() => downloadAttachment(invoice.messageId, attachment.attachmentId, attachment.filename)}
                                                          disabled={downloading.has(dlKey)}
                                                          className="flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 hover:bg-blue-50 px-2 py-1 rounded transition-colors"
                                                        >
                                                          {downloading.has(dlKey) ? (
                                                            <RefreshCw className="w-3 h-3 animate-spin" />
                                                          ) : (
                                                            <Download className="w-3 h-3" />
                                                          )}
                                                          <span className="truncate max-w-[200px]">{attachment.filename}</span>
                                                          <span className="text-xs text-gray-400">({formatSize(attachment.size)})</span>
                                                        </button>
                                                      )
                                                    })}
                                                  </div>
                                                </div>
                                              ))}
                                            </div>
                                          )}
                                        </div>
                                      )
                                    })}
                                </div>
                              )}
                            </div>
                          )
                        })}
                    </div>
                  )}
                </div>
              ))
          ) : (
            <div className="px-6 py-12 text-center text-gray-500">
              <FileText className="w-12 h-12 mx-auto text-gray-300 mb-4" />
              <p>Aucune facture trouvée</p>
              {searchTerm && (
                <button
                  onClick={() => setSearchTerm('')}
                  className="mt-2 text-blue-600 hover:underline"
                >
                  Effacer la recherche
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Note de configuration */}
      {invoicesData && invoicesData.stats.totalInvoices === 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <AlertCircle className="w-5 h-5 text-yellow-600 mt-0.5" />
            <div>
              <h4 className="font-medium text-yellow-800">Aucune facture détectée</h4>
              <p className="text-sm text-yellow-700 mt-1">
                Les factures sont recherchées parmi les emails avec des pièces jointes PDF contenant les mots
                "facture", "invoice", "receipt" ou "reçu". Vérifiez que vos emails de factures correspondent à ces critères.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default FacturesPage
