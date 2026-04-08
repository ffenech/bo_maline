import { useState, useEffect, useMemo, lazy, Suspense } from 'react'
import { TrendingUp, Calendar, BarChart3 } from 'lucide-react'
import { cachedFetch } from '../lib/fetchCache'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'

const LeadsV1Page = lazy(() => import('./LeadsV1Page'))
const LeadsIntegrityPage = lazy(() => import('./LeadsIntegrityPage'))

interface DailyLead {
  date: string
  total_leads: number
}

interface DailyPhoneStats {
  date: string
  leads_with_phone: number
  leads_with_validated_phone: number
}

interface VisitorData {
  [date: string]: number
}

interface RemarksData {
  [date: string]: string
}

type TabType = 'all' | 'fr' | 'es' | 'v1' | 'integrity'

function LeadsPage() {
  const [activeTab, setActiveTab] = useState<TabType>('all')

  // Données pour tous les clients
  const [dailyLeads, setDailyLeads] = useState<DailyLead[]>([])
  const [dailyPhoneStats, setDailyPhoneStats] = useState<DailyPhoneStats[]>([])

  // Données pour les clients espagnols
  const [dailyLeadsEs, setDailyLeadsEs] = useState<DailyLead[]>([])
  const [dailyPhoneStatsEs, setDailyPhoneStatsEs] = useState<DailyPhoneStats[]>([])

  const [visitors, setVisitors] = useState<VisitorData>({})
  const [visitorsEs, setVisitorsEs] = useState<VisitorData>({})
  const [remarks, setRemarks] = useState<RemarksData>({})
  const [clientCountFr, setClientCountFr] = useState<number>(0)
  const [clientCountEs, setClientCountEs] = useState<number>(0)
  const [loading, setLoading] = useState(true)
  const [editingCell, setEditingCell] = useState<string | null>(null)

  // Données France = tous - Espagne
  const dailyLeadsFr = useMemo(() => {
    const esMap = new Map(dailyLeadsEs.map(d => [d.date.split('T')[0], d.total_leads]))
    return dailyLeads.map(d => ({
      date: d.date,
      total_leads: d.total_leads - (esMap.get(d.date.split('T')[0]) || 0)
    }))
  }, [dailyLeads, dailyLeadsEs])

  const dailyPhoneStatsFr = useMemo(() => {
    const esMap = new Map(dailyPhoneStatsEs.map(d => [d.date.split('T')[0], d]))
    return dailyPhoneStats.map(d => {
      const es = esMap.get(d.date.split('T')[0])
      return {
        date: d.date,
        leads_with_phone: d.leads_with_phone - (es?.leads_with_phone || 0),
        leads_with_validated_phone: d.leads_with_validated_phone - (es?.leads_with_validated_phone || 0)
      }
    })
  }, [dailyPhoneStats, dailyPhoneStatsEs])

  // Visiteurs FR = tous - ES
  const visitorsFr = useMemo(() => {
    const result: VisitorData = {}
    for (const [date, count] of Object.entries(visitors)) {
      result[date] = count - (visitorsEs[date] || 0)
    }
    return result
  }, [visitors, visitorsEs])

  // Sélectionner les données selon l'onglet actif
  const currentDailyLeads = activeTab === 'es' ? dailyLeadsEs : activeTab === 'fr' ? dailyLeadsFr : dailyLeads
  const currentDailyPhoneStats = activeTab === 'es' ? dailyPhoneStatsEs : activeTab === 'fr' ? dailyPhoneStatsFr : dailyPhoneStats
  const currentVisitors = activeTab === 'es' ? visitorsEs : activeTab === 'fr' ? visitorsFr : visitors

  useEffect(() => {
    const fetchData = async () => {
      const apiUrl = import.meta.env.VITE_API_URL || '/api'
      const todayStr = new Date().toISOString().split('T')[0]

      // Fonction de filtrage des dates (à partir du 14 septembre 2024)
      const filterByDate = <T extends { date: string }>(data: T[]): T[] => {
        const startDate = '2024-09-14'
        return data.filter((item) => {
          const dateStr = item.date.split('T')[0]
          if (dateStr < startDate) return false
          if (dateStr > todayStr) return false
          return true
        })
      }

      try {
        // Lancer toutes les requêtes en parallèle avec cache
        const [
          leadsData,
          phoneData,
          leadsEsData,
          phoneEsData,
          visitorsData,
          visitorsEsData,
          remarksData,
          clientLocalesData
        ] = await Promise.all([
          cachedFetch<DailyLead[]>(`${apiUrl}/leads/v2-daily`),
          cachedFetch<DailyPhoneStats[]>(`${apiUrl}/leads/v2-daily-phone`),
          cachedFetch<DailyLead[]>(`${apiUrl}/leads/v2-daily-es`),
          cachedFetch<DailyPhoneStats[]>(`${apiUrl}/leads/v2-daily-phone-es`),
          cachedFetch<VisitorData>(`${apiUrl}/ga4/daily-visitors-v2`),
          cachedFetch<any[]>(`${apiUrl}/ga4/daily-visitors-es`),
          cachedFetch<RemarksData>(`${apiUrl}/remarks`),
          cachedFetch<any>(`${apiUrl}/pub-stats?period=30d`)
        ])

        // Traiter les leads (tous)
        setDailyLeads(filterByDate(leadsData || []).reverse())

        // Traiter les statistiques téléphone (tous)
        setDailyPhoneStats(filterByDate(phoneData || []).reverse())

        // Traiter les leads espagnols
        setDailyLeadsEs(filterByDate(leadsEsData || []).reverse())

        // Traiter les statistiques téléphone espagnols
        setDailyPhoneStatsEs(filterByDate(phoneEsData || []).reverse())

        // Traiter les visiteurs GA4 (tous)
        const visitorsMap: VisitorData = {}
        if (Array.isArray(visitorsData)) {
          visitorsData.forEach((item: { date: string; visitors: number }) => {
            visitorsMap[item.date] = item.visitors
          })
        }

        // Traiter les visiteurs GA4 ES
        const visitorsEsMap: VisitorData = {}
        if (Array.isArray(visitorsEsData)) {
          visitorsEsData.forEach((item: { date: string; visitors: number }) => {
            visitorsEsMap[item.date] = item.visitors
          })
        }

        // Récupérer les visiteurs temps réel pour aujourd'hui (cache court 30s)
        try {
          const todayData = await cachedFetch<{ v2: number; v1: number; es: number; date: string }>(
            `${apiUrl}/ga4/today-visitors`, undefined, 30 * 1000
          )
          if (todayData?.date) {
            // Écraser les valeurs du jour avec les données temps réel (plus fraîches)
            if (todayData.v2 > 0) visitorsMap[todayData.date] = todayData.v2
            if (todayData.es > 0) visitorsEsMap[todayData.date] = todayData.es
          }
        } catch (e) {
          console.warn('⚠️ Visiteurs temps réel non disponibles:', e)
        }

        setVisitors(visitorsMap)
        setVisitorsEs(visitorsEsMap)

        // Traiter les remarques
        setRemarks(remarksData || {})

        // Compter les clients actifs par locale
        const activeClients = clientLocalesData?.clients || []
        setClientCountFr(activeClients.filter((c: any) => (c.locale || 'fr_FR') === 'fr_FR').length)
        setClientCountEs(activeClients.filter((c: any) => c.locale === 'es_ES').length)
      } catch (error) {
        console.error('Erreur:', error)
        // Fallback sur localStorage pour les remarques
        const savedRemarks = localStorage.getItem('leads-remarks')
        if (savedRemarks) {
          setRemarks(JSON.parse(savedRemarks))
        }
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [])

  const formatDateShort = (dateString: string) => {
    if (!dateString) return '-'
    return new Date(dateString).toLocaleDateString('fr-FR', {
      day: '2-digit',
      month: 'short'
    })
  }


  const handleRemarkChange = async (date: string, value: string) => {
    const updatedRemarks = {
      ...remarks,
      [date]: value
    }

    try {
      // Essayer de sauvegarder dans l'API
      const response = await fetch(`${import.meta.env.VITE_API_URL || '/api'}/remarks`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ date, remark: value }),
      })

      if (response.ok) {
        // Mettre à jour l'état local
        setRemarks(updatedRemarks)
        console.log('Remarque sauvegardée en BDD:', date, value)
      } else {
        // Fallback sur localStorage si la table n'existe pas
        setRemarks(updatedRemarks)
        localStorage.setItem('leads-remarks', JSON.stringify(updatedRemarks))
        console.log('Remarque sauvegardée dans localStorage (table BDD non créée):', date, value)
      }
    } catch (error) {
      // Fallback sur localStorage en cas d'erreur
      setRemarks(updatedRemarks)
      localStorage.setItem('leads-remarks', JSON.stringify(updatedRemarks))
      console.log('Remarque sauvegardée dans localStorage (erreur BDD):', date, value)
    }
    setEditingCell(null)
  }

  const totalLeads = useMemo(() => {
    return currentDailyLeads.reduce((sum, day) => sum + day.total_leads, 0)
  }, [currentDailyLeads])

  const averageLeads = useMemo(() => {
    if (currentDailyLeads.length === 0) return 0
    return Math.round(totalLeads / currentDailyLeads.length)
  }, [currentDailyLeads, totalLeads])

  // Seuil minimum de visiteurs pour exclure les jours où GA4 n'avait pas assez de données
  const MIN_VISITORS = 50

  // Calculer le taux de conversion moyen et l'écart type
  const conversionStats = useMemo(() => {
    const rates: number[] = []
    currentDailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      if (visitorsCount >= MIN_VISITORS) {
        const rate = (day.total_leads / visitorsCount) * 100
        rates.push(rate)
      }
    })

    if (rates.length === 0) return { average: 0, stdDev: 0 }

    const average = rates.reduce((sum, rate) => sum + rate, 0) / rates.length
    const variance = rates.reduce((sum, rate) => sum + Math.pow(rate - average, 2), 0) / rates.length
    const stdDev = Math.sqrt(variance)

    return { average, stdDev }
  }, [currentDailyLeads, visitors])

  // Calculer le taux de conversion sur les 10 derniers jours
  const conversionStatsLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    const rates: number[] = []
    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      if (visitorsCount >= MIN_VISITORS) {
        const rate = (day.total_leads / visitorsCount) * 100
        rates.push(rate)
      }
    })

    if (rates.length === 0) return { average: 0, stdDev: 0 }

    const average = rates.reduce((sum, rate) => sum + rate, 0) / rates.length
    const variance = rates.reduce((sum, rate) => sum + Math.pow(rate - average, 2), 0) / rates.length
    const stdDev = Math.sqrt(variance)

    return { average, stdDev }
  }, [currentDailyLeads, visitors])

  // Calculer le % tel sur les 10 derniers jours
  const phonePercentLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    let totalLeadsCount = 0
    let totalWithPhone = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalLeadsCount += day.total_leads
      totalWithPhone += phoneStat?.leads_with_phone || 0
    })

    return totalLeadsCount > 0 ? (totalWithPhone / totalLeadsCount) * 100 : 0
  }, [currentDailyLeads, currentDailyPhoneStats])

  // Taux conversion vendeur avec tel (10 derniers jours)
  const conversionPhoneLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    let totalWithPhone = 0
    let totalVisitors = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalWithPhone += phoneStat?.leads_with_phone || 0
      totalVisitors += visitorsCount
    })

    return totalVisitors > 0 ? (totalWithPhone / totalVisitors) * 100 : 0
  }, [currentDailyLeads, currentDailyPhoneStats, visitors])

  // Taux conversion vendeur avec tel validés (10 derniers jours)
  const conversionValidatedPhoneLast10Days = useMemo(() => {
    const last10Days = currentDailyLeads.slice(0, 10)
    let totalValidated = 0
    let totalVisitors = 0

    last10Days.forEach(day => {
      const dateKey = day.date.split('T')[0]
      const visitorsCount = currentVisitors[dateKey] || 0
      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
      totalValidated += phoneStat?.leads_with_validated_phone || 0
      totalVisitors += visitorsCount
    })

    return totalVisitors > 0 ? (totalValidated / totalVisitors) * 100 : 0
  }, [currentDailyLeads, currentDailyPhoneStats, visitors])

  // Commits impactants du site estimateur (estimerlogement.fr)
  // Historique à partir du 29/09/2025
  const siteCommits: { date: string; impact: 'positive' | 'negative' | 'neutral'; summary: string }[] = [
    // ─── Oct 2025 ─── Internationalisation ES + tracking
    { date: '2025-10-02', impact: 'neutral', summary: 'Refonte i18n FR/ES : traduction complète tunnel + autocomplete Google Maps Espagne (country=es) — gros chantier multi-fichiers' },
    { date: '2025-10-03', impact: 'positive', summary: 'Extraction code postal depuis Google Places + envoi city_es/zipcode_es à l\'API prix ES — corrige estimations ES sans CP' },
    { date: '2025-10-05', impact: 'neutral', summary: 'Force language des bulles à la création d\'agence + i18n pages légales (CGU, mentions, charte RGPD)' },
    { date: '2025-10-06', impact: 'neutral', summary: 'Support Catalan (langue + traductions emails/SMS)' },
    { date: '2025-10-07', impact: 'positive', summary: 'Tracking Meta Pixel : normalisation value/currency Lead (200-500€, 2 décimales, EUR) — passage de 55% à 90%+ d\'événements valides sur 26 ad sets + retrait Eruda mobile (page /estimation plus légère)' },
    { date: '2025-10-12', impact: 'neutral', summary: 'Création auto agences ES + simulation SMS pour slugs demo_ + traduction email estimation non-vendeur ES + page estimation entièrement traduite' },
    { date: '2025-10-14', impact: 'positive', summary: 'Validation tel ES (9 chiffres commençant par 6-9) côté client + serveur, format CM.com (0034XXXXXXXXX) — corrige rejets SMS Espagne' },
    { date: '2025-10-15', impact: 'neutral', summary: 'Endpoint insert_rdv_es pour Espagne + séparation email propriétaire / email agent dans payload sendrdv' },
    { date: '2025-10-21', impact: 'positive', summary: 'Correction IP client pour tracking Meta/Google sur VPS OVH (CF-Connecting-IP → X-Forwarded-For → X-Real-IP) + endpoint /api/debug/ip-headers + IP correcte sur send-sms — résout problème 0.0.0.0 dans events PageView/Lead/Complete Registration' },
    { date: '2025-10-22', impact: 'neutral', summary: 'Nouvelle API agences avec récupération codes postaux depuis base externe + filtrage auto agences/CP invalides' },
    { date: '2025-10-23', impact: 'negative', summary: 'Validation adresse BAN stricte au clic Continuer : si voie numérotée détectée et numéro absent → blocage de la redirection. Risque élevé de drop-off si BAN ne reconnaît pas l\'adresse exacte' },
    { date: '2025-10-29', impact: 'positive', summary: 'Fix extraction floor (etage_bien depuis URL avec fallback etage) — adresse RDC ne plante plus' },
    { date: '2025-10-30', impact: 'negative', summary: 'Désactivation insertion estimation à l\'étape téléphone (nom/email manquants obligatoires API) + format tel 0034 ES — moins d\'estimations enregistrées en amont du tunnel' },

    // ─── Nov 2025 ─── Multi-domaine ES + autocomplete DOM-TOM
    { date: '2025-11-04', impact: 'positive', summary: 'Fix double appel get_price_es.php pour non-vendeurs + validation stricte du prix retourné — moins d\'erreurs et calculs dupliqués' },
    { date: '2025-11-10', impact: 'positive', summary: 'Garantie d\'un uniqueId + agent valide avant get_price + acceptation IDs numériques pour projet de vente' },
    { date: '2025-11-16', impact: 'positive', summary: 'Traductions ES complètes (hors admin) : autocomplete ES, get_price_es.php avec city_es/zipcode_es, SMS +34 9 chiffres, tracking PageView/Lead/Complete Registration en ES, email + footer ES' },
    { date: '2025-11-18', impact: 'neutral', summary: 'Bloc prise de RDV : suppression vérification numéro dans formulaires + traduction jours/mois ES + ajout locale=es dans payload sendrdv' },
    { date: '2025-11-20', impact: 'neutral', summary: 'Mode simulation envoi SMS renforcé pour Espagne + recherche agence par external ID si non trouvée par ID interne' },
    { date: '2025-11-21', impact: 'positive', summary: 'Simplification vérif uniqueId/agentId avant get_price : crée un uniqueId si manquant, appelle l\'API agent si invalide — moins de blocages page estimation' },
    { date: '2025-11-26', impact: 'positive', summary: 'Cache localStorage pour adresse + reverse geocoding Google Maps + agent rechargé au changement d\'adresse + fix paramètre agence dans toutes les redirections — moins de pertes contextuelles' },
    { date: '2025-11-27', impact: 'positive', summary: 'Autocomplétion + geocoding étendus aux DOM-TOM (Réunion, Guadeloupe, Martinique) + force-dynamic sur routes API utilisant request — fin des erreurs de build en prod sur ces pages' },

    // ─── Déc 2025 ─── Refactor backend, alertes erreurs, refonte page estimation
    { date: '2025-12-07', impact: 'positive', summary: 'Refactor récupération agent (priorité API + fallback retries) + agency by slug au lieu d\'external ID + retries get_price selon erreur — moins de pages estimation cassées' },
    { date: '2025-12-08', impact: 'neutral', summary: 'Format tel ES (0034) côté API SMS + EstimationContext démarré dès sélection adresse pour traçage du tunnel' },
    { date: '2025-12-09', impact: 'neutral', summary: 'Tracking multi-domaine (estimerlogement.fr + valorar-vivienda.es) + langage HTML dynamique selon domaine + injection country dans user data' },
    { date: '2025-12-11', impact: 'positive', summary: 'Fix Facebook : on ne modifie plus le cookie fbc (anti-pénalité score qualité Meta) + ajout données correspondance avancée (zp, ct, country, currency)' },
    { date: '2025-12-14', impact: 'neutral', summary: 'Refonte handling agency ID dans tunnel + URL estimation avec slug + external ID dans data structure' },
    { date: '2025-12-15', impact: 'positive', summary: 'Facebook Graph API v18 → v21 + extraction fbclid direct depuis URL + génération fbc depuis fbclid si cookie absent (best practice Meta 2025) + debug WebView Facebook' },
    { date: '2025-12-17', impact: 'positive', summary: 'Refonte complète page /estimation : layout mobile-first + design system emerald — meilleure lisibilité et conversion finale' },
    { date: '2025-12-18', impact: 'positive', summary: 'Page estimation : calendrier de prise de RDV intégré + auto-scroll + pré-estimation personnalisée + alertes 500 via logAndAlert500 sur toutes routes tracking (FB, GA4, Google Ads) + force-dynamic sur API analytics' },
    { date: '2025-12-19', impact: 'neutral', summary: 'Refactor analyse funnel : étapes entry/main, stats sessions/agences/sources de trafic, taux drop-off — meilleure mesure interne mais pas d\'effet user' },
    { date: '2025-12-20', impact: 'positive', summary: 'ClientErrorBoundary global + useGlobalErrorHandler pour erreurs JS/promises non gérées — robustesse accrue, moins de pages blanches' },
    { date: '2025-12-21', impact: 'positive', summary: 'Système alertes email comprehensif : capture erreurs Google Maps, suivi redirections EstimationGuard, timeout API 30s, cooldown alertes 1 min — détection rapide des problèmes prod' },
    { date: '2025-12-22', impact: 'positive', summary: 'Insertion estimation passe en MySQL direct (suppression appel API externe) + lead zone en query MySQL directe + venteId pour identifier projet de vente + GTM unique event ID — fiabilité et perfo accrues' },
    { date: '2025-12-24', impact: 'positive', summary: 'Format tel +33 E.164 + soumission tel non-bloquante + agentId fallback + résolution agenceId via userId + alertes non-bloquantes paramètres manquants — moins d\'utilisateurs bloqués au tel' },
    { date: '2025-12-25', impact: 'positive', summary: 'Filtrage erreurs WebView Facebook + Safari mode privé + localStorage erreurs + iOS < 15.4 Google Maps — moins de bruit dans les alertes' },
    { date: '2025-12-29', impact: 'positive', summary: 'Refactor ConversionHero : flow validation adresse via BAN simplifié + fallback OpenCage si CP/ville manquants + détection bots dans error reporting + Next.js 14.2.35' },

    // ─── Jan 2026 ─── Stabilisation + refonte UI
    { date: '2026-01-06', impact: 'positive', summary: 'Formulaire de fallback complet avec autocomplétion BAN si Google Maps HS (loadError ou googleMapsApiError) + monitoring activation fallback en prod' },
    { date: '2026-01-19', impact: 'negative', summary: 'Bouton page tel : passage de full-width à compact — cible tactile réduite sur mobile, possible baisse de taux de clic' },
    { date: '2026-01-21', impact: 'neutral', summary: 'Refonte layout pages tunnel + délai loader popup 600→800ms — friction visuelle légèrement accrue' },
    { date: '2026-01-23', impact: 'neutral', summary: 'Migration GTM vers Stape (1st party tagging) — risque de perte de tracking si mal configuré, gain de qualité de matching côté serveur si OK' },

    // ─── Fév 2026 ─── Tracking, polyfills, refonte étapes
    { date: '2026-02-05', impact: 'positive', summary: 'Filtrage erreurs in-app browsers (Facebook, Instagram, etc.) + erreurs non-bloquantes — alertes plus pertinentes' },
    { date: '2026-02-09', impact: 'neutral', summary: 'Chat bubbles dynamiques chargées depuis API (cache 5 min) + rendu via API data — risque ralentissement si API lente, gain de flexibilité' },
    { date: '2026-02-10', impact: 'neutral', summary: 'Session tracking estimation + analytics reset functionality + sessionId dans liens email — changement de méthode de mesure (peut altérer compa historique)' },
    { date: '2026-02-13', impact: 'neutral', summary: 'Refactor page tel : meilleur handling téléphone + construction des paramètres URL — pas d\'effet visible user' },
    { date: '2026-02-16', impact: 'positive', summary: 'Fix format tel SMS (0033→+33 E.164) + agency title dynamique dans SMS — corrige échecs livraison SMS pour numéros mal formatés' },
    { date: '2026-02-19', impact: 'positive', summary: 'Logique de rotation d\'attribution d\'agent + résolution agency ID sur toutes les pages estimation — distribution leads plus équitable' },
    { date: '2026-02-20', impact: 'negative', summary: 'Champ "nb chambres" rendu obligatoire + refonte styles boutons + ordre étapes modifié + polyfill HTMLDialogElement (anciens iOS) — friction accrue mais compatibilité élargie' },
    { date: '2026-02-23', impact: 'neutral', summary: 'Refactor handling param prix + handling agency dans navigation cross-pages' },
    { date: '2026-02-24', impact: 'positive', summary: 'Bulles personnalisées sur page estimation-renove + meilleure handling Google Maps errors + polyfill HTMLDialogElement — plus de pages cassées sur navigateurs anciens' },
    { date: '2026-02-26', impact: 'positive', summary: 'Renforcement gestion paramètre agence dans toutes les composantes + filtres erreurs supplémentaires GlobalErrorHandler' },
    { date: '2026-02-27', impact: 'positive', summary: 'SMS de relance après abandon : délai 30 min → 5 min + génération short URL + middleware routing follow-up — rattrape utilisateurs engagés à chaud' },

    // ─── Mar 2026 ─── Perfo, SEO, Twilio, refontes
    { date: '2026-03-02', impact: 'positive', summary: 'Refonte UI estimation-coordonnees + ajout données travaux dans tunnel + fix erreurs extensions navigateur (injFunc) — propre et complet' },
    { date: '2026-03-03', impact: 'positive', summary: 'Quick wins SEO : title/description optimisés, og:image, schemas JSON-LD, H1, noindex /conversion + perfo Core Web Vitals (cache headers, WebP, tree-shake Google Maps, defer GTM, code splitting)' },
    { date: '2026-03-04', impact: 'neutral', summary: 'Conditional display "fourchette de valeur" sur ConversionHero selon paramètre agence (afficherFourchette + mettreEnAvantAgence)' },
    { date: '2026-03-05', impact: 'positive', summary: 'Refactor Facebook tracking (sendFacebookEvent unifié) + IRIS code via GPS lookup + scheduling RDV configurable par agence — plus stable et flexible' },
    { date: '2026-03-06', impact: 'positive', summary: 'Facebook Advanced Matching avec re-init manuelle des user data + système Google Ratings avec admin + redirection slugs agences + couleurs primaires dynamiques — meilleure qualité tracking pub' },
    { date: '2026-03-10', impact: 'neutral', summary: 'Système de logs estimation côté serveur + page admin logs + migration agency location vers OpenCage Geocoding + "apartamento" → "piso" en ES' },
    { date: '2026-03-11', impact: 'neutral', summary: 'Migration SMS OTP : CM.com → Twilio Verify (en test)' },
    { date: '2026-03-12', impact: 'negative', summary: 'Code SMS verification : 6 → 4 chiffres (test) — modifie l\'UX de saisie du code, à rebasculer rapidement' },
    { date: '2026-03-13', impact: 'negative', summary: 'Bascule Twilio Verify → Twilio SMS direct + retour à 5 chiffres + provider CM.com de nouveau pour SMS marketing — friction au moment critique de la vérification' },
    { date: '2026-03-15', impact: 'positive', summary: 'Refonte tracking analytics : intégration sur toutes étapes estimation, suppression doublons useAnalytics, upsert au lieu de create (anti race conditions), ordre des étapes corrigé — bundle plus léger + données plus fiables' },
    { date: '2026-03-16', impact: 'positive', summary: 'Polyfills MediaQueryList.addEventListener (anciens Safari) + nouveau tunnel estimer-loyer (location) + ajustements gradients UI' },
    { date: '2026-03-19', impact: 'positive', summary: 'Refactor init agent : chargement plus fiable via liens SMS de relance (récupération uniqueId/agent depuis URL et localStorage)' },
    { date: '2026-03-21', impact: 'neutral', summary: 'SEO multi-locale : meta tags dynamiques selon locale + redesign sélecteur de langue + traduction bulles par défaut selon locale + format tel Belgique' },
    { date: '2026-03-24', impact: 'neutral', summary: 'Système de redirection slugs agences déplacé du middleware vers next.config.js + admin redirection management + paramètres vendeur (nom/email) configurables par agence' },
    { date: '2026-03-25', impact: 'neutral', summary: 'Geocoding API route + filtre acquisitions 14 derniers jours' },
    { date: '2026-03-27', impact: 'positive', summary: 'Streamline redirection adresse dans AskAddressPage + ConversionHero — moins de cas edge avec adresses partiellement reconnues' },
    { date: '2026-03-30', impact: 'neutral', summary: 'Intégration tunnel location dans projet principal pour location.estimerlogement.fr + Google Maps Spanish localization + suppression validation regex adresse RDV ES + endpoint MALINE_API mis à jour' },
    { date: '2026-03-31', impact: 'positive', summary: 'Audit SEO complet (105 villes, 10 guides, 28 sous-guides, schemas, comparateur, FAQ, OG image) + dashboard SEO admin avec KPIs et checklist mensuelle + corrections double header pages SEO + logo net dans header SEO' },

    // ─── Avril 2026 ─── A/B testing complet + veille concurrentielle
    { date: '2026-04-01', impact: 'positive', summary: 'Validation adresse douce : suppression blocage BAN strict du 23/10 + bouton bypass "Continuer quand même" + middleware admin auth + headers sécurité — corrige enfin la régression d\'octobre' },
    { date: '2026-04-02', impact: 'positive', summary: 'Système A/B test HP : variant_b utilise BAN autocomplete + géoloc IP pour prioriser suggestions + tracking avec/sans numéro de rue + suggestions BAN complémentaires + admin avec verdicts statistiques (volume, confiance, p-value)' },
    { date: '2026-04-03', impact: 'positive', summary: 'Page admin liste-estimations avec comparaison BDD V3 (PostgreSQL via tunnel SSH) + matching tel+date + filtres par plage de dates + détail conversions A/B test cliquable' },
    { date: '2026-04-04', impact: 'positive', summary: '3 nouveaux A/B tests (CTA homepage, confiance tel, tunnel compact) + funnel par variante + segmentation device + dupliquer/modifier/archiver tests + A/B test animation-popup (durée animation projet vente) + filtres multi-select admin + retry loadTestsForPage si sessionId pas prêt + abréviations voies (Av., Bd., Chem.)' },
    { date: '2026-04-05', impact: 'positive', summary: 'Variant_b validation-adresse devient le comportement par défaut (3 champs N°/Rue/CP+Ville) — résultat du test A/B' },
    { date: '2026-04-07', impact: 'positive', summary: 'Variant_c cta-homepage devient le CTA par défaut ("VOIR LE PRIX DE MON BIEN") + segmentation mobile/desktop dans résultats des tests + persistance snapshots veille concurrentielle en SQLite' },
  ]

  // Données hebdomadaires pour les courbes de taux de conversion
  const weeklyConversionData = useMemo(() => {
    if (!currentDailyLeads.length) return []
    // Exclure uniquement aujourd'hui (journée incomplète)
    const now = new Date()
    const todayKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
    // Grouper par semaine ISO (lundi → dimanche)
    const weekMap = new Map<string, { visitors: number; leads: number; validatedPhone: number }>()
    currentDailyLeads.forEach(day => {
      const dateKey = day.date.split('T')[0]
      if (dateKey === todayKey) return
      const d = new Date(dateKey + 'T00:00:00')
      // Trouver le lundi de la semaine
      const dayOfWeek = d.getDay()
      const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek
      const monday = new Date(d)
      monday.setDate(d.getDate() + mondayOffset)
      const weekKey = `${monday.getFullYear()}-${String(monday.getMonth() + 1).padStart(2, '0')}-${String(monday.getDate()).padStart(2, '0')}`

      const visitorsCount = currentVisitors[dateKey] || 0
      const phoneStat = currentDailyPhoneStats.find(s => s.date.split('T')[0] === dateKey)
      const validatedPhone = phoneStat?.leads_with_validated_phone || 0

      const existing = weekMap.get(weekKey) || { visitors: 0, leads: 0, validatedPhone: 0 }
      existing.visitors += visitorsCount
      existing.leads += day.total_leads
      existing.validatedPhone += validatedPhone
      weekMap.set(weekKey, existing)
    })

    // Ne garder que les 20 dernières semaines avec assez de visiteurs (min 200/semaine)
    const MIN_WEEKLY_VISITORS = 200
    return Array.from(weekMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .filter(([weekStart, data]) => data.visitors >= MIN_WEEKLY_VISITORS && weekStart >= '2025-09-29')
      .map(([weekStart, data]) => {
        const d = new Date(weekStart + 'T00:00:00')
        const endOfWeek = new Date(d)
        endOfWeek.setDate(d.getDate() + 6)
        const weekEndKey = `${endOfWeek.getFullYear()}-${String(endOfWeek.getMonth() + 1).padStart(2, '0')}-${String(endOfWeek.getDate()).padStart(2, '0')}`
        // Label X-axis = date de fin de semaine (dimanche)
        const label = `${endOfWeek.getDate()}/${endOfWeek.getMonth() + 1}`
        const startLabel = `${d.getDate()}/${d.getMonth() + 1}`
        // Trouver les commits de cette semaine
        const weekCommits = siteCommits.filter(c => c.date >= weekStart && c.date <= weekEndKey)
        return {
          week: label,
          weekStart,
          weekEnd: weekEndKey,
          weekRangeLabel: `${startLabel} → ${label}`,
          tauxConversionLead: data.visitors > 0 ? parseFloat(((data.leads / data.visitors) * 100).toFixed(2)) : 0,
          tauxConversionTelValide: data.visitors > 0 ? parseFloat(((data.validatedPhone / data.visitors) * 100).toFixed(2)) : 0,
          commits: weekCommits,
        }
      })
  }, [currentDailyLeads, currentDailyPhoneStats, currentVisitors])

  // Fonction pour détecter le type d'anomalie (> 1 écart-type)
  const getAnomalyType = (rate: number): 'normal' | 'low' | 'high' => {
    const { average, stdDev } = conversionStats
    const diff = rate - average
    if (Math.abs(diff) <= stdDev) return 'normal'
    if (diff < 0) return 'low' // En dessous de la moyenne - alerte rouge
    return 'high' // Au dessus de la moyenne - positif vert
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="text-gray-600 mt-4">Chargement des leads...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-full overflow-hidden">
      {/* Onglets */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveTab('all')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'all'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Tous les clients ({clientCountFr + clientCountEs})
          </button>
          <button
            onClick={() => setActiveTab('fr')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'fr'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            France ({clientCountFr})
          </button>
          <button
            onClick={() => setActiveTab('es')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'es'
                ? 'border-orange-500 text-orange-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Espagne ({clientCountEs})
          </button>
          <button
            onClick={() => setActiveTab('v1')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'v1'
                ? 'border-amber-500 text-amber-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Ancien esti
          </button>
          <button
            onClick={() => setActiveTab('integrity')}
            className={`py-4 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'integrity'
                ? 'border-red-500 text-red-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Intégrité V2/V3
          </button>
        </nav>
      </div>

      {activeTab === 'v1' ? (
        <Suspense fallback={
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-amber-600 mx-auto"></div>
              <p className="text-gray-600 mt-4">Chargement...</p>
            </div>
          </div>
        }>
          <LeadsV1Page />
        </Suspense>
      ) : activeTab === 'integrity' ? (
        <Suspense fallback={
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-red-600 mx-auto"></div>
              <p className="text-gray-600 mt-4">Chargement...</p>
            </div>
          </div>
        }>
          <LeadsIntegrityPage />
        </Suspense>
      ) : (
      <>
      {/* Statistiques */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-blue-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-blue-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Total Leads étape 3 vendeurs</p>
              <p className="text-xl font-bold text-gray-900">{totalLeads.toLocaleString('fr-FR')}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-green-100 rounded-lg">
              <BarChart3 className="w-5 h-5 text-green-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Moyenne leads étape 3 vendeur / jour</p>
              <p className="text-xl font-bold text-gray-900">{averageLeads}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-purple-100 rounded-lg">
              <Calendar className="w-5 h-5 text-purple-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Jours suivis</p>
              <p className="text-xl font-bold text-gray-900">{currentDailyLeads.length}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-orange-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-orange-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion étape 3 vendeurs</p>
              <p className="text-xl font-bold text-gray-900">{conversionStats.average.toFixed(2)}%</p>
              <p className="text-xs text-gray-500 mt-0.5">±{conversionStats.stdDev.toFixed(2)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-cyan-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-cyan-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion étape 3 (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{conversionStatsLast10Days.average.toFixed(2)}%</p>
              <p className="text-xs text-gray-500 mt-0.5">±{conversionStatsLast10Days.stdDev.toFixed(2)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-pink-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-pink-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">% tel (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{phonePercentLast10Days.toFixed(1)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-teal-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-teal-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion vendeur avec tel (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{conversionPhoneLast10Days.toFixed(2)}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center">
            <div className="p-2 bg-indigo-100 rounded-lg">
              <TrendingUp className="w-5 h-5 text-indigo-600" />
            </div>
            <div className="ml-3">
              <p className="text-xs font-medium text-gray-600">Taux conversion vendeur avec tel validés (10 derniers jours)</p>
              <p className="text-xl font-bold text-gray-900">{conversionValidatedPhoneLast10Days.toFixed(2)}%</p>
            </div>
          </div>
        </div>
      </div>

      {/* Tableau */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900">Détail journalier {activeTab === 'es' ? '- Espagne' : activeTab === 'fr' ? '- France' : ''}</h3>
        </div>
        <div className="overflow-x-auto max-w-full">
          <div className="inline-block min-w-full align-middle">
            <div className="overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">
                  Métrique
                </th>
                {currentDailyLeads.map((day, index) => (
                  <th key={index} className="px-2 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[80px]">
                    {formatDateShort(day.date)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {currentDailyLeads.length === 0 ? (
                <tr>
                  <td colSpan={currentDailyLeads.length + 1} className="px-6 py-8 text-center text-gray-500">
                    Aucun lead trouvé {activeTab === 'es' ? 'pour les clients espagnols' : activeTab === 'fr' ? 'pour les clients français' : 'pour les comptes Estimateur V2'} depuis le 1er septembre
                  </td>
                </tr>
              ) : (
                <>
                  {/* Ligne Nombre visiteurs */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Nombre visiteurs
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      // Normaliser la date de la BDD (ISO) en format YYYY-MM-DD
                      const dateKey = day.date.split('T')[0]
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            {currentVisitors[dateKey] || 0}
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Leads vendeurs */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Leads vendeurs
                    </td>
                    {currentDailyLeads.map((day, index) => (
                      <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                          {day.total_leads}
                        </span>
                      </td>
                    ))}
                  </tr>
                  {/* Ligne Taux conversion lead vendeur */}
                  <tr className="hover:bg-gray-50 transition-colors bg-purple-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-purple-50 z-10">
                      Taux conversion lead vendeur
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const visitorsCount = currentVisitors[dateKey] || 0
                      const leadsCount = day.total_leads
                      const rate = visitorsCount > 0
                        ? (leadsCount / visitorsCount) * 100
                        : 0
                      const conversionRate = rate.toFixed(2)
                      const anomalyType = getAnomalyType(rate)

                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center relative">
                          {anomalyType === 'low' && (
                            <div className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full animate-pulse z-20"></div>
                          )}
                          {anomalyType === 'high' && (
                            <div className="absolute top-1 right-1 w-2 h-2 bg-green-500 rounded-full animate-pulse z-20"></div>
                          )}
                          <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-bold ${
                            anomalyType === 'low'
                              ? 'bg-red-100 text-red-800 ring-1 ring-red-400 shadow-lg'
                              : anomalyType === 'high'
                              ? 'bg-green-100 text-green-800 ring-1 ring-green-400 shadow-lg'
                              : 'bg-purple-100 text-purple-800'
                          }`}>
                            {conversionRate}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Leads vendeurs avec tel */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Leads vendeurs avec tel
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-teal-100 text-teal-800">
                            {phoneStat?.leads_with_phone || 0}
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne % leads vendeurs avec tel */}
                  <tr className="hover:bg-gray-50 transition-colors bg-teal-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-teal-50 z-10">
                      % leads vendeurs avec tel
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      const leadsWithPhone = phoneStat?.leads_with_phone || 0
                      const totalLeadsDay = day.total_leads
                      const percentage = totalLeadsDay > 0
                        ? (leadsWithPhone / totalLeadsDay) * 100
                        : 0
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-teal-100 text-teal-800">
                            {percentage.toFixed(1)}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Taux conversion avec tel */}
                  <tr className="hover:bg-gray-50 transition-colors bg-teal-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-teal-50 z-10">
                      Taux conversion avec tel
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const visitorsCount = currentVisitors[dateKey] || 0
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      const leadsWithPhone = phoneStat?.leads_with_phone || 0
                      const rate = visitorsCount > 0
                        ? (leadsWithPhone / visitorsCount) * 100
                        : 0
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-bold bg-teal-100 text-teal-800">
                            {rate.toFixed(2)}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Leads vendeurs avec tel validés */}
                  <tr className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white z-10">
                      Leads vendeurs avec tel validés
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800">
                            {phoneStat?.leads_with_validated_phone || 0}
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne % leads vendeurs avec tel validés */}
                  <tr className="hover:bg-gray-50 transition-colors bg-indigo-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-indigo-50 z-10">
                      % leads vendeurs avec tel validés
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      const phoneStat = currentDailyPhoneStats.find(stat => stat.date.split('T')[0] === dateKey)
                      const validatedPhone = phoneStat?.leads_with_validated_phone || 0
                      const totalLeadsDay = day.total_leads
                      const percentage = totalLeadsDay > 0
                        ? (validatedPhone / totalLeadsDay) * 100
                        : 0
                      return (
                        <td key={index} className="px-2 py-3 whitespace-nowrap text-sm text-center">
                          <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800">
                            {percentage.toFixed(1)}%
                          </span>
                        </td>
                      )
                    })}
                  </tr>
                  {/* Ligne Remarques */}
                  <tr className="hover:bg-gray-50 transition-colors bg-yellow-50">
                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-yellow-50 z-10">
                      Remarque
                    </td>
                    {currentDailyLeads.map((day, index) => {
                      const dateKey = day.date.split('T')[0]
                      return (
                        <td key={index} className="px-2 py-3 text-sm text-center relative">
                          {editingCell === `remark-${dateKey}` ? (
                            <>
                              {/* Backdrop invisible pour fermer en cliquant à l'extérieur */}
                              <div
                                className="fixed inset-0 z-40"
                                onClick={(e) => {
                                  e.stopPropagation()
                                  // Récupérer la valeur du textarea avant de fermer
                                  const textarea = document.querySelector(`textarea[data-date="${dateKey}"]`) as HTMLTextAreaElement
                                  if (textarea) {
                                    handleRemarkChange(dateKey, textarea.value)
                                  } else {
                                    setEditingCell(null)
                                  }
                                }}
                              />
                              <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 z-50 mb-2">
                                <div className="bg-white border-2 border-yellow-400 rounded-lg shadow-2xl p-3 w-64">
                                  <textarea
                                    data-date={dateKey}
                                    className="w-full px-3 py-2 text-sm border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-yellow-500 resize-none"
                                    defaultValue={remarks[dateKey] || ''}
                                    autoFocus
                                    rows={4}
                                    placeholder="Ajouter une remarque pour ce jour..."
                                    onClick={(e) => e.stopPropagation()}
                                    onKeyDown={(e) => {
                                      if (e.key === 'Enter' && e.ctrlKey) {
                                        handleRemarkChange(dateKey, e.currentTarget.value)
                                      } else if (e.key === 'Escape') {
                                        setEditingCell(null)
                                      }
                                    }}
                                  />
                                  <div className="mt-2 flex items-center justify-between">
                                    <div className="text-xs text-gray-500">
                                      Ctrl+Enter pour sauvegarder, Échap pour annuler
                                    </div>
                                    {remarks[dateKey] && (
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          handleRemarkChange(dateKey, '')
                                        }}
                                        className="text-xs text-red-600 hover:text-red-800 font-medium px-2 py-1 hover:bg-red-50 rounded transition-colors"
                                      >
                                        Supprimer
                                      </button>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </>
                          ) : null}
                          <div
                            className="cursor-pointer hover:bg-yellow-100 px-2 py-1 rounded min-h-[24px] text-xs group relative"
                            onClick={() => setEditingCell(`remark-${dateKey}`)}
                          >
                            {remarks[dateKey] ? (
                              <>
                                {/* Badge avec remarque */}
                                <div className="flex items-center justify-center">
                                  <span className="inline-flex items-center px-2 py-1 rounded-full bg-blue-500 text-white font-semibold ring-2 ring-blue-600 shadow-md">
                                    Note
                                  </span>
                                </div>
                                {/* Tooltip au survol */}
                                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 whitespace-normal w-64 max-w-xs">
                                  <div className="font-semibold mb-1 text-blue-300">Note du {formatDateShort(day.date)}</div>
                                  <div className="text-gray-100">{remarks[dateKey]}</div>
                                  {/* Flèche */}
                                  <div className="absolute top-full left-1/2 transform -translate-x-1/2 -mt-1">
                                    <div className="border-8 border-transparent border-t-gray-900"></div>
                                  </div>
                                </div>
                              </>
                            ) : (
                              <span className="text-gray-400 hover:text-gray-600">+</span>
                            )}
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                </>
              )}
            </tbody>
          </table>
            </div>
          </div>
        </div>
      </div>

      {/* Courbes hebdomadaires taux de conversion */}
      {weeklyConversionData.length > 1 && (() => {
        const impactColor = (impact: string) => impact === 'positive' ? '#16a34a' : impact === 'negative' ? '#dc2626' : '#9ca3af'
        const impactLabel = (impact: string) => impact === 'positive' ? '↑' : impact === 'negative' ? '↓' : '•'

        // Custom dot qui marque les semaines avec commits
        const CommitDot = (props: any) => {
          const { cx, cy, payload } = props
          if (!payload?.commits?.length) return <circle cx={cx} cy={cy} r={3} fill={props.stroke} />
          const mainImpact = payload.commits.find((c: any) => c.impact !== 'neutral')?.impact || 'neutral'
          return (
            <g>
              <circle cx={cx} cy={cy} r={6} fill={impactColor(mainImpact)} stroke="#fff" strokeWidth={2} />
              <text x={cx} y={cy - 12} textAnchor="middle" fontSize={10} fontWeight="bold" fill={impactColor(mainImpact)}>
                {impactLabel(mainImpact)}
              </text>
            </g>
          )
        }

        // Custom tooltip avec détail des commits
        const CommitTooltip = ({ active, payload, dataKey }: any) => {
          if (!active || !payload?.[0]) return null
          const data = payload[0].payload
          const value = data[dataKey]
          const commits = data.commits || []
          return (
            <div className="bg-white border border-gray-300 rounded-lg shadow-lg p-3 max-w-xs">
              <p className="text-sm font-semibold text-gray-900 mb-1">Semaine {data.weekRangeLabel} — {value}%</p>
              {commits.length > 0 && (
                <div className="border-t border-gray-200 pt-2 mt-1 space-y-1.5">
                  <p className="text-xs font-semibold text-gray-600">Déploiements estimateur :</p>
                  {commits.map((c: any, i: number) => {
                    const [y, m, d] = c.date.split('-')
                    return (
                    <div key={i} className="flex items-start gap-1.5">
                      <span className="text-xs mt-0.5" style={{ color: impactColor(c.impact) }}>
                        {c.impact === 'positive' ? '▲' : c.impact === 'negative' ? '▼' : '●'}
                      </span>
                      <p className="text-xs text-gray-700"><span className="font-semibold text-gray-500">{d}/{m}/{y}</span> {c.summary}</p>
                    </div>
                    )
                  })}
                </div>
              )}
            </div>
          )
        }

        // Semaines avec commits pour les ReferenceLine
        const weeksWithCommits = weeklyConversionData.filter(w => w.commits.length > 0)

        return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-4">Taux conversion vendeur avec tel validés (hebdo)</h3>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={weeklyConversionData} margin={{ top: 20 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="week" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v}%`} />
                <Tooltip content={<CommitTooltip dataKey="tauxConversionTelValide" />} />
                {weeksWithCommits.map(w => {
                  const mainImpact = w.commits.find(c => c.impact !== 'neutral')?.impact || 'neutral'
                  return <ReferenceLine key={w.week} x={w.week} stroke={impactColor(mainImpact)} strokeDasharray="3 3" strokeOpacity={0.5} />
                })}
                <Line type="monotone" dataKey="tauxConversionTelValide" stroke="#6366f1" strokeWidth={2} dot={<CommitDot stroke="#6366f1" />} activeDot={{ r: 5 }} name="Taux conv. tel validés" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-4">Taux conversion lead vendeur (hebdo)</h3>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={weeklyConversionData} margin={{ top: 20 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="week" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v}%`} />
                <Tooltip content={<CommitTooltip dataKey="tauxConversionLead" />} />
                {weeksWithCommits.map(w => {
                  const mainImpact = w.commits.find(c => c.impact !== 'neutral')?.impact || 'neutral'
                  return <ReferenceLine key={w.week} x={w.week} stroke={impactColor(mainImpact)} strokeDasharray="3 3" strokeOpacity={0.5} />
                })}
                <Line type="monotone" dataKey="tauxConversionLead" stroke="#a855f7" strokeWidth={2} dot={<CommitDot stroke="#a855f7" />} activeDot={{ r: 5 }} name="Taux conv. lead vendeur" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          {/* Légende des annotations */}
          <div className="lg:col-span-2 flex items-center gap-6 text-xs text-gray-500 px-2">
            <span className="flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-full bg-green-600"></span> Commit positif pour la conversion</span>
            <span className="flex items-center gap-1"><span className="inline-block w-3 h-3 rounded-full bg-red-600"></span> Commit négatif pour la conversion</span>
            <span className="text-gray-400">Survoler un point pour voir le détail des déploiements</span>
          </div>
        </div>
        )
      })()}

      {/* Légende */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-sm font-semibold text-gray-900 mb-4">Légende & Détection d'anomalies</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-purple-100 text-purple-800">
                {conversionStats.average.toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-gray-900">Taux normal</p>
              <p className="text-xs text-gray-500">Valeur dans la plage habituelle (moyenne +/- 1 écart-type)</p>
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1 relative">
              <div className="absolute -top-1 -right-1 w-3 h-3 bg-red-500 rounded-full animate-pulse"></div>
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-red-100 text-red-800 ring-2 ring-red-400">
                {Math.max(0, conversionStats.average - conversionStats.stdDev).toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-red-900">Anomalie basse</p>
              <p className="text-xs text-gray-500">Taux anormalement bas - Nécessite une vérification</p>
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1 relative">
              <div className="absolute -top-1 -right-1 w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-bold bg-green-100 text-green-800 ring-2 ring-green-400">
                {(conversionStats.average + conversionStats.stdDev).toFixed(2)}%
              </span>
            </div>
            <div>
              <p className="text-sm font-medium text-green-900">Anomalie haute</p>
              <p className="text-xs text-gray-500">Taux anormalement haut - Performance exceptionnelle</p>
            </div>
          </div>
        </div>
        <div className="mt-4 pt-4 border-t border-gray-200 text-xs text-gray-500">
          <p>Données visiteurs depuis Google Analytics 4 (activeUsers)</p>
          <p>Leads vendeurs depuis PostgreSQL - Comptes Estimateur V2 uniquement</p>
        </div>
      </div>
      </>
      )}

    </div>
  )
}

export default LeadsPage
