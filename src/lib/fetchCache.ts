// Cache côté client pour les appels API avec déduplication des requêtes concurrentes
const CACHE_TTL_MS = 2 * 60 * 1000 // 2 minutes par défaut

interface CacheEntry<T> {
  data: T
  timestamp: number
  ttl: number
}

const cache = new Map<string, CacheEntry<unknown>>()
// Map pour stocker les promesses en cours (déduplication)
const pendingRequests = new Map<string, Promise<unknown>>()

export async function cachedFetch<T>(
  url: string,
  options?: RequestInit,
  ttl: number = CACHE_TTL_MS
): Promise<T> {
  const cacheKey = url

  // 1. Vérifier le cache
  const entry = cache.get(cacheKey) as CacheEntry<T> | undefined
  if (entry && Date.now() - entry.timestamp < entry.ttl) {
    console.log(`📦 Cache HIT: ${cacheKey}`)
    return entry.data
  }

  // 2. Vérifier si une requête est déjà en cours pour cette URL (déduplication)
  const pendingRequest = pendingRequests.get(cacheKey)
  if (pendingRequest) {
    console.log(`⏳ Dedup - réutilisation requête en cours: ${cacheKey}`)
    return pendingRequest as Promise<T>
  }

  // 3. Nouvelle requête - la stocker comme "en cours"
  const fetchPromise = (async () => {
    try {
      const response = await fetch(url, options)
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }
      const data = await response.json()

      // Stocker en cache
      cache.set(cacheKey, { data, timestamp: Date.now(), ttl })
      console.log(`💾 Cache SET: ${cacheKey}`)

      return data
    } finally {
      // Nettoyer la requête "en cours" une fois terminée
      pendingRequests.delete(cacheKey)
    }
  })()

  // Enregistrer la promesse comme "en cours"
  pendingRequests.set(cacheKey, fetchPromise)

  return fetchPromise as Promise<T>
}

// Invalider le cache pour une clé spécifique
export function invalidateClientCache(keyPattern?: string): void {
  if (keyPattern) {
    for (const key of cache.keys()) {
      if (key.includes(keyPattern)) {
        cache.delete(key)
        console.log(`🗑️ Cache INVALIDATED: ${key}`)
      }
    }
  } else {
    cache.clear()
    console.log('🗑️ Cache CLEARED')
  }
}

// Fonction pour effectuer plusieurs fetches en parallèle avec cache
export async function cachedFetchAll<T extends readonly unknown[]>(
  urls: readonly string[],
  ttl: number = CACHE_TTL_MS
): Promise<{ [K in keyof T]: T[K] }> {
  const results = await Promise.all(
    urls.map(url => cachedFetch<unknown>(url, undefined, ttl))
  )
  return results as { [K in keyof T]: T[K] }
}
