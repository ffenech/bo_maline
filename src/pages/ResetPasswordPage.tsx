import { useEffect, useMemo, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { supabase } from '../lib/supabase'

export default function ResetPasswordPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const [ready, setReady] = useState(false)
  const [updating, setUpdating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState(false)
  const [pwd, setPwd] = useState('')
  const [pwd2, setPwd2] = useState('')
  const [showPwd, setShowPwd] = useState(false)

  const searchParams = useMemo(() => new URLSearchParams(location.search), [location.search])
  const hashParams = useMemo(() => new URLSearchParams(location.hash.replace(/^#/, '')), [location.hash])

  useEffect(() => {
    const init = async () => {
      try {
        // Try PKCE code flow
        const code = searchParams.get('code') || hashParams.get('code')
        if (code) {
          const { error } = await supabase.auth.exchangeCodeForSession(code)
          if (error) throw error
        }

        // Try direct token session (hash or query)
        const accessToken = hashParams.get('access_token') || searchParams.get('access_token')
        const refreshToken = hashParams.get('refresh_token') || searchParams.get('refresh_token')
        if (accessToken && refreshToken) {
          const { error } = await supabase.auth.setSession({ access_token: accessToken, refresh_token: refreshToken })
          if (error) throw error
        }

        // Try token_hash flows (invite/recovery/email change)
        const tokenHash = searchParams.get('token_hash') || searchParams.get('token') || hashParams.get('token_hash') || hashParams.get('token')
        const type = (searchParams.get('type') || hashParams.get('type') || '').toLowerCase()
        if (tokenHash && (type === 'recovery' || type === 'signup' || type === 'invite' || type === 'email')) {
          try {
            // Attempt verify via token_hash when available (cast for broader compatibility)
            const { error } = await (supabase.auth as any).verifyOtp({ token_hash: tokenHash, type: type === 'invite' ? 'signup' : type })
            if (error) throw error
          } catch (_) {
            // ignore if unsupported; we'll rely on existing session or show message
          }
        }
      } catch (_err) {
        // We'll still render page; session might not be set
      } finally {
        setReady(true)
      }
    }
    void init()
  }, [searchParams])

  const [hasSession, setHasSession] = useState(false)
  useEffect(() => {
    const check = async () => {
      const { data } = await supabase.auth.getSession()
      setHasSession(!!data.session)
    }
    if (ready) void check()
  }, [ready])

  const canUpdate = ready && hasSession

  const handleUpdate = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    if (pwd.length < 8) {
      setError('Le mot de passe doit contenir au moins 8 caractères.')
      return
    }
    if (pwd !== pwd2) {
      setError('Les mots de passe ne correspondent pas.')
      return
    }
    setUpdating(true)
    try {
      const { error } = await supabase.auth.updateUser({ password: pwd })
      if (error) {
        setError(mapUpdateError(error))
        return
      }
      setSuccess(true)
      // Option: auto redirect after short delay
      setTimeout(() => navigate('/login'), 1500)
    } catch (_) {
      setError('Impossible de mettre à jour le mot de passe. Réessayez.')
    } finally {
      setUpdating(false)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        <div className="bg-white shadow-md rounded-xl border border-gray-200 p-8">
          <div className="mb-6 text-center">
            <div className="mx-auto w-12 h-12 rounded-full bg-blue-600 flex items-center justify-center text-white font-bold">B</div>
            <h1 className="mt-3 text-xl font-semibold text-gray-900">Réinitialiser le mot de passe</h1>
            <p className="text-sm text-gray-500">Choisissez un nouveau mot de passe</p>
          </div>

          {!ready ? (
            <div className="text-gray-600">Chargement…</div>
          ) : !canUpdate ? (
            <div className="space-y-4">
              <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
                Lien invalide ou manquant. Cliquez sur le lien de l’email, ou redemandez un lien.
              </div>
              <div className="text-center">
                <Link to="/forgot-password" className="text-blue-600 hover:underline">Redemander un lien</Link>
              </div>
            </div>
          ) : success ? (
            <div className="space-y-4">
              <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                Mot de passe mis à jour. Redirection vers la connexion…
              </div>
              <div className="text-center">
                <Link to="/login" className="text-blue-600 hover:underline">Aller à la connexion</Link>
              </div>
            </div>
          ) : (
            <form onSubmit={handleUpdate} className="space-y-4">
              {error && (
                <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div>
              )}

              <div>
                <label htmlFor="pwd" className="block text-sm font-medium text-gray-700">Nouveau mot de passe</label>
                <div className="mt-1 relative">
                  <input
                    id="pwd"
                    type={showPwd ? 'text' : 'password'}
                    value={pwd}
                    onChange={(e) => setPwd(e.target.value)}
                    className="block w-full rounded-lg border border-gray-300 bg-white px-3 py-2 pr-20 text-gray-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-200"
                    placeholder="••••••••"
                    autoComplete="new-password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPwd((s) => !s)}
                    className="absolute right-2 top-1/2 -translate-y-1/2 text-sm text-gray-600 hover:text-gray-800 px-2 py-1"
                  >
                    {showPwd ? 'Masquer' : 'Afficher'}
                  </button>
                </div>
                <p className="mt-1 text-xs text-gray-500">Au moins 8 caractères.</p>
              </div>

              <div>
                <label htmlFor="pwd2" className="block text-sm font-medium text-gray-700">Confirmer le mot de passe</label>
                <input
                  id="pwd2"
                  type={showPwd ? 'text' : 'password'}
                  value={pwd2}
                  onChange={(e) => setPwd2(e.target.value)}
                  className="mt-1 block w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-gray-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-200"
                  placeholder="••••••••"
                  autoComplete="new-password"
                />
              </div>

              <button
                type="submit"
                disabled={updating || !pwd || !pwd2}
                className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-white font-medium shadow hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {updating ? 'Mise à jour…' : 'Mettre à jour le mot de passe'}
              </button>

              <div className="text-center text-sm">
                <Link to="/login" className="text-gray-600 hover:underline">Retour à la connexion</Link>
              </div>
            </form>
          )}
        </div>
      </div>
    </div>
  )
}

function mapUpdateError(error: any): string {
  const msg = String(error?.message || '').toLowerCase()
  if (msg.includes('expired')) return 'Le lien a expiré. Redemandez un nouveau lien.'
  if (msg.includes('invalid')) return 'Lien invalide. Redemandez un nouveau lien.'
  return 'Impossible de mettre à jour le mot de passe.'
}
