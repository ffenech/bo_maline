import { useState } from 'react'
import type { FormEvent } from 'react'
import { Link, Navigate, useLocation } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

export default function LoginPage() {
  const { user, signIn, loading } = useAuth()
  const location = useLocation()
  const from = (location.state as any)?.from?.pathname || '/'

  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [showPassword, setShowPassword] = useState(false)
  const [submitting, setSubmitting] = useState(false)

  if (loading) {
    return (
      <div className="min-h-screen grid place-items-center bg-gray-50">
        <div className="text-gray-600">Chargement…</div>
      </div>
    )
  }

  if (user) {
    return <Navigate to={from} replace />
  }

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      const { error } = await signIn(email.trim(), password)
      if (error) {
        setError(mapLoginError(error))
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        <div className="bg-white shadow-md rounded-xl border border-gray-200 p-8">
          <div className="mb-6 text-center">
            <div className="mx-auto w-12 h-12 rounded-full bg-blue-600 flex items-center justify-center text-white font-bold">B</div>
            <h1 className="mt-3 text-xl font-semibold text-gray-900">Connexion au Back Office</h1>
            <p className="text-sm text-gray-500">Veuillez vous authentifier pour continuer</p>
          </div>

          {error && (
            <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {error}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-gray-700">Email</label>
              <input
                id="email"
                type="email"
                autoComplete="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="mt-1 block w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-gray-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-200"
                placeholder="vous@exemple.com"
              />
            </div>

            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700">Mot de passe</label>
              <div className="mt-1 relative">
                <input
                  id="password"
                  type={showPassword ? 'text' : 'password'}
                  autoComplete="current-password"
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="block w-full rounded-lg border border-gray-300 bg-white px-3 py-2 pr-20 text-gray-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-200"
                  placeholder="••••••••"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((s) => !s)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-sm text-gray-600 hover:text-gray-800 px-2 py-1"
                >
                  {showPassword ? 'Masquer' : 'Afficher'}
                </button>
              </div>
            </div>

            <button
              type="submit"
              disabled={submitting || !email || !password}
              className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-white font-medium shadow hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {submitting ? 'Connexion…' : 'Se connecter'}
            </button>
            <div className="text-center text-sm">
              <Link to="/forgot-password" className="text-gray-600 hover:underline">Mot de passe oublié ?</Link>
            </div>
          </form>
        </div>

        <p className="mt-4 text-center text-xs text-gray-500">
          Accès réservé au personnel autorisé.
        </p>
      </div>
    </div>
  )
}

function mapLoginError(error: any): string {
  const msg = String(error?.message || '').toLowerCase()
  if (msg.includes('invalid login') || msg.includes('invalid credentials')) return 'Identifiants incorrects. Vérifiez votre email et mot de passe.'
  if (msg.includes('email not confirmed')) return 'Email non confirmé. Vérifiez votre boîte mail.'
  return 'Connexion impossible. Réessayez plus tard.'
}
