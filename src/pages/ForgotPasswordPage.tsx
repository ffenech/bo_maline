import { useState } from 'react'
import { Link } from 'react-router-dom'
import { supabase } from '../lib/supabase'

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [sent, setSent] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      // Utiliser VITE_APP_BASE_URL si défini, sinon fallback sur window.location.origin
      const baseUrl = import.meta.env.VITE_APP_BASE_URL || window.location.origin
      const redirectTo = `${baseUrl.replace(/\/$/, '')}/reset-password`
      const { error } = await supabase.auth.resetPasswordForEmail(email.trim(), { redirectTo })
      if (error) {
        setError(mapResetError(error))
        return
      }
      setSent(true)
    } catch (err: any) {
      setError('Une erreur est survenue. Réessayez plus tard.')
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
            <h1 className="mt-3 text-xl font-semibold text-gray-900">Mot de passe oublié</h1>
            <p className="text-sm text-gray-500">Recevez un lien de réinitialisation par email</p>
          </div>

          {sent ? (
            <div className="space-y-4">
              <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                Si un compte existe pour {email}, un email a été envoyé avec les instructions.
              </div>
              <div className="text-center">
                <Link to="/login" className="text-blue-600 hover:underline">Retour à la connexion</Link>
              </div>
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="space-y-4">
              {error && (
                <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div>
              )}
              <div>
                <label htmlFor="email" className="block text-sm font-medium text-gray-700">Email</label>
                <input
                  id="email"
                  type="email"
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="mt-1 block w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-gray-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-200"
                  placeholder="vous@exemple.com"
                />
              </div>
              <button
                type="submit"
                disabled={submitting || !email}
                className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-white font-medium shadow hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {submitting ? 'Envoi…' : 'Envoyer le lien'}
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

function mapResetError(error: any): string {
  const msg = String(error?.message || '').toLowerCase()
  if (msg.includes('rate limit')) return 'Trop de tentatives. Réessayez plus tard.'
  return 'Impossible d’envoyer l’email. Vérifiez l’adresse et réessayez.'
}

