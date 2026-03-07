'use client'

import { useSearchParams } from 'next/navigation'
import { Suspense, useEffect } from 'react'
import Link from 'next/link'

function CallbackContent() {
  const params = useSearchParams()
  const token = params.get('token')
  const login = params.get('login')
  const userId = params.get('user_id')
  const error = params.get('error') ?? params.get('detail')

  // Prefer explicit user_id param; fall back to extracting from token
  let displayUserId = userId
  if (!displayUserId && token) {
    const idx2 = token.lastIndexOf('.')
    const idx1 = token.lastIndexOf('.', idx2 - 1)
    if (idx1 > 0) displayUserId = token.substring(0, idx1)
  }

  useEffect(() => {
    if (token) localStorage.setItem('lastbid_token', token)
    if (login) localStorage.setItem('lastbid_login', login)
    if (displayUserId) localStorage.setItem('lastbid_user_id', displayUserId)
  }, [token, login, displayUserId])

  if (error) {
    return (
      <div className="bg-red-950 border border-red-800 rounded-lg p-6 space-y-3">
        <h2 className="font-bold text-red-400">Błąd logowania</h2>
        <p className="text-sm text-red-300">{error}</p>
        <Link href="/" className="inline-block text-sm text-gray-400 hover:text-white underline">
          Wróć
        </Link>
      </div>
    )
  }

  return (
    <div className="bg-green-950 border border-green-800 rounded-lg p-6 space-y-4">
      <h2 className="font-bold text-green-400">Zalogowano pomyślnie!</h2>
      {login && (
        <p className="text-sm">
          Zalogowany jako: <strong className="text-white">{login}</strong>
        </p>
      )}
      {displayUserId && (
        <div className="bg-black/30 rounded px-3 py-2">
          <p className="text-xs text-gray-500 mb-1">Allegro User ID</p>
          <code className="text-green-400 text-sm break-all">{displayUserId}</code>
        </div>
      )}
      <p className="text-sm text-gray-400">
        Sesja zapisana — możesz teraz korzystać z zakładki Snipes.
      </p>
      <Link
        href="/"
        className="inline-block bg-orange-600 hover:bg-orange-500 text-white px-4 py-2 rounded font-bold transition-colors text-sm"
      >
        Przejdź do Snipes →
      </Link>
    </div>
  )
}

export default function CallbackPage() {
  return (
    <div className="max-w-xl mx-auto p-6 sm:p-10">
      <h1 className="text-xl font-bold mb-6">
        <span className="text-orange-500">Last</span>Bid — Callback
      </h1>
      <Suspense fallback={<p className="text-gray-400">Ładowanie...</p>}>
        <CallbackContent />
      </Suspense>
    </div>
  )
}
