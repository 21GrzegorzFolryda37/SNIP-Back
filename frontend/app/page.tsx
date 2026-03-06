'use client'

import { useState, useEffect, useCallback } from 'react'

const API = process.env.NEXT_PUBLIC_API_URL ?? ''

type Tab = 'health' | 'auth' | 'snipes'

interface Snipe {
  id: string
  allegro_offer_url: string
  offer_title?: string
  offer_image_url?: string
  current_price?: number
  max_bid_amount: number
  status: string
  result_message?: string
  executed_at?: string
  offer_end_time?: string
  created_at: string
}

interface HealthData {
  status: string
  environment: string
  ntp_synced: boolean
  ntp_offset_ms?: number
  active_snipes: number
  scheduler_running: boolean
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    waiting: 'bg-yellow-800 text-yellow-200',
    active: 'bg-blue-800 text-blue-200',
    executing: 'bg-purple-800 text-purple-200',
    won: 'bg-green-800 text-green-200',
    lost: 'bg-red-800 text-red-200',
    error: 'bg-red-900 text-red-300',
    cancelled: 'bg-gray-700 text-gray-300',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-bold ${colors[status] ?? 'bg-gray-700 text-gray-300'}`}>
      {status}
    </span>
  )
}

// ─── Health Tab ───────────────────────────────────────────────────────────────

function HealthTab() {
  const [data, setData] = useState<HealthData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [lastChecked, setLastChecked] = useState<Date | null>(null)

  const fetchHealth = useCallback(async () => {
    if (!API) return
    setLoading(true)
    setError('')
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 8000)
    try {
      const res = await fetch(`${API}/health`, { cache: 'no-store', signal: controller.signal })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      setData(await res.json())
      setLastChecked(new Date())
    } catch (e: unknown) {
      if (e instanceof Error && e.name === 'AbortError') {
        setError('Nie można połączyć: timeout (>8s)')
      } else {
        setError(`Nie można połączyć: ${e}`)
      }
      setData(null)
    } finally {
      clearTimeout(timeout)
      setLoading(false)
    }
  }, [])

  // Auto-refresh co 5s
  useEffect(() => {
    fetchHealth()
    const id = setInterval(fetchHealth, 5000)
    return () => clearInterval(id)
  }, [fetchHealth])

  if (!API) {
    return (
      <div className="bg-yellow-950 border border-yellow-700 rounded-lg p-6 space-y-2">
        <p className="font-bold text-yellow-400">NEXT_PUBLIC_API_URL nie jest ustawione</p>
        <p className="text-sm text-yellow-200">
          W Vercel Dashboard → Settings → Environment Variables dodaj:
        </p>
        <code className="block bg-black/40 rounded px-3 py-2 text-green-400 text-sm">
          NEXT_PUBLIC_API_URL = https://twój-backend.railway.app
        </code>
        <p className="text-xs text-yellow-600">Po dodaniu zrób Redeploy w Vercel.</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className={`w-3 h-3 rounded-full ${
            !data ? 'bg-gray-600' :
            data.status === 'ok' ? 'bg-green-500 animate-pulse' : 'bg-red-500 animate-pulse'
          }`} />
          <span className="text-sm font-medium">
            {loading && !data ? 'Łączenie...' : data ? `Backend ${data.status === 'ok' ? 'działa' : 'błąd'}` : error ? 'Niedostępny' : '—'}
          </span>
          {lastChecked && (
            <span className="text-xs text-gray-600">
              {lastChecked.toLocaleTimeString('pl-PL')}
            </span>
          )}
        </div>
        <button
          onClick={fetchHealth}
          disabled={loading}
          className="text-xs bg-gray-800 hover:bg-gray-700 disabled:opacity-40 px-3 py-1.5 rounded transition-colors"
        >
          Odśwież
        </button>
      </div>

      <div className="bg-gray-900 rounded px-3 py-2">
        <span className="text-xs text-gray-500">API: </span>
        <span className="text-xs text-gray-300">{API}</span>
      </div>

      {error && (
        <div className="bg-red-950 border border-red-800 rounded-lg p-4 text-sm text-red-300">
          {error}
        </div>
      )}

      {data && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          {[
            { label: 'Status', value: data.status, ok: data.status === 'ok' },
            { label: 'Środowisko', value: data.environment },
            { label: 'NTP sync', value: data.ntp_synced ? 'tak' : 'nie', ok: data.ntp_synced },
            {
              label: 'NTP offset',
              value: data.ntp_offset_ms != null ? `${data.ntp_offset_ms.toFixed(1)} ms` : 'N/A',
            },
            {
              label: 'Scheduler',
              value: data.scheduler_running ? 'działa' : 'zatrzymany',
              ok: data.scheduler_running,
            },
            { label: "Aktywne snipe'y", value: String(data.active_snipes) },
          ].map(({ label, value, ok }) => (
            <div key={label} className="bg-gray-900 rounded-lg p-4">
              <p className="text-xs text-gray-500">{label}</p>
              <p className={`font-bold mt-1 ${
                ok === true ? 'text-green-400' : ok === false ? 'text-red-400' : 'text-white'
              }`}>
                {value}
              </p>
            </div>
          ))}
        </div>
      )}

      <p className="text-xs text-gray-700 text-center">auto-odświeżanie co 5s</p>
    </div>
  )
}

// ─── Auth Tab ─────────────────────────────────────────────────────────────────

function AuthTab() {
  const loginUrl = `${API}/auth/login`
  return (
    <div className="space-y-5">
      <div className="bg-gray-900 rounded-lg p-6 space-y-4">
        <h2 className="font-bold">Logowanie przez Allegro OAuth2</h2>
        <p className="text-sm text-gray-400">
          Po zalogowaniu zostaniesz przekierowany na <code className="text-green-400">/callback</code> —
          skopiuj stamtąd <code className="text-green-400">allegro_user_id</code> i użyj go w zakładce Snipes.
        </p>
        {API ? (
          <a
            href={loginUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-block bg-orange-600 hover:bg-orange-500 text-white px-5 py-2.5 rounded-lg font-bold transition-colors"
          >
            Zaloguj przez Allegro
          </a>
        ) : (
          <p className="text-yellow-500 text-sm">Najpierw ustaw NEXT_PUBLIC_API_URL.</p>
        )}
      </div>
    </div>
  )
}

// ─── Snipes Tab ───────────────────────────────────────────────────────────────

function SnipesTab() {
  const [userId, setUserId] = useState('')
  const [url, setUrl] = useState('')
  const [maxBid, setMaxBid] = useState('')
  const [snipes, setSnipes] = useState<Snipe[]>([])
  const [loading, setLoading] = useState(false)
  const [addLoading, setAddLoading] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')

  const fetchSnipes = useCallback(async () => {
    if (!userId.trim()) return
    setLoading(true)
    setError('')
    try {
      const res = await fetch(`${API}/snipes?user_id=${encodeURIComponent(userId.trim())}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      setSnipes(await res.json())
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [userId])

  const addSnipe = async (e: React.FormEvent) => {
    e.preventDefault()
    setAddLoading(true)
    setError('')
    setSuccess('')
    try {
      const res = await fetch(`${API}/snipes?user_id=${encodeURIComponent(userId.trim())}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ allegro_offer_url: url.trim(), max_bid_amount: parseFloat(maxBid) }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail ?? `HTTP ${res.status}`)
      setSuccess(`Snipe dodany! ID: ${data.id}`)
      setUrl('')
      setMaxBid('')
      fetchSnipes()
    } catch (e) {
      setError(String(e))
    } finally {
      setAddLoading(false)
    }
  }

  const cancelSnipe = async (snipeId: string) => {
    try {
      const res = await fetch(`${API}/snipes/${snipeId}/cancel?user_id=${encodeURIComponent(userId.trim())}`, {
        method: 'POST',
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail) }
      fetchSnipes()
    } catch (e) { setError(String(e)) }
  }

  const deleteSnipe = async (snipeId: string) => {
    try {
      const res = await fetch(`${API}/snipes/${snipeId}?user_id=${encodeURIComponent(userId.trim())}`, {
        method: 'DELETE',
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail) }
      fetchSnipes()
    } catch (e) { setError(String(e)) }
  }

  return (
    <div className="space-y-5">
      <div className="bg-gray-900 rounded-lg p-4 flex gap-3 items-end">
        <div className="flex-1">
          <label className="block text-xs text-gray-400 mb-1">Allegro User ID</label>
          <input
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm focus:outline-none focus:border-orange-500"
            placeholder="np. 12345678"
            value={userId}
            onChange={e => setUserId(e.target.value)}
          />
        </div>
        <button
          onClick={fetchSnipes}
          disabled={!userId.trim() || loading}
          className="bg-gray-700 hover:bg-gray-600 disabled:opacity-40 px-4 py-2 rounded text-sm transition-colors"
        >
          {loading ? '...' : 'Pobierz'}
        </button>
      </div>

      <form onSubmit={addSnipe} className="bg-gray-900 rounded-lg p-4 space-y-3">
        <h2 className="font-bold text-sm">Dodaj snipe</h2>
        <input
          className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm focus:outline-none focus:border-orange-500"
          placeholder="https://allegro.pl/oferta/nazwa-12345678"
          value={url}
          onChange={e => setUrl(e.target.value)}
          required
        />
        <div className="flex gap-3 items-center">
          <input
            className="w-36 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm focus:outline-none focus:border-orange-500"
            type="number" step="0.01" min="0.01" placeholder="Max kwota (PLN)"
            value={maxBid} onChange={e => setMaxBid(e.target.value)} required
          />
          <button
            type="submit"
            disabled={!userId.trim() || addLoading}
            className="bg-orange-600 hover:bg-orange-500 disabled:opacity-40 px-4 py-2 rounded font-bold transition-colors text-sm"
          >
            {addLoading ? '...' : 'Dodaj'}
          </button>
        </div>
        {error && <p className="text-red-400 text-sm">{error}</p>}
        {success && <p className="text-green-400 text-sm">{success}</p>}
      </form>

      {snipes.length > 0 && (
        <div className="space-y-2">
          <h2 className="text-xs text-gray-500 font-medium uppercase tracking-wider">
            Snipe&apos;y ({snipes.length})
          </h2>
          {snipes.map(s => (
            <div key={s.id} className="bg-gray-900 rounded-lg p-4 flex gap-3">
              {s.offer_image_url && (
                // eslint-disable-next-line @next/next/no-img-element
                <img src={s.offer_image_url} alt="" className="w-12 h-12 object-cover rounded shrink-0" />
              )}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <StatusBadge status={s.status} />
                  <span className="text-sm truncate">{s.offer_title ?? s.allegro_offer_url}</span>
                </div>
                <div className="text-xs text-gray-500 mt-1 space-x-3">
                  <span>Maks: <strong className="text-white">{s.max_bid_amount} zł</strong></span>
                  {s.current_price != null && <span>Cena: <strong className="text-white">{s.current_price} zł</strong></span>}
                </div>
                {s.result_message && <p className="text-xs text-gray-600 mt-0.5 truncate">{s.result_message}</p>}
              </div>
              <div className="flex gap-2 shrink-0 items-start">
                {(s.status === 'waiting' || s.status === 'active') && (
                  <button onClick={() => cancelSnipe(s.id)} className="text-xs bg-yellow-900 hover:bg-yellow-800 px-2 py-1 rounded">Anuluj</button>
                )}
                {['cancelled', 'error', 'won', 'lost'].includes(s.status) && (
                  <button onClick={() => deleteSnipe(s.id)} className="text-xs bg-red-950 hover:bg-red-900 px-2 py-1 rounded">Usuń</button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── Root ─────────────────────────────────────────────────────────────────────

export default function Home() {
  const [tab, setTab] = useState<Tab>('health')

  const tabs: { id: Tab; label: string }[] = [
    { id: 'health', label: 'Health' },
    { id: 'auth', label: 'Auth' },
    { id: 'snipes', label: 'Snipes' },
  ]

  return (
    <div className="max-w-2xl mx-auto p-4 sm:p-8">
      <header className="mb-6">
        <h1 className="text-xl font-bold">
          <span className="text-orange-500">Last</span>Bid
          <span className="text-gray-600 text-sm ml-2 font-normal">dev panel</span>
        </h1>
      </header>

      <div className="flex gap-1 mb-5 bg-gray-900 p-1 rounded-lg">
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`flex-1 py-2 px-3 rounded-md text-sm font-medium transition-colors ${
              tab === t.id ? 'bg-orange-600 text-white' : 'text-gray-500 hover:text-white'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'health' && <HealthTab />}
      {tab === 'auth' && <AuthTab />}
      {tab === 'snipes' && <SnipesTab />}
    </div>
  )
}
