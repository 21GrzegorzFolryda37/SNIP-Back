# LastBid — kontekst projektu (CLAUDE.md)

## Co to jest
**LastBid** — serwis do sniperowania aukcji Allegro.
Użytkownik loguje się przez Allegro OAuth2, dodaje snipe (URL aukcji + max kwota), a system automatycznie składa ofertę w ostatniej chwili przed końcem aukcji.

---

## Struktura monorepo

```
lastbid/                          ← root repo
├── backend/                      # FastAPI — Railway (Docker)
│   ├── app/
│   │   ├── main.py               # FastAPI app, startup/shutdown (NTP sync, scheduler)
│   │   ├── config.py             # Pydantic settings z .env (+ FRONTEND_URL dla CORS)
│   │   ├── api/
│   │   │   ├── router.py         # Montuje wszystkie routery
│   │   │   ├── auth.py           # OAuth2: /auth/login, /auth/callback, /auth/refresh
│   │   │   ├── snipes.py         # CRUD snipe'ów + POST /snipes/{id}/cancel
│   │   │   └── health.py         # GET /health
│   │   ├── models/
│   │   │   └── schemas.py        # Pydantic models + SnipeStatus enum
│   │   ├── services/
│   │   │   ├── supabase_client.py  # DB ops (users, snipes, snipe_logs)
│   │   │   ├── allegro_client.py   # HTTP klient Allegro API
│   │   │   └── token_manager.py    # Szyfrowanie tokenów (Fernet)
│   │   └── sniper/
│   │       ├── scheduler.py      # APScheduler — skanuje co minutę aktywne snipe'y
│   │       ├── engine.py         # SniperEngine — 3 bidy przed końcem aukcji
│   │       └── timing.py         # NTP sync + precise_sleep
│   ├── Dockerfile                # Deploy na Railway
│   ├── requirements.txt
│   └── .env.example
│
├── frontend/                     # Next.js 14 — Vercel
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx              # Główna strona (3 zakładki: Health, Auth, Snipes)
│   │   └── callback/
│   │       └── page.tsx          # Strona po OAuth2 callback — pokazuje user_id
│   ├── package.json
│   ├── next.config.js
│   ├── tailwind.config.js
│   ├── vercel.json
│   └── .env.local.example        # NEXT_PUBLIC_API_URL=...
│
├── .gitignore                    # Python + Node + secrets
├── README.md
└── CLAUDE.md                     # Ten plik
```

---

## Stack

### Backend
- **Python 3.12**, FastAPI, APScheduler, aiohttp
- **Supabase** (PostgreSQL) — baza danych
- **Hosting**: Railway (Docker)
- **CORS**: konfigurowany przez `FRONTEND_URL` env var

### Frontend
- **Next.js 14** (App Router), TypeScript, Tailwind CSS
- **Hosting**: Vercel
- **Komunikacja z backendem**: `NEXT_PUBLIC_API_URL` (env var)
- Aktualnie: prosty test UI (Health / Auth / Snipes tabs)

---

## Deployment

| Część | Platforma | Konfiguracja |
|---|---|---|
| Backend | Railway | Root dir: `backend/`, Dockerfile obecny |
| Frontend | Vercel | Root directory: `frontend/` |

### Zmienne środowiskowe

**Railway (backend):**
```
ALLEGRO_CLIENT_ID=
ALLEGRO_CLIENT_SECRET=
ALLEGRO_REDIRECT_URI=https://<backend>.railway.app/auth/callback
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
ENCRYPTION_KEY=          # Fernet key — KRYTYCZNE
FRONTEND_URL=https://<frontend>.vercel.app
ENVIRONMENT=production
SNIPE_OFFSET_MS=100
```

**Vercel (frontend):**
```
NEXT_PUBLIC_API_URL=https://<backend>.railway.app
```

---

## Baza danych (Supabase)

### Tabele
- `users` — allegro_user_id, allegro_login, encrypted_access_token, encrypted_refresh_token, token_expires_at, email, plan, stripe_customer_id, stripe_subscription_id
- `snipes` — user_id, allegro_offer_id, allegro_offer_url, offer_title, offer_image_url, offer_end_time, current_price, max_bid_amount, status, result_message, executed_at
- `snipe_logs` — snipe_id, action, details (TEXT)

### Status flow
`waiting` → `active` → `executing` → `won` / `lost` / `error` / `cancelled`

### RLS
RLS włączone. Backend używa `supabase_service_key` (omija RLS).

---

## Kluczowe zachowania

- **3 bidy** przed końcem aukcji: 300ms, 200ms, `SNIPE_OFFSET_MS` ms
- **NTP sync** przy starcie + co godzinę — precyzyjny timing
- **Auto-refresh tokenów** — jeśli access token wygasa w <5 min
- **PKCE state store** — w pamięci (`_pending_states`), OK dla single-instance

---

## Status deploymentu

| Serwis | URL | Status |
|---|---|---|
| Frontend | https://lastbid.pl | ✓ działa |
| Backend | https://snip-back-production.up.railway.app | ✓ działa |
| Supabase | kwvqkwiqsyflyafltnpj.supabase.co | ✓ działa |

### Zweryfikowane działanie
- ✓ `/health` — NTP sync, scheduler, Supabase połączenie
- ✓ `/auth/login` + `/auth/callback` — OAuth2 Allegro działa end-to-end
- ✓ HMAC-signed state (zastąpił in-memory `_pending_states`) — multi-instance safe
- ✓ Supabase `service_role` key ustawiony poprawnie w Railway

### Ważne szczegóły konfiguracji
- `SUPABASE_SERVICE_KEY` musi być kluczem `service_role` (nie `anon`) — inaczej RLS blokuje zapis
- `ALLEGRO_REDIRECT_URI` musi być ustawiona w Railway Variables (nie ma defaultu)
- Railway Variables mogą nie być propagowane przy pierwszym ustawieniu — delete + re-add rozwiązuje problem

## Znane braki / TODO

- [ ] Callback zwraca JSON bezpośrednio — docelowo powinien przekierowywać na frontend z user_id
- [ ] Brak autentykacji endpointów API (każdy z user_id może działać)
- [ ] `_check_win` w engine.py — logika wygranej do weryfikacji z real Allegro
- [ ] Brak rate limitingu i limitu snipe'ów na usera
- [ ] Integracja Stripe (plany basic/unlimited)
- [ ] Frontend produkcyjny (aktualny to test UI)
