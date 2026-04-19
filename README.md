# AstroLlama

Python client for llama.cpp with MCP support, local conversation persistence, and optional Microsoft Entra authentication using MSAL.

## Quick start

1. Copy `.env.example` to `.env`.
2. Start your llama.cpp server.
3. Run `./run_client.ps1` (PowerShell).
4. Open `http://127.0.0.1:8080`.

## Microsoft Entra authentication (MSAL)

The app supports bearer-token auth for all `/api/*` endpoints. The web UI signs users in with `msal-browser` and sends access tokens to FastAPI. The backend validates JWT signatures against Entra JWKS.

### 1) Create the API app registration (resource)

1. Go to Entra admin center > App registrations > New registration.
2. Name it something like LocalAI Chat API.
3. Keep supported account type to your org default.
4. Register the app.
5. Copy Application (client) ID. This is ENTRA_API_CLIENT_ID.

### 2) Expose an API scope on the API app

1. Open the API app registration.
2. Go to Expose an API.
3. Set Application ID URI (typically api://<api-client-id>) if prompted.
4. Add a scope:
	- Scope name: access_as_user
	- Who can consent: Admins and users (or your policy)
5. Save scope.
6. Build ENTRA_API_SCOPE as:
	- api://<ENTRA_API_CLIENT_ID>/access_as_user

### 3) Create the SPA app registration (frontend)

1. Go to App registrations > New registration.
2. Name it something like LocalAI Chat SPA.
3. Register the app.
4. Copy Application (client) ID. This is ENTRA_SPA_CLIENT_ID.
5. Open Authentication:
	- Add platform > Single-page application
	- Redirect URI: http://127.0.0.1:8080
6. This redirect URI value is ENTRA_REDIRECT_URI.

### 4) Grant SPA permission to call the API

1. Open SPA app registration > API permissions.
2. Add a permission > My APIs > select your API app.
3. Choose Delegated permissions and select access_as_user.
4. Grant admin consent if required by tenant policy.

### 5) Find tenant ID

1. Go to Entra ID > Overview.
2. Copy Tenant ID. This is ENTRA_TENANT_ID.

### 6) Configure environment

Set these values in `.env`:

- `ENTRA_AUTH_ENABLED=true`
- `ENTRA_TENANT_ID=<tenant-guid>`
- `ENTRA_SPA_CLIENT_ID=<spa-app-client-id>`
- `ENTRA_API_CLIENT_ID=<api-app-client-id>`
- `ENTRA_API_SCOPE=api://<api-app-client-id>/access_as_user`
- `ENTRA_REDIRECT_URI=http://127.0.0.1:8080`

If `ENTRA_AUTH_ENABLED=false`, auth is disabled and the app behaves as before.

### Variable mapping summary

- ENTRA_TENANT_ID: Entra ID > Overview > Tenant ID
- ENTRA_SPA_CLIENT_ID: SPA app registration > Overview > Application (client) ID
- ENTRA_API_CLIENT_ID: API app registration > Overview > Application (client) ID
- ENTRA_API_SCOPE: API app registration > Expose an API > created scope value
- ENTRA_REDIRECT_URI: SPA app registration > Authentication > SPA redirect URI

### About client secret (Secret ID and Secret Value)

- For this implementation, you do not need a client secret.
- The browser uses MSAL public-client flow and obtains user delegated access tokens.
- The backend only validates tokens; it does not exchange codes using a confidential client.
- Do not place Secret ID or Secret Value in `.env` for this setup.

## API behavior

- `GET /api/auth/config` is public and provides auth bootstrap config for the SPA.
- All other `/api/*` routes require a bearer token when auth is enabled.

## Notes

- This project remains local-first; host is `127.0.0.1` by default.
- Keep `.env` out of source control.
