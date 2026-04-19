/* =========================================================
   LocalAI Chat — app.js
   No external dependencies. Uses fetch + ReadableStream for
   POST-based SSE (EventSource only supports GET).
   ========================================================= */

(function () {
  "use strict";

  // Detect popup context EARLY to prevent MSAL popup blocking
  const isInPopup = window.opener !== null && window.opener !== undefined;
  const isChildWindow = window.self !== window.top;
  console.log("[Boot] Popup detection: isInPopup=", isInPopup, "isChildWindow=", isChildWindow);

  // ---- State -------------------------------------------------------
  const state = {
    conversationId: null,
    /** messages displayed in the current session (for saving) */
    messages: [],
    abortController: null,
    streaming: false,
    authEnabled: false,
    authReady: false,
    msalApp: null,
    account: null,
    accessToken: null,
    apiScope: null,
  };

  // ---- DOM refs ----------------------------------------------------
  const messagesEl   = document.getElementById("messages");
  const promptInput  = document.getElementById("prompt-input");
  const btnSend      = document.getElementById("btn-send");
  const btnCancel    = document.getElementById("btn-cancel");
  const btnNewChat   = document.getElementById("btn-new-chat");
  const btnSaveConv  = document.getElementById("btn-save-conv");
  const convList     = document.getElementById("conv-list");
  const statusLlama  = document.getElementById("status-llama");
  const statusMcp    = document.getElementById("status-mcp");
  const authBar      = document.getElementById("auth-bar");
  const authUser     = document.getElementById("auth-user");
  const btnLogin     = document.getElementById("btn-login");
  const btnLogout    = document.getElementById("btn-logout");

  // Settings inputs
  const sTemperature  = document.getElementById("s-temperature");
  const sTopP         = document.getElementById("s-top_p");
  const sMaxTokens    = document.getElementById("s-max_tokens");
  const sSystemPrompt = document.getElementById("s-system_prompt");

  // ---- Persistence helpers (localStorage) --------------------------
  function loadSettings() {
    try {
      const saved = JSON.parse(localStorage.getItem("chat_settings") || "{}");
      if (saved.temperature  !== undefined) sTemperature.value  = saved.temperature;
      if (saved.top_p        !== undefined) sTopP.value         = saved.top_p;
      if (saved.max_tokens   !== undefined) sMaxTokens.value    = saved.max_tokens;
      if (saved.system_prompt !== undefined) sSystemPrompt.value = saved.system_prompt;
    } catch (_) {}
  }

  function saveSettings() {
    localStorage.setItem("chat_settings", JSON.stringify({
      temperature:   parseFloat(sTemperature.value),
      top_p:         parseFloat(sTopP.value),
      max_tokens:    parseInt(sMaxTokens.value, 10),
      system_prompt: sSystemPrompt.value,
    }));
  }

  [sTemperature, sTopP, sMaxTokens, sSystemPrompt].forEach(el =>
    el.addEventListener("change", saveSettings)
  );

  // ---- Auth + API wrapper ----------------------------------------
  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const existing = document.querySelector(`script[src="${src}"]`);
      if (existing) {
        // If MSAL is already present, this source has effectively loaded.
        if (window.msal || existing.dataset.loaded === "true") {
          resolve();
          return;
        }

        // An existing tag that already failed will not emit events again; reject fast
        // so the caller can try fallback sources.
        if (existing.dataset.failed === "true") {
          reject(new Error(`Failed to load script: ${src}`));
          return;
        }

        existing.addEventListener("load", () => {
          existing.dataset.loaded = "true";
          resolve();
        }, { once: true });
        existing.addEventListener("error", () => {
          existing.dataset.failed = "true";
          reject(new Error(`Failed to load script: ${src}`));
        }, { once: true });
        return;
      }

      const script = document.createElement("script");
      script.src = src;
      script.async = true;
      script.addEventListener("load", () => {
        script.dataset.loaded = "true";
        resolve();
      }, { once: true });
      script.addEventListener("error", () => {
        script.dataset.failed = "true";
        reject(new Error(`Failed to load script: ${src}`));
      }, { once: true });
      document.head.appendChild(script);
    });
  }

  async function ensureMsalAvailable() {
    if (window.msal) {
      console.log("[Auth] MSAL already loaded in window");
      return;
    }
    console.log("[Auth] MSAL not found, attempting to load...");

    const sources = [
      // Try jsdelivr first (more reliable for user networks)
      "https://cdn.jsdelivr.net/npm/@azure/msal-browser@2.38.3/lib/msal-browser.min.js",
      // Fallback to Microsoft's official CDN
      "https://alcdn.msauth.net/browser/2.38.3/js/msal-browser.min.js",
    ];

    let lastError = null;
    for (const src of sources) {
      try {
        console.log(`[Auth] Attempting to load from: ${src}`);
        await loadScript(src);
        if (window.msal) {
          console.log("[Auth] ✅ MSAL loaded successfully from:", src);
          console.log("[Auth] window.msal.PublicClientApplication available:", typeof window.msal.PublicClientApplication);
          return;
        } else {
          console.warn(`[Auth] Script loaded but window.msal not found`);
        }
      } catch (err) {
        lastError = err;
        console.warn(`[Auth] ❌ Failed to load MSAL from ${src}:`, err.message);
      }
    }

    throw new Error(`Cannot load MSAL from any CDN. Last error: ${lastError?.message || "unknown"}. Check network access and browser console for CORS/CSP issues.`);
  }

  async function initAuth() {
    console.log("[Auth] initAuth() called");
    const r = await fetch("/api/auth/config");
    if (!r.ok) {
      console.error("[Auth] Failed to fetch auth config", r.status);
      throw new Error("Failed to load auth config");
    }

    const cfg = await r.json();
    console.log("[Auth] Auth config retrieved:", cfg);
    state.authEnabled = Boolean(cfg.enabled);
    state.apiScope = cfg.api_scope || null;
    const redirectUri = cfg.redirect_uri || window.location.origin;
    console.log("[Auth] Using redirect URI:", redirectUri);
    console.log("[Auth] Current window.location.href:", window.location.href);

    if (!state.authEnabled) {
      console.log("[Auth] Auth disabled via config");
      state.authReady = true;
      authBar.style.display = "none";
      return;
    }

    console.log("[Auth] Auth enabled, initializing MSAL...");
    const tenantId = cfg.tenant_id;
    const spaClientId = cfg.spa_client_id;
    console.log("[Auth] Environment values - Tenant:", tenantId, "Client:", spaClientId, "Scope:", state.apiScope);

    if (!tenantId || !spaClientId || !state.apiScope) {
      console.error("[Auth] Missing Entra config:", { tenantId, spaClientId, apiScope: state.apiScope });
      throw new Error("Entra auth is enabled but missing tenant/client/scope settings");
    }

    await ensureMsalAvailable();
    console.log("[Auth] MSAL ready, creating PublicClientApplication...");

    state.msalApp = new msal.PublicClientApplication({
      auth: {
        clientId: spaClientId,
        authority: `https://login.microsoftonline.com/${tenantId}`,
        redirectUri: redirectUri,
      },
      cache: {
        cacheLocation: "localStorage",
        storeAuthStateInCookie: false,
      },
    });
    console.log("[Auth] PublicClientApplication created with config:", {
      clientId: spaClientId,
      authority: `https://login.microsoftonline.com/${tenantId}`,
      redirectUri: redirectUri,
      currentUrl: window.location.href,
      urlSearchParams: window.location.search
    });

    console.log("[Auth] Attempting handleRedirectPromise...");
    try {
      const redirectResult = await state.msalApp.handleRedirectPromise();
      if (redirectResult) {
        console.log("[Auth] ✅ Redirect promise resolved with auth result:", {
          accessToken: redirectResult.accessToken ? "✓ present" : "✗ missing",
          expiresOn: redirectResult.expiresOn,
          account: redirectResult.account?.username || "unknown"
        });
        state.account = redirectResult.account;
        state.msalApp.setActiveAccount(state.account);
        state.accessToken = redirectResult.accessToken;
        updateSystemPromptWithUserName(); // ← Add user name to system prompt on redirect
        
        // If we're in a popup and got a redirect result, close the popup
        // MSAL should handle this, but we ensure it happens
        if (window.opener) {
          console.log("[Auth] Auth completed in popup context. Closing popup...");
          setTimeout(() => {
            window.close();
          }, 100);
          // Return early; don't continue with UI init in popup
          return;
        }
        
        setTimeout(() => { 
          console.log("[Auth] Auth flow completed, ready for requests"); 
        }, 500);
      } else {
        console.log("[Auth] handleRedirectPromise returned null (no redirect context, normal flow)");
      }
    } catch (error) {
      console.error("[Auth] ❌ handleRedirectPromise error:", error.message, error);
    }

    const accounts = state.msalApp.getAllAccounts();
    console.log("[Auth] Existing accounts after redirect check:", accounts.length > 0 ? accounts.map(a => a.username) : "none");
    if (accounts.length > 0 && !state.account) {
      state.account = accounts[0];
      state.msalApp.setActiveAccount(state.account);
      console.log("[Auth] Using first account:", state.account.username);
      updateSystemPromptWithUserName(); // ← Add user name to system prompt
      await ensureToken();
    }

    state.authReady = true;
    console.log("[Auth] Authentication initialization complete, authReady=true");
    console.log("[Auth] Auth initialization complete");
    updateAuthUi();
  }

  // Minimal MSAL init for popup redirect handling only
  // This function ONLY initializes MSAL, handles redirect, and closes the popup
  // It is used ONLY when we detect we are inside the auth redirect popup
  async function initMsalForRedirect() {
    console.log("[Auth/Redirect] Initializing minimal MSAL for redirect handling in popup...");
    
    // Fetch auth config first since we're in a popup and may not have it
    let cfg;
    try {
      const r = await fetch("/api/auth/config");
      if (!r.ok) throw new Error("Failed to fetch auth config");
      cfg = await r.json();
      console.log("[Auth/Redirect] Auth config retrieved:", cfg);
    } catch (err) {
      console.error("[Auth/Redirect] Failed to fetch auth config:", err);
      window.close();
      return;
    }
    
    const redirectUri = cfg.redirect_uri || window.location.origin;
    const tenantId = cfg.tenant_id;
    const spaClientId = cfg.spa_client_id;
    
    if (!tenantId || !spaClientId) {
      console.error("[Auth/Redirect] Missing Entra config in popup");
      window.close();
      return;
    }
    
    console.log("[Auth/Redirect] Creating minimal PublicClientApplication...");
    await ensureMsalAvailable();
    
    const msalApp = new msal.PublicClientApplication({
      auth: {
        clientId: spaClientId,
        authority: `https://login.microsoftonline.com/${tenantId}`,
        redirectUri: redirectUri,
      },
      cache: {
        cacheLocation: "localStorage",
        storeAuthStateInCookie: false,
      },
    });
    
    console.log("[Auth/Redirect] Calling handleRedirectPromise() - waiting for auth redirect...");
    console.log("[Auth/Redirect] Current URL:", window.location.href);
    console.log("[Auth/Redirect] Redirect URI:", redirectUri);
    
    try {
      // handleRedirectPromise() will wait for the redirect and resolve when auth code is received
      const redirectResult = await msalApp.handleRedirectPromise();
      
      if (redirectResult) {
        console.log("[Auth/Redirect] ✅ Redirect completed, token received!");
        console.log("[Auth/Redirect] Account:", redirectResult.account?.username);
        console.log("[Auth/Redirect] Token present:", !!redirectResult.accessToken);
        console.log("[Auth/Redirect] Scopes:", redirectResult.scopes);
      } else {
        console.log("[Auth/Redirect] handleRedirectPromise returned null (no active redirect)");
      }
    } catch (err) {
      console.error("[Auth/Redirect] handleRedirectPromise error:", err);
    }
    
    // Give localStorage time to sync, then close
    console.log("[Auth/Redirect] ✅ Waiting 500ms for state to sync, then closing popup...");
    await new Promise(resolve => setTimeout(resolve, 500));
    console.log("[Auth/Redirect] Closing popup now.");
    window.close();
  }

  async function ensureToken() {
    console.log("[Auth] ensureToken() called");
    if (!state.authEnabled) return null;
    if (!state.account) throw new Error("Sign-in required");

    const request = {
      scopes: [state.apiScope],
      account: state.account,
    };
    console.log("[Auth] Token request with scopes:", request.scopes);
    console.log("[Auth] Account:", state.account.username);

    try {
      console.log("[Auth] Attempting silent token acquisition...");
      const result = await state.msalApp.acquireTokenSilent(request);
      console.log("[Auth] ✅ Token acquired silently");
      console.log("[Auth] Token scope:", result.scopes);
      console.log("[Auth] Token expires:", result.expiresOn);
      state.accessToken = result.accessToken;
      console.log("[Auth] Returning token:", state.accessToken.substring(0, 50) + "...");
      return state.accessToken;
    } catch (err) {
      console.log("[Auth] ⚠ Silent token failed:", err.message);
      console.log("[Auth] Attempting popup token acquisition...");
      try {
        const result = await state.msalApp.acquireTokenPopup(request);
        console.log("[Auth] ✅ Token acquired via popup");
        console.log("[Auth] Token scope:", result.scopes);
        console.log("[Auth] Token expires:", result.expiresOn);
        state.accessToken = result.accessToken;
        state.account = result.account;
        state.msalApp.setActiveAccount(state.account);
        updateAuthUi();
        return state.accessToken;
      } catch (popupErr) {
        console.error("[Auth] ❌ Popup token acquisition failed:", popupErr.message);
        throw popupErr;
      }
    }
  }

  // Update system prompt with user name when they sign in
  function getDisplayName() {
    const claims = state.account?.idTokenClaims || {};

    // Helper: convert UPN-style string to "First Last"
    const formatUpn = (s) => s.split("@")[0].replace(/[._]/g, " ").replace(/\b\w/g, c => c.toUpperCase());

    // Prefer given_name + family_name from token claims (most reliable for proper names)
    if (claims.given_name || claims.family_name) {
      return [claims.given_name, claims.family_name].filter(Boolean).join(" ");
    }
    // Use display name if it looks like a real name (contains a space), otherwise format it
    if (claims.name) {
      return claims.name.includes(" ") ? claims.name : formatUpn(claims.name);
    }
    // Last resort: derive from UPN / username
    const upn = state.account?.username || "User";
    return formatUpn(upn);
  }

  function updateSystemPromptWithUserName() {
    if (!state.account) return;
    
    const displayName = getDisplayName();
    
    console.log("[Auth] Updating system prompt with user name:", displayName);
    
    // Get current system prompt
    let currentPrompt = sSystemPrompt.value.trim();
    
    // Remove any existing user name prefix from previous logins
    const userPrefixPattern = /^User name: .+?\n\n/;
    currentPrompt = currentPrompt.replace(userPrefixPattern, "");
    
    // Add new user name prefix
    const userPrefix = `User name: ${displayName}\n\n`;
    sSystemPrompt.value = userPrefix + currentPrompt;
    
    // Save to localStorage
    saveSettings();
  }
  
  // Clear user name from system prompt on logout
  function clearUserNameFromSystemPrompt() {
    console.log("[Auth] Clearing user name from system prompt");
    
    let currentPrompt = sSystemPrompt.value.trim();
    
    // Remove user name prefix
    const userPrefixPattern = /^User name: .+?\n\n/;
    currentPrompt = currentPrompt.replace(userPrefixPattern, "");
    
    sSystemPrompt.value = currentPrompt;
    
    // Save to localStorage
    saveSettings();
  }
  
  function updateAuthUi() {
    console.log("[Auth] updateAuthUi() called");
    if (!state.authEnabled) return;
    const signedIn = Boolean(state.account);
    console.log("[Auth] Signed in:", signedIn);
    authUser.textContent = signedIn
      ? getDisplayName()
      : "Not signed in";
    btnLogin.disabled = signedIn;
    btnLogout.disabled = !signedIn;
    btnSend.disabled = !signedIn || state.streaming;
    promptInput.disabled = !signedIn;
  }

  async function signIn() {
    console.log("[Auth] signIn() called", {
      authEnabled: state.authEnabled,
      msalReady: !!state.msalApp,
      currentUrl: window.location.href,
      isPopup: window.opener ? "YES (popup window)" : "NO (main window)"
    });
    
    // Guard: prevent nested popups. If we're currently IN a popup, don't open another one
    if (window.opener) {
      console.log("[Auth] ⚠️ signIn() called from within a popup. Ignoring to prevent nested popup.");
      console.log("[Auth] The redirect handler should be processing this authentication.");
      return;
    }
    
    if (!state.authEnabled || !state.msalApp) {
      console.error("[Auth] Cannot sign in: auth not ready");
      alert("Authentication not ready");
      return;
    }
    
    const loginRequest = { scopes: [state.apiScope] };
    console.log("[Auth] Initiating login popup with request:", loginRequest);
    
    try {
      console.log("[Auth] Calling msalApp.loginPopup()...");
      const result = await state.msalApp.loginPopup(loginRequest);
      console.log("[Auth] ✅ loginPopup completed successfully");
      console.log("[Auth] Result account:", result.account?.username);
      console.log("[Auth] Result accessToken:", result.accessToken ? "✓ present" : "✗ missing");
      state.account = result.account;
      state.msalApp.setActiveAccount(state.account);
      console.log("[Auth] Account set in state, calling ensureToken...");
      updateSystemPromptWithUserName(); // ← Add user name to system prompt
      await ensureToken();
      updateAuthUi();
      console.log("[Auth] Sign in flow complete");
      await pollStatus();
      await loadConvList();
    } catch (error) {
      console.error("[Auth] ❌ Sign in failed:", error.message);
      console.error("[Auth] Error details:", error);
      alert("Sign in failed: " + error.message);
    }
  }

  async function signOut() {
    console.log("[Auth] signOut() called");
    if (!state.authEnabled || !state.msalApp || !state.account) return;
    console.log("[Auth] Logging out...");
    await state.msalApp.logoutPopup({ account: state.account });
    console.log("[Auth] Logout successful");
    state.account = null;
    state.accessToken = null;
    state.conversationId = null;
    state.messages = [];
    clearUserNameFromSystemPrompt(); // ← Clear user name from system prompt
    messagesEl.innerHTML = "";
    convList.innerHTML = "";
    updateAuthUi();
    setChipStatus(statusLlama, false, "llama");
    setChipStatus(statusMcp, false, "mcp");
  }

  async function apiFetch(url, options = {}) {
    const opts = { ...options };
    opts.headers = { ...(opts.headers || {}) };

    if (state.authEnabled) {
      if (!state.authReady) throw new Error("Authentication is not ready");
      const token = await ensureToken();
      opts.headers.Authorization = `Bearer ${token}`;
      console.log(`[API] Sending ${options.method || 'GET'} ${url} with auth header`);
      console.log(`[API] Bearer token (first 50 chars):`, token.substring(0, 50) + "...");
    }

    const response = await fetch(url, opts);
    
    console.log(`[API] ${options.method || 'GET'} ${url} → ${response.status}`);
    if (!response.ok && state.authEnabled) {
      console.warn(`[API] Response not OK, status: ${response.status}`);
      if (response.status === 401) {
        console.log("[API] Got 401, clearing token and updating UI");
        state.accessToken = null;
        updateAuthUi();
      }
    }

    return response;
  }

  // ---- Status polling ----------------------------------------------
  async function pollStatus() {
    try {
      const r = await apiFetch("/api/status");
      if (!r.ok) return;
      const data = await r.json();
      setChipStatus(statusLlama, data.llama_server === "ok", "llama");
      setChipStatus(statusMcp,   data.mcp_server   === "ok", "mcp");
    } catch (_) {
      setChipStatus(statusLlama, false, "llama");
      setChipStatus(statusMcp,   false, "mcp");
    }
  }

  function setChipStatus(el, ok, label) {
    el.className = "status-chip " + (ok ? "ok" : "error");
    el.textContent = label + " \u25CF";
  }

  // ---- Conversation list -------------------------------------------
  async function loadConvList() {
    try {
      const r = await apiFetch("/api/conversations");
      if (!r.ok) return;
      const list = await r.json();
      convList.innerHTML = "";
      list.forEach(conv => {
        const li = document.createElement("li");
        if (conv.id === state.conversationId) li.classList.add("active");

        const nameSpan = document.createElement("span");
        nameSpan.className = "conv-name";
        nameSpan.textContent = conv.name || "Untitled";

        const delBtn = document.createElement("button");
        delBtn.className = "del-btn";
        delBtn.textContent = "\u2715";
        delBtn.title = "Delete";
        delBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          deleteConv(conv.id);
        });

        li.appendChild(nameSpan);
        li.appendChild(delBtn);
        li.addEventListener("click", () => loadConv(conv.id));
        convList.appendChild(li);
      });
    } catch (_) {}
  }

  async function loadConv(id) {
    try {
      const r = await apiFetch(`/api/conversations/${id}`);
      if (!r.ok) return;
      const conv = await r.json();
      state.conversationId = id;
      state.messages = conv.messages || [];
      messagesEl.innerHTML = "";
      for (const msg of state.messages) {
        if (msg.role === "system") continue;
        await appendMessage(msg.role === "user" ? "user" : "assistant", msg.content || "");
      }
      loadConvList();
    } catch (_) {}
  }

  async function deleteConv(id) {
    if (!confirm("Delete this conversation?")) return;
    await apiFetch(`/api/conversations/${id}`, { method: "DELETE" });
    if (state.conversationId === id) newChat();
    loadConvList();
  }

  // ---- New chat ----------------------------------------------------
  function newChat() {
    state.conversationId = null;
    state.messages = [];
    messagesEl.innerHTML = "";
    loadConvList();
  }

  // ---- Save conversation -------------------------------------------
  async function saveConv() {
    if (!state.conversationId || state.messages.length === 0) {
      alert("Nothing to save yet.");
      return;
    }
    const name = prompt("Conversation name:", "My conversation") || "Untitled";
    const body = {
      conversation_id: state.conversationId,
      name,
      messages: state.messages,
      settings: {
        temperature:   parseFloat(sTemperature.value),
        top_p:         parseFloat(sTopP.value),
        max_tokens:    parseInt(sMaxTokens.value, 10),
        system_prompt: sSystemPrompt.value,
      },
    };
    const r = await apiFetch("/api/conversations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (r.ok) loadConvList();
    else alert("Failed to save conversation.");
  }

  // ---- Message rendering -------------------------------------------
  async function appendMessage(role, text) {
    const div = document.createElement("div");
    div.className = "msg " + role;
    const label = document.createElement("div");
    label.className = "msg-label";
    label.textContent = role === "user" ? "You" : "Assistant";
    div.appendChild(label);
    const content = document.createElement("div");
    content.className = "msg-content";
    if (role === "assistant") {
      content.innerHTML = await renderHighlighted(text);
    } else {
      const p = document.createElement("p");
      p.textContent = text;
      content.appendChild(p);
    }
    div.appendChild(content);
    messagesEl.appendChild(div);
    scrollToBottom();
    return div;
  }

  function appendThinking() {
    const div = document.createElement("div");
    div.className = "msg assistant";
    const label = document.createElement("div");
    label.className = "msg-label";
    label.textContent = "Assistant";
    div.appendChild(label);
    const thinking = document.createElement("div");
    thinking.className = "thinking";
    thinking.innerHTML = "<span></span><span></span><span></span>";
    div.appendChild(thinking);
    messagesEl.appendChild(div);
    scrollToBottom();
    return div;
  }

  function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  // ---- Tool call details block (inside an assistant message) -------
  function ensureToolDetails(msgEl) {
    let details = msgEl.querySelector(".tool-details");
    if (!details) {
      details = document.createElement("details");
      details.className = "tool-details";
      const summary = document.createElement("summary");
      summary.textContent = "Tool calls";
      details.appendChild(summary);
      msgEl.appendChild(details);
    }
    return details;
  }

  function addToolStart(msgEl, name, args) {
    const details = ensureToolDetails(msgEl);
    const entry = document.createElement("div");
    entry.className = "tool-entry";
    entry.dataset.toolName = name;
    entry.innerHTML = `<div class="tool-name">\u2699\ufe0f ${escHtml(name)}</div>` +
      `<div class="tool-args">Args: ${escHtml(JSON.stringify(args, null, 2))}</div>`;
    details.appendChild(entry);
    details.open = false;
  }

  function addToolResult(msgEl, name, result) {
    const details = ensureToolDetails(msgEl);
    const entry = [...details.querySelectorAll(".tool-entry")]
      .reverse()
      .find(e => e.dataset.toolName === name);
    if (entry) {
      entry.dataset.done = "true";
    }
  }

  function addToolDownload(msgEl, name, url, size) {
    const details = ensureToolDetails(msgEl);
    const entry = [...details.querySelectorAll(".tool-entry")]
      .reverse()
      .find(e => e.dataset.toolName === name);
    const sizeStr = size > 1048576
      ? (size / 1048576).toFixed(1) + " MB"
      : (size / 1024).toFixed(1) + " KB";
    const linkHtml = `<div class="tool-download">💾 Result too large for inline display (${escHtml(sizeStr)}) — ` +
      `<a href="${escHtml(url)}" download>Download file</a></div>`;
    if (entry) {
      entry.dataset.done = "true";
      entry.insertAdjacentHTML("beforeend", linkHtml);
    } else {
      const div = document.createElement("div");
      div.className = "tool-entry";
      div.dataset.toolName = name;
      div.dataset.done = "true";
      div.innerHTML = `<div class="tool-name">⚙️ ${escHtml(name)}</div>` + linkHtml;
      details.appendChild(div);
    }
  }

  function addToolError(msgEl, name, error) {
    const details = ensureToolDetails(msgEl);
    const errDiv = document.createElement("div");
    errDiv.className = "tool-entry";
    errDiv.innerHTML = `<div class="tool-name">\u274c ${escHtml(name)}</div>` +
      `<div class="tool-error">${escHtml(error)}</div>`;
    details.appendChild(errDiv);
  }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // ---- Pygments syntax highlighting via backend API ---------------

  /** Fetch the Pygments CSS once and inject it into <head>. */
  async function initHighlightStyles() {
    try {
      const r = await apiFetch("/api/highlight/styles");
      if (!r.ok) return;
      const css = await r.text();
      const style = document.createElement("style");
      style.id = "pygments-styles";
      style.textContent = css;
      document.head.appendChild(style);
    } catch (_) {}
  }

  /**
   * Send *text* to the backend highlight endpoint and return an HTML string.
   * Falls back to client-side <pre><code> rendering on error.
   */
  async function renderHighlighted(text) {
    try {
      const r = await apiFetch("/api/highlight", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      });
      if (r.ok) {
        const data = await r.json();
        return data.html;
      }
      console.warn("[Highlight] API returned", r.status, "— using client-side fallback");
    } catch (err) {
      console.warn("[Highlight] API call failed:", err);
    }
    // Client-side fallback: use marked.js for full markdown rendering
    return marked.parse(text);
  }

  // ---- Send message ------------------------------------------------
  async function sendMessage() {
    const text = promptInput.value.trim();
    if (!text || state.streaming) return;

    // Set streaming flag immediately to prevent double-send
    state.streaming = true;
    btnSend.disabled = true;
    btnCancel.disabled = false;

    promptInput.value = "";
    promptInput.style.height = "";

    // Display user message immediately
    appendMessage("user", text);
    state.messages.push({ role: "user", content: text });

    // Show thinking indicator
    const aiBubble = appendThinking();

    state.abortController = new AbortController();

    const requestBody = {
      conversation_id: state.conversationId,
      message: text,
      settings: {
        temperature:   parseFloat(sTemperature.value),
        top_p:         parseFloat(sTopP.value),
        max_tokens:    parseInt(sMaxTokens.value, 10),
        system_prompt: sSystemPrompt.value || null,
      },
    };

    let contentEl = null;
    let assistantText = "";

    try {
      const response = await apiFetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
        signal: state.abortController.signal,
      });

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop(); // last partial line

        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          const dataStr = line.slice(5).trim();
          if (!dataStr) continue;

          let event;
          try { event = JSON.parse(dataStr); } catch (_) { continue; }

          switch (event.type) {
            case "conversation_id":
              state.conversationId = event.conversation_id;
              loadConvList();
              break;

            case "token":
              if (!contentEl) {
                // Replace thinking indicator with real content
                const thinking = aiBubble.querySelector(".thinking");
                if (thinking) thinking.remove();
                contentEl = document.createElement("div");
                contentEl.className = "msg-content";
                aiBubble.appendChild(contentEl);
              }
              assistantText += event.text;
              contentEl.textContent = assistantText;
              scrollToBottom();
              break;

            case "tool_start":
              addToolStart(aiBubble, event.name, event.args);
              scrollToBottom();
              break;

            case "tool_result":
              addToolResult(aiBubble, event.name, event.result);
              scrollToBottom();
              break;

            case "tool_download":
              addToolDownload(aiBubble, event.name, event.url, event.size);
              scrollToBottom();
              break;

            case "tool_error":
              addToolError(aiBubble, event.name, event.error);
              scrollToBottom();
              break;

            case "error": {
              const errDiv = document.createElement("div");
              errDiv.className = "msg error";
              const lbl = document.createElement("div");
              lbl.className = "msg-label";
              lbl.textContent = "Error";
              errDiv.appendChild(lbl);
              const eContent = document.createElement("div");
              eContent.textContent = event.message;
              errDiv.appendChild(eContent);
              // Replace thinking bubble with error
              aiBubble.replaceWith(errDiv);
              scrollToBottom();
              break;
            }

            case "done":
              if (assistantText) {
                state.messages.push({ role: "assistant", content: assistantText });
                if (contentEl) {
                  contentEl.innerHTML = await renderHighlighted(assistantText);
                  scrollToBottom();
                }
              }
              break;
          }
        }
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        // Show error in the bubble
        const thinking = aiBubble.querySelector(".thinking");
        if (thinking) thinking.remove();
        const errEl = document.createElement("div");
        errEl.className = "tool-error";
        errEl.textContent = "Connection error: " + err.message;
        aiBubble.appendChild(errEl);
        scrollToBottom();
      } else {
        // Cancelled — clean up thinking if still shown
        const thinking = aiBubble.querySelector(".thinking");
        if (thinking) {
          aiBubble.querySelector(".msg-label").textContent = "Assistant (cancelled)";
          thinking.remove();
          const cancelNote = document.createElement("div");
          cancelNote.className = "msg-content";
          cancelNote.style.opacity = "0.5";
          cancelNote.textContent = assistantText || "(cancelled)";
          aiBubble.appendChild(cancelNote);
        }
      }
    } finally {
      state.streaming = false;
      state.abortController = null;
      btnSend.disabled = false;
      btnCancel.disabled = true;
    }
  }

  // ---- Event listeners --------------------------------------------
  btnSend.addEventListener("click", sendMessage);

  promptInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  btnCancel.addEventListener("click", () => {
    if (state.abortController) state.abortController.abort();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && state.abortController) {
      state.abortController.abort();
    }
  });

  btnNewChat.addEventListener("click", newChat);
  btnSaveConv.addEventListener("click", saveConv);
  btnLogin.addEventListener("click", () => {
    console.log("[UI] Sign in button clicked");
    signIn().catch(err => {
      console.error("[UI] Sign in error:", err);
      alert(err.message || "Sign in failed");
    });
  });
  btnLogout.addEventListener("click", () => {
    console.log("[UI] Sign out button clicked");
    signOut().catch(err => {
      console.error("[UI] Sign out error:", err);
      alert(err.message || "Sign out failed");
    });
  });

  // ---- Init -------------------------------------------------------
  async function init() {
    console.log("[Init] Starting app initialization...");
    console.log("[Init] Environment info:", {
      isPopup: isInPopup,
      isChildWindow: isChildWindow,
      currentUrl: window.location.href,
      hasRedirectCode: window.location.search.includes("code=") ? "YES" : "NO",
      hasState: window.location.search.includes("state=") ? "YES" : "NO",
      hasError: window.location.search.includes("error=") ? "YES" : "NO",
    });
    
    // CRITICAL: If we're in a popup, ONLY handle redirect and close.
    // Do NOT initialize MSAL, UI, or any other app functionality.
    if (isInPopup) {
      console.log("[Init] 🔴 POPUP DETECTED: Skipping app initialization, only handling auth redirect...");
      try {
        loadSettings(); // Load settings first so we have MSAL config
        
        // Initialize minimal MSAL just for redirect handling
        await initMsalForRedirect();
        
        console.log("[Init] Popup should have closed after redirect handling.");
      } catch (err) {
        console.error("[Init] Error in popup redirect handling:", err);
        window.close();
      }
      return;
    }
    
    // Main window initialization
    loadSettings();
    await initHighlightStyles();

    try {
      await initAuth();
    } catch (err) {
      console.error("[Init] Auth initialization failed:", err);
      authUser.textContent = "Authentication setup failed";
      btnLogin.disabled = true;
      btnLogout.disabled = true;
      btnSend.disabled = true;
      promptInput.disabled = true;
      alert(err.message || "Authentication setup failed");
      return;
    }

    console.log("[Init] Auth initialized successfully");
    updateAuthUi();
    await loadConvList();
    await pollStatus();
    setInterval(pollStatus, 10_000);
  }

  console.log("[Boot] Script loaded, calling init()...");
  init();
})();
