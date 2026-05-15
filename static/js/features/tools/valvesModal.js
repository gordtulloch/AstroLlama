(function () {
  "use strict";

  function createToolValvesController(deps) {
    const apiFetch = deps && deps.apiFetch;

    const btnTools = document.getElementById("btn-tools");
    const toolsModal = document.getElementById("tools-modal");
    const btnToolsClose = document.getElementById("btn-tools-close");
    const btnToolsRefresh = document.getElementById("btn-tools-refresh");
    const btnToolsSave = document.getElementById("btn-tools-save");
    const toolsSelect = document.getElementById("tools-select");
    const toolsValvesForm = document.getElementById("tools-valves-form");

    const state = {
      loaded: false,
      loading: false,
      data: null,
      activeTool: "",
      bound: false,
    };

    function listAvailableValvesTools() {
      const valves = state.data && state.data.valves ? state.data.valves : {};
      return Object.keys(valves).sort(function (a, b) {
        return a.localeCompare(b);
      });
    }

    function inferInputType(prop) {
      const enumValues = Array.isArray(prop.enum) ? prop.enum : null;
      if (enumValues && enumValues.length > 0) return "enum";
      if (prop.type === "boolean") return "boolean";
      if (prop.type === "integer") return "integer";
      if (prop.type === "number") return "number";
      if (prop.input && prop.input.type === "password") return "password";
      return "string";
    }

    function renderValveInput(fieldName, propSchema, value) {
      const wrap = document.createElement("label");
      wrap.className = "tools-valve-item";

      const title = document.createElement("span");
      title.textContent = fieldName;
      wrap.appendChild(title);

      const inputType = inferInputType(propSchema || {});
      let input;

      if (inputType === "enum") {
        input = document.createElement("select");
        const enumValues = propSchema.enum || [];
        enumValues.forEach(function (optionVal) {
          const opt = document.createElement("option");
          opt.value = String(optionVal);
          opt.textContent = String(optionVal);
          if (optionVal === value) opt.selected = true;
          input.appendChild(opt);
        });
      } else if (inputType === "boolean") {
        wrap.classList.add("full");
        input = document.createElement("input");
        input.type = "checkbox";
        input.checked = Boolean(value);
      } else if (inputType === "integer" || inputType === "number") {
        input = document.createElement("input");
        input.type = "number";
        if (typeof propSchema.minimum === "number") input.min = String(propSchema.minimum);
        if (typeof propSchema.maximum === "number") input.max = String(propSchema.maximum);
        input.step = inputType === "integer" ? "1" : "any";
        input.value = value === null || value === undefined ? "" : String(value);
      } else {
        input = document.createElement("input");
        input.type = inputType === "password" ? "password" : "text";
        input.value = value === null || value === undefined ? "" : String(value);
      }

      input.dataset.fieldName = fieldName;
      input.dataset.valueType = inputType;
      wrap.appendChild(input);

      if (propSchema && propSchema.description) {
        const help = document.createElement("span");
        help.className = "help";
        help.textContent = propSchema.description;
        wrap.appendChild(help);
      }

      return wrap;
    }

    function renderToolValvesForm(toolName) {
      if (!toolsValvesForm || !btnToolsSave) return;

      const valves = state.data && state.data.valves ? state.data.valves : {};
      const tool = valves[toolName] || {};
      const schema = tool.schema || {};
      const values = tool.values || {};
      const props = schema.properties || {};

      toolsValvesForm.innerHTML = "";

      const fields = Object.keys(props);
      if (fields.length === 0) {
        const empty = document.createElement("div");
        empty.className = "tools-valve-item full";
        empty.textContent = "This tool does not expose a Valves model.";
        toolsValvesForm.appendChild(empty);
        btnToolsSave.disabled = true;
        return;
      }

      btnToolsSave.disabled = false;
      fields.forEach(function (fieldName) {
        const propSchema = props[fieldName] || {};
        const value = values[fieldName];
        toolsValvesForm.appendChild(renderValveInput(fieldName, propSchema, value));
      });
    }

    function renderToolsSelect() {
      if (!toolsSelect) return;

      const tools = listAvailableValvesTools();
      toolsSelect.innerHTML = "";

      if (tools.length === 0) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "No tools with valves available";
        toolsSelect.appendChild(opt);
        toolsSelect.disabled = true;
        renderToolValvesForm("");
        return;
      }

      toolsSelect.disabled = false;
      const activeTool = tools.indexOf(state.activeTool) >= 0 ? state.activeTool : tools[0];
      state.activeTool = activeTool;

      tools.forEach(function (toolName) {
        const opt = document.createElement("option");
        opt.value = toolName;
        opt.textContent = toolName;
        if (toolName === activeTool) opt.selected = true;
        toolsSelect.appendChild(opt);
      });

      renderToolValvesForm(activeTool);
    }

    async function loadToolValves(force) {
      const refresh = Boolean(force);
      if (state.loading) return;
      if (state.loaded && !refresh) return;
      if (typeof apiFetch !== "function") {
        throw new Error("Tool valves controller is missing apiFetch dependency");
      }

      state.loading = true;
      if (btnToolsRefresh) btnToolsRefresh.disabled = true;

      try {
        const r = await apiFetch("/api/tools/valves");
        if (!r.ok) {
          const msg = await r.text();
          throw new Error(msg || "Failed to load tools (" + r.status + ")");
        }

        const data = await r.json();
        state.data = data;
        state.loaded = true;
        renderToolsSelect();
      } finally {
        state.loading = false;
        if (btnToolsRefresh) btnToolsRefresh.disabled = false;
      }
    }

    function collectToolValveValues() {
      const values = {};
      const currentValues = (state.data && state.data.valves && state.data.valves[state.activeTool] && state.data.valves[state.activeTool].values)
        ? state.data.valves[state.activeTool].values
        : {};
      const inputs = toolsValvesForm ? toolsValvesForm.querySelectorAll("input, select, textarea") : [];

      inputs.forEach(function (input) {
        const fieldName = input.dataset.fieldName;
        const valueType = input.dataset.valueType || "string";
        if (!fieldName) return;

        if (valueType === "boolean") {
          values[fieldName] = Boolean(input.checked);
          return;
        }

        if (valueType === "integer") {
          const parsedInt = parseInt(input.value, 10);
          values[fieldName] = Number.isNaN(parsedInt) ? currentValues[fieldName] : parsedInt;
          return;
        }

        if (valueType === "number") {
          const parsedFloat = parseFloat(input.value);
          values[fieldName] = Number.isNaN(parsedFloat) ? currentValues[fieldName] : parsedFloat;
          return;
        }

        values[fieldName] = input.value;
      });

      return values;
    }

    async function saveToolValves() {
      if (!state.activeTool || !btnToolsSave) return;

      btnToolsSave.disabled = true;
      try {
        const values = collectToolValveValues();
        const r = await apiFetch("/api/tools/" + encodeURIComponent(state.activeTool) + "/valves", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ values: values }),
        });

        if (!r.ok) {
          const text = await r.text();
          throw new Error(text || "Failed to save valves (" + r.status + ")");
        }

        await loadToolValves(true);
        alert("Saved valves for " + state.activeTool + ".");
      } catch (err) {
        alert("Unable to save valves: " + (err && err.message ? err.message : err));
      } finally {
        btnToolsSave.disabled = false;
      }
    }

    async function openToolsModal() {
      if (!toolsModal) return;

      toolsModal.classList.remove("hidden");
      toolsModal.setAttribute("aria-hidden", "false");
      if (toolsValvesForm && !state.loaded) {
        toolsValvesForm.innerHTML = "<p style='color:var(--text-muted);padding:8px 0'>Loading…</p>";
      }

      try {
        await loadToolValves(false);
      } catch (err) {
        if (toolsValvesForm) {
          toolsValvesForm.innerHTML = "<p style='color:var(--error,#f55);padding:8px 0'>Error loading tool valves: " + (err && err.message ? err.message : String(err)) + "</p>";
        }
        return;
      }
    }

    function closeToolsModal() {
      if (!toolsModal) return;
      toolsModal.classList.add("hidden");
      toolsModal.setAttribute("aria-hidden", "true");
    }

    function isOpen() {
      return Boolean(toolsModal && !toolsModal.classList.contains("hidden"));
    }

    function bind() {
      if (state.bound) return;
      state.bound = true;

      console.log("[Tools] bind() called. btnTools=", btnTools, "toolsModal=", toolsModal);

      if (btnTools) {
        btnTools.addEventListener("click", function () {
          console.log("[Tools] btn-tools clicked");
          openToolsModal();
        });
      } else {
        console.warn("[Tools] btn-tools element not found — click handler NOT attached");
      }

      if (btnToolsClose) {
        btnToolsClose.addEventListener("click", closeToolsModal);
      }

      if (btnToolsRefresh) {
        btnToolsRefresh.addEventListener("click", function () {
          loadToolValves(true).catch(function (err) {
            alert("Unable to refresh tool valves: " + (err && err.message ? err.message : err));
          });
        });
      }

      if (btnToolsSave) {
        btnToolsSave.addEventListener("click", function () {
          saveToolValves();
        });
      }

      if (toolsSelect) {
        toolsSelect.addEventListener("change", function () {
          state.activeTool = toolsSelect.value;
          renderToolValvesForm(state.activeTool);
        });
      }

      if (toolsModal) {
        toolsModal.addEventListener("click", function (e) {
          if (e.target === toolsModal) closeToolsModal();
        });
      }
    }

    return {
      bind: bind,
      preload: loadToolValves,
      open: openToolsModal,
      close: closeToolsModal,
      isOpen: isOpen,
    };
  }

  window.createToolValvesController = createToolValvesController;
})();
