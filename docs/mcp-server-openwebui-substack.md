# Building an MCP Server That Can Run OpenWebUI Tools Unmodified

When people first hear Model Context Protocol (MCP), it can sound abstract. In practice, MCP is a very pragmatic interface: it gives language-model clients a standard way to discover tools, call them with structured arguments, and read resources from external systems.

In AstroLlama, we used MCP as the backbone for astronomy tools, then added a compatibility layer so OpenWebUI-style tool modules could be dropped in and executed without rewriting them.

This article explains:
- what MCP is in plain language
- how the AstroLlama MCP server is constructed
- how the OpenWebUI compatibility layer works internally
- why this design makes tool ecosystems portable

## MCP in One Page

At a high level, MCP is a protocol between two parties:
- MCP client: usually an LLM host or assistant runtime
- MCP server: a process exposing capabilities the model can use

An MCP server typically provides three things:

1. Tools
Callable functions with a name, description, and JSON input schema.

2. Resources
Read-only documents or data endpoints exposed via URIs.

3. Transport
How messages move between client and server. Common options include stdio and HTTP streamable transport.

Why this matters: once your capabilities are wrapped as MCP tools/resources, any compatible client can use them without a custom integration per host.

## The AstroLlama MCP Server Architecture

AstroLlama builds around a central MCP server object, then registers handlers for:
- list_resources
- read_resource
- list_tools
- call_tool

The design is intentionally modular: tools are not hardcoded into one giant registry. Instead, the server dynamically discovers tool files from a tools folder and projects them into MCP tool metadata.

### Dynamic Tool Discovery

The loader scans only top-level Python files in the tools directory.

That root-only policy is important because it enables a clean staging workflow:
- root folder: active production tools
- subfolders: ignored by runtime (for example, experimental or untested modules)

The server also computes a lightweight fingerprint from file names plus modified timestamps. If anything changes, it hot-reloads the registry.

This gives you development ergonomics without adding a separate watcher process.

### Duplicate Name Safety

MCP tool names live in one global namespace per server. If two modules export the same method name, collisions can happen.

AstroLlama handles this defensively: first tool wins, duplicates are logged and skipped.

That behavior is simple, deterministic, and avoids accidental runtime ambiguity.

## MCP Resources in AstroLlama

Beyond tools, the server exposes MCP resources under an astro URI scheme.

Examples include:
- help overview
- data source status
- tool valve schemas and current values

The valve resource is especially useful because it gives clients machine-readable visibility into tool configuration models (including defaults and constraints).

In other words, the server is not only callable, it is introspectable.

## Transport Modes: Stdio and Streamable HTTP

AstroLlama supports two runtime modes.

1. Stdio mode
Default MCP transport for local process integration.

2. HTTP mode
A streamable HTTP endpoint mounted at /mcp, with session management via StreamableHTTPSessionManager.

HTTP mode includes practical UX touches:
- root endpoint returns server metadata
- plain GET to /mcp (without SSE accept header) returns usage instructions instead of opaque errors
- explicit support expectations for POST, SSE GET, and session DELETE semantics

That makes debugging and external client onboarding significantly easier.

## OpenWebUI Compatibility: The Core Challenge

OpenWebUI tools are usually authored against OpenWebUI runtime assumptions:
- a Tools class containing callable methods
- optional valves/user_valves Pydantic models
- framework-specific imports from open_webui namespaces
- optional magic runtime arguments like event emitters, user, request, files, and metadata

A plain MCP server cannot execute these modules directly without adaptation.

AstroLlama solves this with an adapter layer that does four jobs:
- import shimming
- method introspection
- schema projection
- invocation mediation

## 1) Import Shimming

The compatibility layer installs lightweight Python module shims for OpenWebUI-only imports.

That means unmodified tool modules can import expected objects such as:
- open_webui.models users/chats/files
- open_webui routers for file/image handling
- utility helpers used by common OpenWebUI tools

The shim is intentionally minimal but functional:
- file uploads are persisted to AstroLlama downloads storage
- image uploads return stable file ids and API-style paths
- user/chat helpers return compatible placeholder objects

This is the key move that avoids editing imported tool code.

## 2) Method Introspection and Schema Generation

Once a tool module is imported and Tools is instantiated, the adapter inspects public methods.

For each method it builds:
- tool name
- human description from Sphinx-style docstrings
- JSON input schema from Python signatures and type annotations
- required vs optional arguments inferred from defaults

Type mapping covers common Python annotations like:
- str, int, float, bool
- list/tuple/dict
- Optional and Union patterns
- Literal enums

Result: OpenWebUI methods become valid MCP tool descriptors automatically, with no hand-written schema boilerplate.

## 3) Projecting Specs into MCP Tools

Generated specs are converted into MCP tool descriptors and returned by list_tools.

From the MCP client perspective, these tools are now first-class native tools:
- discoverable
- documented
- schema-validated

This is where protocol standardization pays off: heterogeneous tool authoring style on one side, uniform MCP representation on the other.

## 4) Invocation Mediation

Calling the tool is not just method forwarding.

The adapter mediates runtime differences by injecting OpenWebUI-style magic parameters when methods request them, including placeholders for:
- event emitter
- user context
- request
- messages/files/model/task/metadata/tool maps
- generic double-underscore ids and context args

Then it normalizes return values into MCP text content:
- plain strings pass through
- dict/list outputs become JSON strings
- tuple outputs prefer the textual message component
- response objects with byte bodies are decoded when possible

This coercion ensures consistent MCP responses even when upstream tools have varied return conventions.

## Why This Architecture Works

The design succeeds because responsibilities are sharply separated:

- Server runtime handles MCP lifecycle, transport, and endpoint behavior.
- Discovery layer handles loading/reloading and collision policy.
- Adapter handles compatibility and schema translation.
- Tool modules remain mostly untouched and focused on domain logic.

That separation makes the system both extensible and resilient.

If tomorrow you add 20 new OpenWebUI tools, the MCP surface updates automatically at runtime.

## Practical Example Flow

A request path looks like this:

1. Client asks list_tools.
2. Server loads or reloads tool registry if fingerprint changed.
3. Adapter introspects Tools methods and emits MCP schemas.
4. Client calls one tool with JSON args.
5. Server resolves spec by name and invokes via adapter.
6. Adapter injects magic args, awaits method, normalizes output.
7. Server returns MCP text content to client.

No bespoke glue code per individual tool.

## Intro-to-Advanced Takeaways for MCP Builders

If you are building your own MCP server, these are the patterns worth borrowing:

- Keep transport concerns separate from tool concerns.
- Generate schemas from code when possible to avoid drift.
- Decide duplicate-name policy explicitly; do not leave collisions implicit.
- Build compatibility layers around foreign ecosystems instead of rewriting every plugin.
- Expose configuration and status as MCP resources for client introspection.

## Closing

MCP gives you a common language for tool use across AI hosts. The AstroLlama server shows a practical extension of that idea: not only exposing native tools, but also translating an existing OpenWebUI tool ecosystem into MCP with minimal friction.

That is the bigger strategic advantage of MCP: interoperability as architecture, not an afterthought.
