# MCP in Practice: How AstroLlama Runs OpenWebUI Tools Without Rewrites

If you have heard about Model Context Protocol (MCP) but have not built with it yet, here is the practical version:

MCP is a standard interface that lets AI clients discover tools, read resources, and call capabilities from external servers in a predictable way.

In AstroLlama, we used MCP as the core server interface for astronomy tooling. Then we added a compatibility layer so OpenWebUI-style tools can be dropped in and used as MCP tools without editing their source.

This post is the short version of how that architecture works.

## MCP Basics in 60 Seconds

An MCP server usually exposes three things:

1. Tools
Named functions with descriptions and JSON input schemas.

2. Resources
Read-only URIs for status, docs, or structured metadata.

3. Transport
How clients connect, typically stdio or HTTP streamable transport.

The payoff is interoperability: your tool server can serve multiple AI clients without custom one-off integrations.

## How AstroLlama’s MCP Server Is Structured

AstroLlama’s server registers the standard MCP handlers:
- list_resources
- read_resource
- list_tools
- call_tool

From there, the important design decision is dynamic tool loading.

### Dynamic Discovery

Instead of hardcoding every tool, AstroLlama scans the top-level tools directory for Python modules and loads them at runtime.

Two intentional guardrails:
- Root-only scan: subfolders are ignored, so you can keep experimental modules out of production load.
- Fingerprint-based reload: file name + modified timestamp are used to detect changes and rebuild the tool registry automatically.

This keeps local iteration fast while preserving predictable runtime behavior.

### Name Collision Policy

MCP tool names are global within a server. If two modules export the same tool name, ambiguity is dangerous.

AstroLlama uses a deterministic rule: first loaded tool wins, duplicates are skipped with warnings.

That sounds simple, but it prevents subtle bugs when teams add new modules quickly.

## Transport: Stdio and Streamable HTTP

AstroLlama supports:
- stdio mode for local process clients
- HTTP mode via a streamable MCP endpoint at /mcp

HTTP mode includes practical usability details:
- Root endpoint publishes server metadata
- Plain GET to /mcp returns usage guidance if SSE headers are missing
- POST/GET/DELETE expectations are clearly documented in the endpoint response

This significantly reduces friction when integrating external clients.

## The OpenWebUI Compatibility Problem

OpenWebUI tools are often written with OpenWebUI runtime assumptions:
- Tools class with public callable methods
- optional valves or user_valves models
- imports from open_webui.* modules
- optional magic runtime parameters (event emitter, user context, request, messages, files, metadata, etc.)

A plain MCP runtime does not provide these out of the box.

Rather than rewriting each imported tool, AstroLlama uses an adapter that makes OpenWebUI modules feel at home.

## How the Compatibility Layer Works

The adapter does four jobs.

### 1) Runtime Shims for OpenWebUI Imports

A lightweight shim installs expected open_webui modules at runtime so unmodified tools can import what they need.

It also provides practical behavior where necessary, like:
- storing uploaded files in AstroLlama’s downloads folder
- returning stable file identifiers/paths
- providing placeholder user/chat helper objects

The goal is compatibility, not full OpenWebUI reimplementation.

### 2) Tool Introspection

Each loaded module’s Tools instance is inspected for public methods.

For every method, the adapter extracts:
- tool name
- summary/description from docstrings
- argument schema from function signatures and type annotations
- required fields inferred from missing defaults

That means tool definitions stay close to code, and schema drift is minimized.

### 3) MCP Projection

The introspected method specs are projected into MCP tool descriptors and returned by list_tools.

From the client perspective, these tools now look native to MCP:
- discoverable
- documented
- schema-aware

This is the bridge from OpenWebUI authoring style to MCP protocol format.

### 4) Invocation Mediation

When a client calls a tool, the adapter:
- maps incoming arguments
- injects requested OpenWebUI magic args when absent
- awaits async methods when needed
- normalizes outputs into consistent MCP text content

Normalization handles mixed return styles:
- strings pass through
- dict/list become JSON strings
- tuple payloads prefer textual message components
- response-like byte bodies are decoded where possible

This keeps downstream behavior stable even when tool modules return data in different conventions.

## Why This Pattern Is Useful

This architecture has one major advantage: portability.

You can ingest a growing set of OpenWebUI-compatible tools while presenting a consistent MCP interface to AI clients.

In practice, that gives you:
- faster tool onboarding
- less custom glue code
- cleaner separation of concerns
- lower risk when adding third-party tool modules

## End-to-End Flow

A typical call sequence looks like this:

1. MCP client requests list_tools.
2. Server refreshes registry if tool fingerprint changed.
3. Adapter introspects methods and serves MCP schemas.
4. Client calls a tool by name with JSON arguments.
5. Adapter injects runtime placeholders, executes method, normalizes output.
6. MCP response returns as standard text content.

No per-tool hand wiring required.

## If You’re Building Your Own MCP Server

Three practical recommendations:

1. Keep protocol/runtime concerns separate from tool implementation concerns.
2. Generate schemas from signatures when possible to reduce maintenance burden.
3. Build compatibility adapters around existing ecosystems instead of rewriting every plugin.

MCP becomes most powerful when it is not just a protocol endpoint, but a stable interoperability boundary.

## Closing

AstroLlama’s MCP server demonstrates a pragmatic approach: standard MCP on the outside, flexible compatibility on the inside.

That combination lets you move quickly with existing OpenWebUI tooling while keeping a clean, client-agnostic interface for the future.