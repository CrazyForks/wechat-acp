import type { Server as HttpServer } from "node:http";
import type { AddressInfo } from "node:net";
import { randomBytes, randomUUID } from "node:crypto";

import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { NextFunction, Request, Response } from "express";
import { z } from "zod";

import { ArtifactStore } from "./store.js";
import type { AgentFile, AgentResourceLink } from "./types.js";

const MCP_PATH = "/mcp";

export interface ArtifactMcpLease {
  mcpServer: {
    name: string;
    url: string;
    headers: Array<{ name: string; value: string }>;
    type: "http";
  };
  close(): Promise<void>;
}

export interface ArtifactMcpServer {
  createLease(): ArtifactMcpLease;
  resolveResourceLink(link: AgentResourceLink): Promise<AgentFile | null>;
  close(): Promise<void>;
}

interface ArtifactLeaseState {
  sessionIds: Set<string>;
  inFlightInitializations: Set<Promise<void>>;
  closing: boolean;
}

export async function startArtifactMcpServer(options: {
  rootDir: string;
  log: (message: string) => void;
}): Promise<ArtifactMcpServer> {
  const store = await ArtifactStore.create({ rootDir: options.rootDir });
  const sessions = new Map<
    string,
    {
      mcp: McpServer;
      transport: StreamableHTTPServerTransport;
      leaseToken: string;
    }
  >();
  const leases = new Map<string, ArtifactLeaseState>();

  const app = createMcpExpressApp({ host: "127.0.0.1" });
  app.use(
    MCP_PATH,
    (req: Request, res: Response, next: NextFunction) => {
      if (req.headers.origin) {
        res.status(403).json({ error: "Origin requests are not allowed" });
        return;
      }
      const token = bearerToken(req.headers.authorization);
      if (!token || !leases.has(token)) {
        res.status(401).json({ error: "Unauthorized" });
        return;
      }
      res.locals.artifactLeaseToken = token;
      next();
    },
  );

  app.all(MCP_PATH, async (req: Request, res: Response) => {
    const leaseToken = res.locals.artifactLeaseToken as string;
    const lease = leases.get(leaseToken);
    if (!lease || lease.closing) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }

    let finishInitialization: (() => void) | undefined;
    let initializationFinished: Promise<void> | undefined;
    try {
      const sessionIdHeader = req.headers["mcp-session-id"];
      const sessionId =
        typeof sessionIdHeader === "string" ? sessionIdHeader : undefined;
      let session = sessionId ? sessions.get(sessionId) : undefined;
      let isNewSession = false;
      if (session && session.leaseToken !== leaseToken) {
        session = undefined;
      }

      if (!session) {
        if (
          req.method !== "POST" ||
          typeof req.body !== "object" ||
          req.body === null ||
          req.body.method !== "initialize"
        ) {
          res.status(sessionId ? 404 : 400).json({
            error: sessionId ? "Unknown MCP session" : "Missing MCP initialization",
          });
          return;
        }
        initializationFinished = new Promise<void>((resolve) => {
          finishInitialization = resolve;
        });
        lease.inFlightInitializations.add(initializationFinished);
        const created = await createMcpSession(store, options.log);
        session = { ...created, leaseToken };
        isNewSession = true;
      }

      try {
        await session.transport.handleRequest(req, res, req.body);
        if (isNewSession) {
          const newSessionId = session.transport.sessionId;
          if (!newSessionId) {
            throw new Error("MCP transport did not create a session ID");
          }
          sessions.set(newSessionId, session);
          lease.sessionIds.add(newSessionId);
        } else if (req.method === "DELETE" && sessionId) {
          await session.mcp.close();
          sessions.delete(sessionId);
          lease.sessionIds.delete(sessionId);
        }
      } catch (err) {
        options.log(`[artifact-mcp] request failed: ${String(err)}`);
        if (isNewSession) {
          try {
            await session.mcp.close();
          } catch (cleanupErr) {
            const cleanupSessionId =
              session.transport.sessionId ?? `failed-${randomUUID()}`;
            sessions.set(cleanupSessionId, session);
            lease.sessionIds.add(cleanupSessionId);
            options.log(
              `[artifact-mcp] deferred failed session cleanup: ${String(cleanupErr)}`,
            );
          }
        }
        if (!res.headersSent) {
          res.status(500).json({ error: "MCP request failed" });
        }
      }
    } finally {
      if (initializationFinished) {
        lease.inFlightInitializations.delete(initializationFinished);
        finishInitialization!();
      }
    }
  });

  const httpServer = await listen(app);
  const address = httpServer.address() as AddressInfo;
  const url = `http://127.0.0.1:${address.port}${MCP_PATH}`;
  options.log(`[artifact-mcp] listening at ${url}`);
  let serverClosing = false;
  let serverClose: Promise<void> | undefined;

  return {
    createLease: () => {
      if (serverClosing) {
        throw new Error("Artifact MCP server is closing");
      }
      const token = randomBytes(32).toString("base64url");
      const lease: ArtifactLeaseState = {
        sessionIds: new Set(),
        inFlightInitializations: new Set(),
        closing: false,
      };
      leases.set(token, lease);
      let closed = false;
      let closing: Promise<void> | undefined;
      return {
        mcpServer: {
          name: "wechat-acp-artifacts",
          url,
          headers: [{ name: "Authorization", value: `Bearer ${token}` }],
          type: "http",
        },
        close: async () => {
          if (closed) return;
          if (closing) return closing;
          const attempt = closeLease(
            token,
            lease,
            leases,
            sessions,
          );
          closing = attempt;
          try {
            await attempt;
            closed = true;
          } finally {
            if (closing === attempt) {
              closing = undefined;
            }
          }
        },
      };
    },
    resolveResourceLink: (link) => store.resolveResourceLink(link),
    close: () => {
      if (serverClose) return serverClose;
      serverClosing = true;
      serverClose = closeArtifactServer(
        leases,
        sessions,
        httpServer,
        store,
      );
      return serverClose;
    },
  };
}

async function closeArtifactServer(
  leases: Map<string, ArtifactLeaseState>,
  sessions: Map<
    string,
    {
      mcp: McpServer;
      transport: StreamableHTTPServerTransport;
      leaseToken: string;
    }
  >,
  httpServer: HttpServer,
  store: ArtifactStore,
): Promise<void> {
  let sessionError: unknown;
  try {
    await closeArtifactSessions(leases, sessions);
  } catch (err) {
    sessionError = err;
  }
  let httpError: unknown;
  try {
    await closeHttpServer(httpServer);
  } catch (err) {
    httpError = err;
  } finally {
    store.close();
  }
  if (sessionError !== undefined || httpError !== undefined) {
    throw new AggregateError(
      [
        ...(sessionError !== undefined ? [sessionError] : []),
        ...(httpError !== undefined ? [httpError] : []),
      ],
      "Failed to close artifact MCP server",
    );
  }
}

export async function closeArtifactSessions(
  leases: Map<string, ArtifactLeaseState>,
  sessions: Map<
    string,
    {
      mcp: McpServer;
      transport: StreamableHTTPServerTransport;
      leaseToken: string;
    }
  >,
): Promise<void> {
  const leaseStates = [...leases.values()];
  for (const lease of leaseStates) {
    lease.closing = true;
  }
  leases.clear();
  await Promise.all(
    leaseStates.flatMap((lease) => [...lease.inFlightInitializations]),
  );

  const results = await Promise.allSettled(
    [...sessions.values()].map(({ mcp }) => mcp.close()),
  );
  sessions.clear();
  const failures = results.filter(
    (result): result is PromiseRejectedResult =>
      result.status === "rejected",
  );
  if (failures.length > 0) {
    throw new AggregateError(
      failures.map((failure) => failure.reason),
      "Failed to close artifact MCP sessions",
    );
  }
}

export async function closeLease(
  token: string,
  lease: ArtifactLeaseState,
  leases: Map<string, ArtifactLeaseState>,
  sessions: Map<
    string,
    {
      mcp: McpServer;
      transport: StreamableHTTPServerTransport;
      leaseToken: string;
    }
  >,
): Promise<void> {
  lease.closing = true;
  leases.delete(token);
  await Promise.all([...lease.inFlightInitializations]);
  const sessionIds = lease.sessionIds;
  const closing: Array<{
    sessionId: string;
    session: {
      mcp: McpServer;
      transport: StreamableHTTPServerTransport;
      leaseToken: string;
    };
    promise: Promise<void>;
  }> = [];
  for (const sessionId of sessionIds) {
    const session = sessions.get(sessionId);
    if (!session || session.leaseToken !== token) {
      sessionIds.delete(sessionId);
      continue;
    }
    closing.push({
      sessionId,
      session,
      promise: Promise.resolve().then(() => session.mcp.close()),
    });
  }
  const results = await Promise.allSettled(
    closing.map(({ promise }) => promise),
  );
  const failures: unknown[] = [];
  for (let index = 0; index < results.length; index++) {
    const result = results[index]!;
    const operation = closing[index]!;
    if (result.status === "fulfilled") {
      if (sessions.get(operation.sessionId) === operation.session) {
        sessions.delete(operation.sessionId);
      }
      sessionIds.delete(operation.sessionId);
    } else {
      failures.push(result.reason);
    }
  }
  if (failures.length > 0) {
    throw new AggregateError(
      failures,
      "Failed to close artifact MCP lease",
    );
  }
}

function bearerToken(authorization: string | undefined): string | null {
  if (!authorization?.startsWith("Bearer ")) return null;
  const token = authorization.slice("Bearer ".length);
  return token || null;
}

async function createMcpSession(
  store: ArtifactStore,
  log: (message: string) => void,
): Promise<{ mcp: McpServer; transport: StreamableHTTPServerTransport }> {
  const mcp = new McpServer({
    name: "wechat-acp-artifacts",
    version: "1.0.0",
  });
  mcp.registerTool(
    "attach_file",
    {
      title: "Attach a file",
      description:
        "Attach an existing file from the current workspace to the user. " +
        "Call this after creating a file that the user should receive.",
      inputSchema: {
        path: z.string().min(1).describe("Workspace-relative or absolute path"),
        name: z.string().min(1).max(255).optional(),
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: false,
      },
    },
    async ({ path, name }) => {
      try {
        const artifact = await store.publish(path, name);
        return {
          content: [
            {
              type: "resource_link" as const,
              uri: artifact.uri,
              name: artifact.name,
              mimeType: artifact.mimeType,
              size: artifact.size,
              description: "File attachment for the current user",
            },
          ],
        };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log(`[artifact-mcp] attach_file rejected: ${message}`);
        return {
          isError: true,
          content: [
            {
              type: "text" as const,
              text: `Unable to attach file: ${message}`,
            },
          ],
        };
      }
    },
  );

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: randomUUID,
  });
  transport.onerror = (err) => {
    log(`[artifact-mcp] transport error: ${err.message}`);
  };
  await mcp.connect(transport);
  return { mcp, transport };
}

function listen(
  app: ReturnType<typeof createMcpExpressApp>,
): Promise<HttpServer> {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, "127.0.0.1", () => resolve(server));
    server.once("error", reject);
  });
}

function closeHttpServer(server: HttpServer): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}
