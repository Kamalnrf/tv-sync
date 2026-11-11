// One Durable Object per space. Ported from SimpleServer in loro-dev/protocol, with these changes:
// - hibernatable WebSockets: the object can sleep while sockets stay open;
// - every room is a LoroDoc persisted as a snapshot in SQLite (no update log);
// - joins authenticate with a device token; devices come from one-time invites and can be revoked;
// - a join always backfills what the client lacks, even when nobody else is connected.

import { DurableObject } from "cloudflare:workers";
import type { Env } from "../alchemy.run";
import { initSync, LoroDoc, VersionVector } from "loro-crdt/web";
import loroWasm from "loro-crdt/web/loro_wasm_bg.wasm";
import {
  CrdtType,
  decode,
  encode,
  JoinErrorCode,
  MessageType,
  RoomErrorCode,
  UpdateStatusCode,
  type HexString,
  type ProtocolMessage,
} from "loro-protocol";
import { bearer, json, randomToken, sha256 } from "./http";

initSync({ module: loroWasm });

const SNAPSHOT_CHUNK = 1_000_000; // DO SQLite caps a row at 2 MB, so snapshots are split
const FRAGMENT_SIZE = 200 * 1024; // the protocol caps a message at 256 KiB
const FRAGMENT_TIMEOUT_MS = 10_000;
const INVITE_TTL_MS = 15 * 60_000;
const CLOSE_REVOKED = 4003;

/** Per-socket state. Stored as the socket's attachment so it survives hibernation. */
type Attachment = { sid: string; device?: string };

type Partial = { room: string; parts: Uint8Array[]; got: number };

export class Space extends DurableObject<Env> {
  private sql: SqlStorage;
  // Caches. Hibernation drops them; they're rebuilt from SQLite on demand.
  private docs = new Map<string, LoroDoc>();
  private partials = new Map<string, Partial>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS devices (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
        created_at INTEGER NOT NULL, last_seen INTEGER, revoked_at INTEGER);
      CREATE TABLE IF NOT EXISTS invites (
        code_hash TEXT PRIMARY KEY, created_by TEXT NOT NULL,
        expires_at INTEGER NOT NULL, used_at INTEGER);
      CREATE TABLE IF NOT EXISTS snapshots (
        room TEXT NOT NULL, chunk INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (room, chunk));
      CREATE TABLE IF NOT EXISTS members (sid TEXT NOT NULL, room TEXT NOT NULL, PRIMARY KEY (sid, room));
    `);
    // Keepalive without waking the object: the runtime answers "ping" itself.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    // Forget memberships of sockets that died without a close event (e.g. across a deploy).
    const live = new Set(ctx.getWebSockets().map((ws) => attachment(ws).sid));
    for (const { sid } of this.sql.exec<{ sid: string }>("SELECT DISTINCT sid FROM members")) {
      if (!live.has(sid)) this.sql.exec("DELETE FROM members WHERE sid = ?", sid);
    }
  }

  // ── RPC from the Worker ────────────────────────────────────────────────────────────────

  async create(name: string, device: string) {
    const creds = await newCredentials();
    if (this.created()) throw new Error("space already exists");
    this.sql.exec("INSERT INTO meta (key, value) VALUES ('created', ?), ('name', ?)", String(Date.now()), name);
    return this.insertDevice(device, creds);
  }

  /** The device ID owning this token, or null if it's unknown or revoked. */
  async authorize(token: string): Promise<string | null> {
    return (await this.deviceFor(token))?.id ?? null;
  }

  // ── HTTP ──────────────────────────────────────────────────────────────────────────────

  async fetch(req: Request): Promise<Response> {
    if (!this.created()) return json({ error: "no such space" }, 404);
    // /spaces/:id/<rest>
    const rest = new URL(req.url).pathname.split("/").filter(Boolean).slice(2).join("/");
    const route = `${req.method} /${rest}`;

    if (route === "GET /ws") return this.accept(req);
    if (route === "POST /join") return this.redeem(await req.json());

    const me = await this.deviceFor(bearer(req));
    if (!me) return json({ error: "unauthorized" }, 401);
    if (route === "POST /invites") return this.invite(me.id);
    if (route === "GET /devices") return json(this.devices(me.id));
    const revoke = route.match(/^POST \/devices\/([^/]+)\/revoke$/);
    if (revoke) return this.revoke(revoke[1]);
    return json({ error: "not found" }, 404);
  }

  private async invite(createdBy: string) {
    const code = randomToken("inv_", 16);
    const expiresAt = Date.now() + INVITE_TTL_MS;
    this.sql.exec(
      "INSERT INTO invites (code_hash, created_by, expires_at) VALUES (?, ?, ?)",
      await sha256(code),
      createdBy,
      expiresAt,
    );
    return json({ code, expires_at: expiresAt }, 201);
  }

  private async redeem(body: { code?: string; name?: string }) {
    const { code, name } = body;
    if (!code || !name) return json({ error: "code and name are required" }, 400);
    const codeHash = await sha256(code);
    const creds = await newCredentials();
    // No awaits from here on, so no other request can run in between the checks and writes.
    const taken = this.sql.exec("SELECT 1 FROM devices WHERE name = ? AND revoked_at IS NULL", name).toArray();
    if (taken.length) return json({ error: `a device named "${name}" already exists` }, 409);
    const now = Date.now();
    // Check and spend in one statement: two joins with the same code can't both win.
    const spent = this.sql.exec(
      "UPDATE invites SET used_at = ? WHERE code_hash = ? AND used_at IS NULL AND expires_at > ?",
      now,
      codeHash,
      now,
    );
    if (spent.rowsWritten !== 1) return json({ error: "invite is invalid, expired or used" }, 403);
    return json(this.insertDevice(name, creds), 201);
  }

  private insertDevice(name: string, creds: Credentials) {
    // Only the token's hash is stored: a leaked database can't be used to sign in.
    this.sql.exec(
      "INSERT INTO devices (id, name, token_hash, created_at) VALUES (?, ?, ?, ?)",
      creds.id,
      name,
      creds.tokenHash,
      Date.now(),
    );
    const spaceName = this.sql.exec<{ value: string }>("SELECT value FROM meta WHERE key = 'name'").one().value;
    return { space_id: this.ctx.id.toString(), name: spaceName, device_id: creds.id, token: creds.token };
  }

  private devices(me: string) {
    const online = new Set(this.ctx.getWebSockets().map((ws) => attachment(ws).device));
    return this.sql
      .exec<{ id: string; name: string; created_at: number; last_seen: number | null; revoked_at: number | null }>(
        "SELECT id, name, created_at, last_seen, revoked_at FROM devices ORDER BY created_at",
      )
      .toArray()
      .map((d) => ({ ...d, online: online.has(d.id), you: d.id === me }));
  }

  private revoke(id: string) {
    const done = this.sql.exec(
      "UPDATE devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
      Date.now(),
      id,
    );
    if (done.rowsWritten !== 1) return json({ error: "no such active device" }, 404);
    // Its unused invites die with it.
    this.sql.exec("DELETE FROM invites WHERE created_by = ? AND used_at IS NULL", id);
    for (const ws of this.ctx.getWebSockets()) {
      const a = attachment(ws);
      if (a.device !== id) continue;
      this.sql.exec("DELETE FROM members WHERE sid = ?", a.sid);
      ws.close(CLOSE_REVOKED, "device revoked");
    }
    return json({ revoked: id });
  }

  private async deviceFor(token: string): Promise<{ id: string } | undefined> {
    if (!token) return undefined;
    const row = this.sql
      .exec<{ id: string; last_seen: number | null }>(
        "SELECT id, last_seen FROM devices WHERE token_hash = ? AND revoked_at IS NULL",
        await sha256(token),
      )
      .toArray()[0];
    if (row && Date.now() - (row.last_seen ?? 0) > 60_000) {
      this.sql.exec("UPDATE devices SET last_seen = ? WHERE id = ?", Date.now(), row.id);
    }
    return row;
  }

  private created(): boolean {
    return this.sql.exec("SELECT 1 FROM meta WHERE key = 'created'").toArray().length > 0;
  }

  // ── WebSocket: the Loro Protocol ─────────────────────────────────────────────────────

  private accept(req: Request): Response {
    if (req.headers.get("Upgrade") !== "websocket") return json({ error: "expected websocket" }, 426);
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ sid: crypto.randomUUID() } satisfies Attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, data: ArrayBuffer | string) {
    if (typeof data === "string") {
      if (data === "ping") ws.send("pong");
      return;
    }
    let msg: ProtocolMessage;
    try {
      msg = decode(new Uint8Array(data));
    } catch {
      ws.close(1007, "malformed message");
      return;
    }
    switch (msg.type) {
      case MessageType.JoinRequest:
        return this.onJoin(ws, msg);
      case MessageType.DocUpdate:
        return this.onUpdate(ws, msg.roomId, msg.updates, msg.batchId);
      case MessageType.DocUpdateFragmentHeader:
        return this.onFragmentHeader(ws, msg.roomId, msg.batchId, msg.fragmentCount);
      case MessageType.DocUpdateFragment:
        return this.onFragment(ws, msg.batchId, msg.index, msg.fragment);
      case MessageType.Leave:
        this.sql.exec("DELETE FROM members WHERE sid = ? AND room = ?", attachment(ws).sid, msg.roomId);
        return;
      default:
        // Clients only send Ack to report their own import failures; nothing to do here.
        return;
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string) {
    this.sql.exec("DELETE FROM members WHERE sid = ?", attachment(ws).sid);
    try {
      ws.close(code, reason);
    } catch {
      // already closed
    }
  }

  async webSocketError(ws: WebSocket) {
    this.sql.exec("DELETE FROM members WHERE sid = ?", attachment(ws).sid);
  }

  private async onJoin(ws: WebSocket, msg: Extract<ProtocolMessage, { type: typeof MessageType.JoinRequest }>) {
    const { roomId } = msg;
    const crdt = CrdtType.Loro;
    const a = attachment(ws);
    if (msg.crdt !== crdt) {
      return this.send(ws, { type: MessageType.JoinError, crdt: msg.crdt, roomId, code: JoinErrorCode.Unknown, message: "only Loro rooms are supported" });
    }
    const device = await this.deviceFor(new TextDecoder().decode(msg.auth));
    // One socket belongs to one device: a second token on the same socket is refused.
    if (!device || (a.device && a.device !== device.id)) {
      return this.send(ws, { type: MessageType.JoinError, crdt, roomId, code: JoinErrorCode.AuthFailed, message: "invalid or revoked device token" });
    }
    if (!a.device) ws.serializeAttachment({ ...a, device: device.id } satisfies Attachment);
    this.sql.exec("INSERT OR IGNORE INTO members (sid, room) VALUES (?, ?)", a.sid, roomId);

    const doc = this.room(roomId);
    const serverVersion = doc.oplogVersion();
    this.send(ws, { type: MessageType.JoinResponseOk, crdt, roomId, permission: "write", version: serverVersion.encode() });

    // Backfill: send what the client lacks. The client does the mirror image with the version
    // above, so after one round trip both sides have everything.
    let clientVersion = new VersionVector(null);
    try {
      if (msg.version.length) clientVersion = VersionVector.decode(msg.version);
    } catch {
      // unreadable version: treat the client as empty and send everything
    }
    const cmp = serverVersion.compare(clientVersion);
    // 1: server is ahead; undefined: concurrent (each has something the other lacks).
    if (cmp === 1 || cmp === undefined) {
      this.sendUpdate(ws, roomId, doc.export({ mode: "update", from: clientVersion }));
    }
  }

  private onUpdate(ws: WebSocket, roomId: string, updates: Uint8Array[], batchId: HexString) {
    const crdt = CrdtType.Loro;
    const { sid } = attachment(ws);
    const ack = (status: (typeof UpdateStatusCode)[keyof typeof UpdateStatusCode]) =>
      this.send(ws, { type: MessageType.Ack, crdt, roomId, refId: batchId, status });

    if (!this.isMember(sid, roomId)) return ack(UpdateStatusCode.PermissionDenied);
    const doc = this.room(roomId);
    let status;
    try {
      status = doc.importBatch(updates);
    } catch {
      return ack(UpdateStatusCode.InvalidUpdate);
    }
    this.save(roomId, doc);
    ack(UpdateStatusCode.Ok);
    for (const peer of this.peers(roomId, sid)) {
      for (const update of updates) this.sendUpdate(peer, roomId, update);
    }
    if (status.pending) {
      // The update builds on history this server never got. Ask the client to rejoin: the
      // join handshake makes it send everything the server is missing.
      this.sql.exec("DELETE FROM members WHERE sid = ? AND room = ?", sid, roomId);
      this.send(ws, { type: MessageType.RoomError, crdt, roomId, code: RoomErrorCode.RejoinSuggested, message: "server is missing history; rejoin" });
    }
  }

  private onFragmentHeader(ws: WebSocket, roomId: string, batchId: HexString, count: number) {
    const key = `${attachment(ws).sid}:${batchId}`;
    this.partials.set(key, { room: roomId, parts: new Array(count), got: 0 });
    setTimeout(() => {
      if (!this.partials.delete(key)) return;
      this.send(ws, { type: MessageType.Ack, crdt: CrdtType.Loro, roomId, refId: batchId, status: UpdateStatusCode.FragmentTimeout });
    }, FRAGMENT_TIMEOUT_MS);
  }

  private onFragment(ws: WebSocket, batchId: HexString, index: number, fragment: Uint8Array) {
    const key = `${attachment(ws).sid}:${batchId}`;
    const p = this.partials.get(key);
    if (!p || index >= p.parts.length) return;
    if (!p.parts[index]) p.got++;
    p.parts[index] = fragment;
    if (p.got < p.parts.length) return;
    this.partials.delete(key);
    this.onUpdate(ws, p.room, [concat(p.parts)], batchId);
  }

  // ── Rooms ────────────────────────────────────────────────────────────────────────────

  private room(id: string): LoroDoc {
    let doc = this.docs.get(id);
    if (doc) return doc;
    doc = new LoroDoc();
    const chunks = this.sql
      .exec<{ data: ArrayBuffer }>("SELECT data FROM snapshots WHERE room = ? ORDER BY chunk", id)
      .toArray()
      .map((r) => new Uint8Array(r.data));
    if (chunks.length) doc.import(concat(chunks));
    this.docs.set(id, doc);
    return doc;
  }

  private save(id: string, doc: LoroDoc) {
    const snapshot = doc.export({ mode: "snapshot" });
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM snapshots WHERE room = ?", id);
      for (let i = 0; i * SNAPSHOT_CHUNK < snapshot.length; i++) {
        const part = snapshot.slice(i * SNAPSHOT_CHUNK, (i + 1) * SNAPSHOT_CHUNK);
        this.sql.exec("INSERT INTO snapshots (room, chunk, data) VALUES (?, ?, ?)", id, i, part);
      }
    });
  }

  private isMember(sid: string, room: string): boolean {
    return this.sql.exec("SELECT 1 FROM members WHERE sid = ? AND room = ?", sid, room).toArray().length > 0;
  }

  private peers(room: string, except: string): WebSocket[] {
    const sids = new Set(
      this.sql
        .exec<{ sid: string }>("SELECT sid FROM members WHERE room = ? AND sid != ?", room, except)
        .toArray()
        .map((r) => r.sid),
    );
    return this.ctx.getWebSockets().filter((ws) => sids.has(attachment(ws).sid));
  }

  private sendUpdate(ws: WebSocket, roomId: string, bytes: Uint8Array) {
    const crdt = CrdtType.Loro;
    const batchId = randomBatchId();
    if (bytes.length <= FRAGMENT_SIZE) {
      return this.send(ws, { type: MessageType.DocUpdate, crdt, roomId, updates: [bytes], batchId });
    }
    const count = Math.ceil(bytes.length / FRAGMENT_SIZE);
    this.send(ws, { type: MessageType.DocUpdateFragmentHeader, crdt, roomId, batchId, fragmentCount: count, totalSizeBytes: bytes.length });
    for (let i = 0; i < count; i++) {
      const fragment = bytes.subarray(i * FRAGMENT_SIZE, (i + 1) * FRAGMENT_SIZE);
      this.send(ws, { type: MessageType.DocUpdateFragment, crdt, roomId, batchId, index: i, fragment });
    }
  }

  private send(ws: WebSocket, msg: ProtocolMessage) {
    try {
      ws.send(encode(msg));
    } catch {
      // socket is closing; its close handler cleans up
    }
  }
}

type Credentials = { id: string; token: string; tokenHash: string };

async function newCredentials(): Promise<Credentials> {
  const token = randomToken("tvd_", 32);
  return { id: randomToken("dev_", 6), token, tokenHash: await sha256(token) };
}

function attachment(ws: WebSocket): Attachment {
  return ws.deserializeAttachment() as Attachment;
}

function randomBatchId(): HexString {
  const bytes = crypto.getRandomValues(new Uint8Array(8));
  return `0x${[...bytes].map((b) => b.toString(16).padStart(2, "0")).join("")}`;
}

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}
