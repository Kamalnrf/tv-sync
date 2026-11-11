// The Worker: routes HTTP to the right space's Durable Object and serves file blobs from R2.
//
//   POST /spaces {name, device}           create a space (anyone, 10 a minute per address)
//                                         → first device's token
//   POST /spaces/:id/join                 trade a one-time invite code for a device token
//   POST /spaces/:id/invites              (device) make an invite code
//   GET  /spaces/:id/devices              (device) list devices
//   POST /spaces/:id/devices/:dev/revoke  (device) revoke a device
//   GET  /spaces/:id/ws                   WebSocket speaking the Loro Protocol
//   HEAD|GET|PUT /spaces/:id/blobs/:sha256  (device) file contents, addressed by hash

import type { Env } from "../alchemy.run";
import { Space } from "./space";
import { bearer, json } from "./http";

export { Space };

export default {
  async fetch(req, env): Promise<Response> {
    const [top, spaceId, sub, hash] = new URL(req.url).pathname.split("/").filter(Boolean);
    if (top !== "spaces") return json({ error: "not found" }, 404);

    if (!spaceId) {
      if (req.method !== "POST") return json({ error: "method not allowed" }, 405);
      // Open to anyone: a space stays private through its unguessable ID, one-time invites and
      // device tokens. The limit only keeps one address from creating spaces in bulk.
      const from = req.headers.get("cf-connecting-ip") ?? "local";
      if (!(await env.CREATE_LIMIT.limit({ key: from })).success) {
        return json({ error: "too many new spaces from this address; try again in a minute" }, 429);
      }
      const { name, device } = await req.json<{ name?: string; device?: string }>();
      if (!name || !device) return json({ error: "space name and device name are required" }, 400);
      // newUniqueId() rather than idFromName(): IDs can't be guessed, and idFromString() below
      // rejects IDs this namespace never issued, so random URLs can't create empty objects.
      return json(await env.SPACES.get(env.SPACES.newUniqueId()).create(name, device), 201);
    }

    let space: DurableObjectStub<Space>;
    try {
      space = env.SPACES.get(env.SPACES.idFromString(spaceId));
    } catch {
      return json({ error: "no such space" }, 404);
    }
    if (sub === "blobs" && hash) return blob(req, env, space, spaceId, hash);
    return space.fetch(req);
  },
} satisfies ExportedHandler<Env>;

async function blob(
  req: Request,
  env: Env,
  space: DurableObjectStub<Space>,
  spaceId: string,
  hash: string,
): Promise<Response> {
  if (!/^[0-9a-f]{64}$/.test(hash)) return json({ error: "blob name must be a sha256 hex" }, 400);
  if (!(await space.authorize(bearer(req)))) return json({ error: "unauthorized" }, 401);
  // Keyed per space, so a hash learned elsewhere can't read another space's files.
  const key = `${spaceId}/${hash}`;
  switch (req.method) {
    case "HEAD":
      return new Response(null, { status: (await env.BLOBS.head(key)) ? 200 : 404 });
    case "GET": {
      const obj = await env.BLOBS.get(key);
      return obj ? new Response(obj.body) : json({ error: "no such blob" }, 404);
    }
    case "PUT":
      try {
        // R2 hashes the body itself and rejects it if it doesn't match, so nothing can be
        // stored under the wrong name.
        await env.BLOBS.put(key, req.body, { sha256: hash });
      } catch {
        return json({ error: "content does not match its sha256" }, 400);
      }
      return new Response(null, { status: 201 });
    default:
      return json({ error: "method not allowed" }, 405);
  }
}
