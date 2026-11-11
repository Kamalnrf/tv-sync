// The relay, for both local runs and Cloudflare:
//   npm run dev      runs it on this machine (workerd, local SQLite and R2), no account needed;
//                    on port 8787, or RELAY_PORT
//   npm run login    once, to connect the `tv-sync` profile to your Cloudflare account
//   npm run deploy   makes Cloudflare match this file
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import type { Space } from "./src/space.ts";

export const Relay = Cloudflare.Worker("Relay", {
  name: "tv-sync-relay",
  // Anchored to this file: a bare "./src/index.ts" resolves against the working directory, so
  // running Alchemy from another folder would find no Worker.
  main: new URL("./src/index.ts", import.meta.url).href,
  compatibility: { date: "2026-10-01" },
  observability: { enabled: true },
  dev: { port: Number(process.env.RELAY_PORT ?? 8787) },
  env: {
    // One Durable Object per space, SQLite-backed (Alchemy's default storage).
    SPACES: Cloudflare.DurableObject<Space>("Space"),
    // File contents by sha256.
    BLOBS: Cloudflare.R2.Bucket("Blobs", { name: "tv-sync-blobs" }),
    // Anyone may create a space; this only caps how many one address creates a minute.
    CREATE_LIMIT: Cloudflare.RateLimit("CreateLimit", { namespaceId: 7301, simple: { limit: 10, period: 60 } }),
  },
});

/** The bindings src/ sees, derived from the declaration above so the two can't disagree. */
export type Env = Cloudflare.InferEnv<typeof Relay>;

export default Alchemy.Stack(
  "TvSyncRelay",
  // State (what Alchemy deployed, to diff the next deploy against) lives in .alchemy/.
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const relay = yield* Relay;
    return { url: relay.url };
  }),
);
