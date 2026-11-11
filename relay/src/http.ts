export function json(body: unknown, status = 200): Response {
  return Response.json(body, { status });
}

export function bearer(req: Request): string {
  return req.headers.get("Authorization")?.replace(/^Bearer /, "") ?? "";
}

export function randomToken(prefix: string, bytes: number): string {
  const raw = crypto.getRandomValues(new Uint8Array(bytes));
  return prefix + btoa(String.fromCharCode(...raw)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
