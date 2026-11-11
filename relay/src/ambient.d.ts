// Workers import a .wasm file as a compiled module (space.ts loads Loro's this way).
declare module "*.wasm" {
  const value: WebAssembly.Module;
  export default value;
}

// alchemy.run.ts runs in Node, but it's type-checked with src/ because src imports its Env type.
// Worker types don't declare import.meta.url; this adds only that, not all of Node's globals.
interface ImportMeta {
  readonly url: string;
}
