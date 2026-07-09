import { $ } from "bun";
import { rm } from "node:fs/promises";

await rm("./dist", { recursive: true, force: true });
await $`bunx tsc -p tsconfig.build.json`;

await Bun.build({
  entrypoints: ["./src/index.ts"],
  outdir: "./dist",
  minify: true,
  target: "bun",
  packages: "external",
});
