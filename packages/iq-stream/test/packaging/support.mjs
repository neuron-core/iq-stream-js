// Shared by the packaging tests. They never look at the source tree: they pack the package as npm
// would publish it, unpack the tarball into a fixture's node_modules, and build small applications
// against that.
import { execFileSync } from 'node:child_process';
import { cp, mkdir, readFile, readdir, rm } from 'node:fs/promises';
import path from 'node:path';

export const packageDir = path.resolve(import.meta.dirname, '../..');
const outRoot = path.join(import.meta.dirname, '.out');

export const MARKS = {
  publisherNode: 'iq-stream/publisher/node',
  publisherFetch: 'iq-stream/publisher/fetch',
  client: 'iq-stream/client',
  stub: 'iq-stream/browser-stub',
};
/** Anything that only a publisher build contains. */
const PUBLISHER_TRACES = [/iq-stream\/publisher\/(node|fetch)/, /node:(http|https|crypto|net|tls)/, /x-ndjson/];

let tarball;
/** `npm pack` once per process: the files a release would contain, and nothing else. */
export function pack() {
  if (!tarball) {
    execFileSync('npm', ['pack', '--silent', '--pack-destination', outRoot], { cwd: packageDir, stdio: ['ignore', 'pipe', 'inherit'] });
    tarball = path.join(outRoot, execFileSync('ls', [outRoot], { encoding: 'utf8' }).split('\n').find((f) => f.endsWith('.tgz')));
  }
  return tarball;
}

/** A directory with the packed package installed under node_modules, and the test applications. */
export async function fixture(name) {
  const dir = path.join(outRoot, name), installed = path.join(dir, 'node_modules/@neuron-core/iq-stream');
  await rm(dir, { recursive: true, force: true });
  await mkdir(outRoot, { recursive: true });
  await mkdir(installed, { recursive: true });
  execFileSync('tar', ['-xzf', pack(), '-C', installed, '--strip-components=1']);
  await cp(path.join(import.meta.dirname, 'apps'), path.join(dir, 'apps'), { recursive: true });
  return dir;
}

/** The JavaScript a build wrote, concatenated. */
export async function output(dir) {
  let text = '';
  for (const file of await readdir(dir, { recursive: true })) {
    if (/\.(m?js|cjs)$/.test(file)) text += await readFile(path.join(dir, file), 'utf8');
  }
  return text;
}

export const publisherTraces = (text) => PUBLISHER_TRACES.filter((trace) => trace.test(text)).map(String);

/**
 * Loads a bundle the way a page would, in a process of its own, then calls `__use()` if the
 * application defined it. Returns 'loaded', 'load failed: …', 'used' or 'use failed: …'.
 */
export function execute(file) {
  const script = `
    try { await import(${JSON.stringify(file)}); }
    catch (error) { console.log('load failed: ' + error.message); process.exit(0); }
    if (!globalThis.__use) { console.log('loaded'); process.exit(0); }
    try { globalThis.__use(); console.log('used'); } catch (error) { console.log('use failed: ' + error.message); }
    process.exit(0);`;
  return execFileSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' }).trim();
}

const externals = ['ai', '@ag-ui/client', 'rxjs'];

/** One way to build each of the four bundlers for a browser, each writing `<out>/app.mjs`. */
export const browserBundlers = {
  async esbuild(dir, entry, out) {
    const { build } = await import('esbuild');
    await build({ absWorkingDir: dir, entryPoints: [entry], bundle: true, format: 'esm', platform: 'browser', outfile: path.join(out, 'app.mjs'), external: externals, logLevel: 'silent' });
  },
  async vite(dir, entry, out) {
    const { build } = await import('vite');
    await build({ root: dir, logLevel: 'silent', configFile: false, build: { outDir: out, emptyOutDir: true, minify: false, lib: { entry: path.join(dir, entry), formats: ['es'], fileName: () => 'app.mjs' }, rollupOptions: { external: externals } } });
  },
  async webpack(dir, entry, out) {
    const { default: webpack } = await import('webpack');
    await new Promise((resolve, reject) => {
      webpack({ mode: 'production', target: 'web', context: dir, entry: path.join(dir, entry), optimization: { minimize: false }, experiments: { outputModule: true },
        externalsType: 'module', externals, output: { path: out, filename: 'app.mjs', module: true, library: { type: 'module' } } }, (error, stats) => {
        const problems = stats?.toJson({ all: false, errors: true }).errors ?? [];
        if (error || problems.length) reject(error ?? new Error(problems[0].message)); else resolve();
      });
    });
  },
  async rollup(dir, entry, out) {
    const { rollup } = await import('rollup');
    const { default: resolve } = await import('@rollup/plugin-node-resolve');
    const bundle = await rollup({ input: path.join(dir, entry), external: externals, plugins: [resolve({ browser: true, rootDir: dir })], onwarn() {} });
    await bundle.write({ file: path.join(out, 'app.mjs'), format: 'es' });
  },
};
