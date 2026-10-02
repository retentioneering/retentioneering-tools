// Record actual UI states from generate_readme_demos.py exports.
// Requires Playwright, Chromium and ffmpeg. See .github/readme/ASSETS.md.
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');
const { spawnSync } = require('child_process');
const { chromium } = require(process.env.PLAYWRIGHT_PACKAGE || 'playwright');

const repo = path.resolve(__dirname, '../..');
const build = path.join(repo, 'docs/build/readme');
const assets = path.join(repo, '.github/readme');

async function main() {
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : {}),
  });
  const checks = [];
  try {
    for (const name of ['step-matrix', 'cluster-analysis']) {
      const page = await browser.newPage({ viewport: { width: 760, height: 760 }, offline: true });
      const errors = [], network = [];
      page.on('pageerror', e => errors.push(e.message));
      page.on('request', r => { if (/^https?:/.test(r.url())) network.push(r.url()); });
      const frames = path.join(build, name + '-frames');
      fs.mkdirSync(frames, { recursive: true });
      await page.goto(pathToFileURL(path.join(build, name + '.html')).href);
      await page.waitForFunction(() => document.querySelector('#retentioneering-root')?.textContent.length > 30);
      const root = page.locator('#retentioneering-root');
      if (name === 'cluster-analysis') {
        const handle = page.locator('[style*="cursor: col-resize"]').first();
        const box = await handle.boundingBox();
        if (!box) throw new Error('Cluster metric-column resize handle is missing');
        await page.mouse.move(box.x + box.width / 2, box.y + 12);
        await page.mouse.down();
        await page.mouse.move(box.x + 155, box.y + 12, { steps: 12 });
        await page.mouse.up();
        await page.mouse.move(0, 0);
      }
      let index = 0;
      async function capture() {
        // Allow tooltip or tab repaint to reach the compositor.
        await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
        await root.screenshot({ path: path.join(frames, `frame-${String(index++).padStart(2, '0')}.png`) });
      }
      await capture();
      await root.screenshot({ path: path.join(assets, name + '.png') });
      if (name === 'step-matrix') {
        await page.locator('tr[data-event="purchase"] td[data-step="1"]').hover();
        await capture();
        await page.locator('tr[data-event="path_end"] td[data-step="1"]').hover();
        await capture();
        await page.mouse.move(0, 0);
        await capture();
      } else {
        await page.getByRole('button', { name: 'Silhouette', exact: true }).click();
        await capture();
        await page.getByRole('button', { name: 'Overview', exact: true }).click();
        await capture();
      }
      const ffmpeg = spawnSync(process.env.FFMPEG || 'ffmpeg', [
        '-y', '-v', 'error', '-framerate', '1/2', '-i', path.join(frames, 'frame-%02d.png'),
        '-filter_complex', '[0:v]split[a][b];[a]palettegen=stats_mode=diff[p];[b][p]paletteuse=dither=bayer:bayer_scale=4',
        '-loop', '0', path.join(assets, name + '.gif'),
      ], { encoding: 'utf8' });
      if (ffmpeg.error || ffmpeg.status !== 0) throw new Error(ffmpeg.error?.message || ffmpeg.stderr);
      checks.push({ name, frames: index, errors, network, bytes: fs.statSync(path.join(assets, name + '.gif')).size });
      await page.close();
    }
    const page = await browser.newPage({ viewport: { width: 960, height: 720 }, offline: true });
    const errors = [], network = [];
    page.on('pageerror', e => errors.push(e.message));
    page.on('request', r => { if (/^https?:/.test(r.url())) network.push(r.url()); });
    await page.goto(pathToFileURL(path.join(build, 'agent-runs.html')).href);
    await page.locator('canvas').first().waitFor();
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    await page.waitForTimeout(700); // the graph animates its initial layout
    await page.locator('#retentioneering-root').screenshot({ path: path.join(assets, 'agent-paths.png') });
    checks.push({ name: 'agent-paths', frames: 1, errors, network,
      bytes: fs.statSync(path.join(assets, 'agent-paths.png')).size });
    await page.close();
  } finally {
    await browser.close();
  }
  fs.writeFileSync(path.join(build, 'recording-checks.json'), JSON.stringify(checks, null, 2) + '\n');
  console.log(JSON.stringify(checks, null, 2));
  if (checks.some(c => c.errors.length || c.network.length)) process.exitCode = 1;
}

main().catch(e => { console.error(e); process.exitCode = 1; });
