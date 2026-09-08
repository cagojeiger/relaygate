const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const { chromium } = require('playwright');

async function main() {
  const output = process.env.GRAFANA_SCREENSHOTS;
  assert(output, 'GRAFANA_SCREENSHOTS is required');
  await fs.mkdir(output, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1050 } });
  page.setDefaultTimeout(30_000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('response', response => {
    if (response.url().includes('/api/ds/query') && response.status() >= 400) {
      errors.push(`query HTTP ${response.status()}`);
    }
  });
  const base = process.env.GRAFANA_URL || 'http://127.0.0.1:23000';
  const params = 'var-cluster=compose&var-namespace=relaygate&from=now-15m&to=now';
  async function capture(name) {
    // Settling is only for the screenshot; contract assertions use locators/API results.
    await page.waitForLoadState('networkidle');
    await page.evaluate(() => document.fonts.ready);
    await page.screenshot({ path: `${output}/${name}.png`, fullPage: true });
  }
  try {
    for (const uid of ['relaygate-overview', 'relaygate-runtime', 'relaygate-sdk']) {
      const response = await page.request.get(`${base}/api/dashboards/uid/${uid}`);
      assert.equal(response.status(), 200, `provisioning ${uid}`);
    }
    await page.goto(`${base}/d/relaygate-overview?${params}`, { waitUntil: 'domcontentloaded' });
    await page.getByText('고유 Pipe', { exact: true }).first().waitFor();
    await page.getByText('용량 · 자원별 최대 점유율', { exact: true }).waitFor();
    await capture('overview-light');

    await page.getByRole('link', { name: 'GW·RT 진단', exact: true }).click();
    await page.waitForURL('**/d/relaygate-runtime**');
    const current = new URL(page.url());
    for (const [key, value] of new URLSearchParams(params)) {
      assert.equal(current.searchParams.get(key), value, `preserved ${key}`);
    }
    await page.getByText('연결과 큐 · Gateway', { exact: true }).waitFor();
    assert.equal(await page.getByText('진행 중 연결', { exact: true }).isVisible(), false);
    await capture('runtime-collapsed');
    await page.getByText('연결과 큐 · Gateway', { exact: true }).click();
    await page.getByText('진행 중 연결', { exact: true }).waitFor();
    await capture('runtime-gateway');

    await page.getByRole('link', { name: 'SDK 복구', exact: true }).click();
    await page.waitForURL('**/d/relaygate-sdk**');
    await page.getByText('아직 재연결 중인 수', { exact: true }).waitFor();
    await page.getByText('No data', { exact: true }).first().waitFor();
    await capture('sdk-no-data');

    await page.goto(`${base}/d/relaygate-overview?${params}&theme=dark`, { waitUntil: 'domcontentloaded' });
    await page.getByText('용량 · 자원별 최대 점유율', { exact: true }).waitFor();
    await capture('overview-dark');
    await page.setViewportSize({ width: 768, height: 1024 });
    await capture('overview-compact');
    assert.deepEqual(errors, [], 'Grafana runtime/query errors');
  } catch (error) {
    await page.screenshot({ path: `${output}/failure.png`, fullPage: true }).catch(() => {});
    await fs.writeFile(`${output}/failure.txt`, `${error.stack}\n${await page.locator('body').innerText()}`);
    throw error;
  } finally {
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
