// Requires Playwright and Chromium. Optional: PLAYWRIGHT_PACKAGE, CHROME_PATH.
// Run after generate_readme_figure.py. This captures native library output.
const {chromium} = require(process.env.PLAYWRIGHT_PACKAGE || 'playwright');
const path = require('path');
const fs = require('fs');
const {pathToFileURL} = require('url');
const root = path.resolve(__dirname, '../..');
(async () => {
  const browser = await chromium.launch({headless:true,
    ...(process.env.CHROME_PATH ? {executablePath:process.env.CHROME_PATH} : {})});
  const page = await browser.newPage({viewport:{width:1040,height:850},deviceScaleFactor:2});
  const errors=[], network=[];
  page.on('pageerror',error=>errors.push(error.message));
  page.on('request',request=>{if(/^https?:/.test(request.url()))network.push(request.url());});
  await page.goto(pathToFileURL(path.join(root,'artifacts/readme/figure.html')).href);
  const frame = await (await page.locator('iframe').elementHandle()).contentFrame();
  await frame.waitForFunction(()=>document.querySelector('#retentioneering-root')?.textContent.length>20);
  await frame.locator('canvas').first().waitFor();
  await page.waitForTimeout(400);
  const destination=path.join(root,'docs/img/readme/checkout-comparison.png');
  await page.locator('.figure').screenshot({path:destination});
  await browser.close();
  const checks={errors,network,screenshot:path.relative(root,destination)};
  fs.writeFileSync(path.join(root,'artifacts/readme/browser-checks.json'),JSON.stringify(checks,null,2));
  console.log(JSON.stringify(checks,null,2));
  if(errors.length || network.length) process.exitCode=1;
})().catch(error=>{console.error(error);process.exit(1);});
