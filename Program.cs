using Microsoft.Playwright;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Diagnostics;

static class UX
{
    public const int Short = 1200, Med = 3000, Long = 10000, XL = 15000;

    // All-models grid
    public const string AllModelCard = "div.allmodelscard.container.responsivegrid[role='button']";
    public const string CardExpandBuild = ".cmp-allmodelscarddetail__expandview-buttons-animated:has(a:has-text('Build & Price'))";
    public const string BuildBtn = "a.cmp-button:has-text('Build & Price')";
    public const string LogoLink = "a.cmp-logo__link";

    // Configurator
    public const string LinesList = "con-lines-list .con-tile-line";
    public const string LineName = "p.line-name";
    public const string ChangeEngineBtn = "button:has-text('Change Engine')";
    public const string EngineModalTile = "con-tile-engine.engine-tile";
    public const string SummaryBtn = "button.button-summary:has-text('Summary')";
    public const string ContinueBtn = "button.button-primary:has-text('Continue')";
    public const string BackToConfig = "a:has-text('Back to configuration')";
    public const string ConfigureInTab = "button:has-text('Configure in current tab'), button[aria-label='Configure in current tab']";

    public const string SummaryAccordions = "con-accordion";
    public const string SummaryItemValues = "span.item-value";
    public const string HeroImage = "#heroVisualsSection img#image";
}

// --- Minimal profiler ---------------------------------------------------------
static class Perf
{
    private static readonly object _lock = new();
    private static Stopwatch _sw = new();
    private static string _group = "";

    public static void StartGroup(string groupName)
    {
        lock (_lock)
        {
            _group = groupName;
            _sw.Restart();
            Console.WriteLine($"⏱️ [Perf] ➜ Start '{_group}'");
        }
    }

    public static void EndGroup(string? note = null)
    {
        lock (_lock)
        {
            if (_sw.IsRunning)
            {
                long total = _sw.ElapsedMilliseconds;
                string suffix = string.IsNullOrWhiteSpace(note) ? "" : $" — {note}";
                Console.WriteLine($"⏱️ [Perf] ➜ Finished '{_group}' in {total} ms{suffix}");
                _sw.Reset();
                _group = "";
            }
        }
    }
}

class BMWCARS
{
    // ----------------- Run controls -----------------
    private const int MAX_CARS_TO_SCRAPE = int.MaxValue;   // <— process ALL cards
    private const bool KEEP_FIRST_SUMMARY_OPEN = false;     // leave first summary tab open for inspection

    // Output files
    private const string URLS_CSV = "bmw_summary_urls.csv";
    private const string DATA_CSV = "bmw_summary_data.csv";

    // ---------- URL & code de-dupe helpers ----------
    private static readonly HashSet<string> SeenUrlKeys = new(StringComparer.OrdinalIgnoreCase);   // de-dupe by host+path
    private static readonly HashSet<string> SeenLineCodes = new(StringComparer.OrdinalIgnoreCase); // de-dupe by line code (e.g., IX22 / IXSC)

    private static string UrlKey(string? url)
    {
        if (string.IsNullOrWhiteSpace(url)) return "";
        try
        {
            var u = new Uri(url);
            return $"{u.Scheme}://{u.Host}{u.AbsolutePath}".TrimEnd('/');
        }
        catch { return url.Trim().TrimEnd('/'); }
    }

    private static string BestUrlKey(string configureUrl, string summaryUrl)
    {
        var k = UrlKey(configureUrl);
        return string.IsNullOrEmpty(k) ? UrlKey(summaryUrl) : k;
    }

    private static (string seriesCode, string lineCode) ExtractSeriesAndLineFromConfigure(string? url)
    {
        if (string.IsNullOrWhiteSpace(url)) return ("", "");
        try
        {
            var u = new Uri(url);
            var segs = u.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            int i = Array.FindIndex(segs, s => s.Equals("configure", StringComparison.OrdinalIgnoreCase));
            if (i >= 0 && segs.Length >= i + 3)
            {
                return (segs[i + 1], segs[i + 2]); // {Series}/{Line}
            }
        }
        catch { }
        return ("", "");
    }

    private static (string seriesCode, string lineCode, string modelCode) ExtractCodesFromSummary(string? url)
    {
        if (string.IsNullOrWhiteSpace(url)) return ("", "", "");
        try
        {
            var u = new Uri(url);
            var segs = u.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            int i = Array.FindIndex(segs, s => s.Equals("summary", StringComparison.OrdinalIgnoreCase));
            string series = "", line = "", model = "";
            if (i >= 0 && segs.Length >= i + 3)
            {
                series = segs[i + 1];
                line = segs[i + 2];
            }
            var last = segs.LastOrDefault() ?? "";
            if (Regex.IsMatch(last, @"^[A-Z]{2}\d{6}$")) model = last;
            return (series, line, model);
        }
        catch { }
        return ("", "", "");
    }

    public static async Task Main()
    {
        // Create + launch with correct disposal semantics
        using var playwright = await Playwright.CreateAsync(); // IDisposable
        await using var browser = await playwright.Chromium.LaunchAsync(new() { Headless = false }); // IAsyncDisposable
        await using var context = await browser.NewContextAsync(); // IAsyncDisposable
        var page = await context.NewPageAsync();

        await page.GotoAsync("https://www.bmw.co.uk/en/all-models.html", new() { WaitUntil = WaitUntilState.NetworkIdle });
        await AcceptCookiesIfPresentAsync(page);

        // ---------------- PHASE 1: collect unique summary URLs + metadata ----------------
        var urlRows = new List<Dictionary<string, string>>();
        var processedModelBodyKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Build ordered list from grid
        string baseCardSel = UX.AllModelCard;
        var allCards = page.Locator(baseCardSel);
        int totalCards = await allCards.CountAsync();

        var cardsOrdered = new List<(int Counter, string ModelName, string BodyType)>();
        for (int i = 0; i < totalCards; i++)
        {
            var card = allCards.Nth(i);
            string counterStr = await card.GetAttributeAsync("data-counter") ?? "-1";
            if (!int.TryParse(counterStr, out int counter)) continue;

            string modelName = await ExtractModelNameFromCardAsync(card);
            if (string.IsNullOrWhiteSpace(modelName)) continue;

            string bodyType = await ExtractBodyTypeFromCardAsync(card);
            if (string.IsNullOrWhiteSpace(bodyType)) bodyType = "Unknown";

            cardsOrdered.Add((counter, modelName, bodyType));
        }

        cardsOrdered = cardsOrdered.OrderBy(t => t.Counter).ToList();
        Console.WriteLine($"🔢 Models in order: {string.Join(", ", cardsOrdered.Select(x => $"{x.ModelName} [{x.BodyType}]:{x.Counter}"))}");

        // Limit to N (now: all)
        int limit = Math.Min(cardsOrdered.Count, MAX_CARS_TO_SCRAPE);

        for (int idx = 0; idx < limit; idx++)
        {
            var (counter, carName, bodyType) = cardsOrdered[idx];
            var key = MakeModelBodyKey(carName, bodyType);

            if (processedModelBodyKeys.Contains(key)) continue;

            Perf.StartGroup($"{carName} [{bodyType}] (order={counter})");
            Console.WriteLine($"\n🚘 [{idx + 1}/{limit}] {carName} [{bodyType}] (order={counter})");

            var card = page.Locator($"{baseCardSel}[data-counter='{counter}']");
            bool opened = false;
            try
            {
                await card.ScrollIntoViewIfNeededAsync();
                await Task.Delay(250);
                await card.ClickAsync();
                await Task.Delay(800);

                var expanded = page.Locator(UX.CardExpandBuild);
                var buildBtn = expanded.Locator(UX.BuildBtn);
                await buildBtn.WaitForAsync(new() { State = WaitForSelectorState.Visible, Timeout = 12000 });
                await buildBtn.ClickAsync();

                Console.WriteLine("✅ Opened configurator.");
                opened = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Could not open/configure {carName} [{bodyType}]: {ex.Message}");
                await SafeReturnToGrid(page, baseCardSel);
                Perf.EndGroup("open failed");
                continue;
            }

            int rowsBefore = urlRows.Count;

            if (opened)
            {
                var scrapedEnginesByModel = new Dictionary<string, HashSet<string>>();
                await WaitForConfiguratorReadyAsync(page); // readiness wait
                await ScrapeTrimsAndEngines_Phase1_CollectUrls(
                    page, carName, bodyType, urlRows, scrapedEnginesByModel);
            }

            if (urlRows.Count > rowsBefore)
            {
                processedModelBodyKeys.Add(key);
            }
            else
            {
                Console.WriteLine($"⚠️ No rows saved for {carName} [{bodyType}] — will allow later duplicates to try.");
            }

            await SafeReturnToGrid(page, baseCardSel);
            await Task.Delay(400);
            Perf.EndGroup();
        }

        // Final de-dup for URLs CSV: prefer LineCode then path key
        string RowKey1(Dictionary<string, string> r)
        {
            if (r.TryGetValue("LineCode", out var lc) && !string.IsNullOrWhiteSpace(lc)) return lc!;
            var k = UrlKey(r.TryGetValue("SummaryUrl", out var su) ? su : "");
            if (!string.IsNullOrWhiteSpace(k)) return k;
            return UrlKey(r.TryGetValue("ConfigureUrl", out var cu) ? cu : "");
        }

        urlRows = urlRows
            .GroupBy(RowKey1)
            .Select(g => g.First())
            .ToList();

        // Write Phase 1 CSV
        var urlHeaders = new List<string> {
            "Car","BodyType","Model","Engine",
            "SeriesCode","LineCode","ModelCode",
            "ImageUrl","ConfigureUrl","SummaryUrl"
        };

        using (var writer = new StreamWriter(URLS_CSV, append: false))
        {
            writer.WriteLine(string.Join(",", urlHeaders));
            foreach (var row in urlRows)
            {
                var line = string.Join(",", urlHeaders.Select(h => row.ContainsKey(h) ? $"\"{row[h].Replace("\"", "\"\"")}\"" : ""));
                writer.WriteLine(line);
            }
        }
        Console.WriteLine($"✅ Phase 1: wrote {urlRows.Count} rows to {URLS_CSV}");

        // ---------------- PHASE 2: visit each unique summary URL and scrape values ----------------
        Perf.StartGroup("Phase 2 — Summary scrape");
        var summaryRows = new List<Dictionary<string, string>>();
        var summaryHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var targets = urlRows
            .GroupBy(r => UrlKey(r["SummaryUrl"]))
            .Select(g => g.First())
            .ToList();

        Console.WriteLine($"🔍 Summary targets: {targets.Count}");

        IPage? previewPage = null;
        if (KEEP_FIRST_SUMMARY_OPEN && targets.Count > 0)
        {
            previewPage = await context.NewPageAsync();
            var okPrev = await GotoSummaryAsync(previewPage, CleanSummaryUrl(targets[0]["SummaryUrl"]));
            if (okPrev)
            {
                await AcceptCookiesIfPresentAsync(previewPage);
                await ScrollToBottomAsync(previewPage);     // trigger lazy content
                await ExpandAllAccordionsAsync(previewPage);
                Console.WriteLine($"👁️ Preview left open: {previewPage.Url}");
            }
        }

        var page2 = await context.NewPageAsync();
        page2.SetDefaultTimeout(30000);
        page2.SetDefaultNavigationTimeout(60000);

        int startIndex = (KEEP_FIRST_SUMMARY_OPEN ? 1 : 0);
        for (int i = startIndex; i < targets.Count; i++)
        {
            var meta = targets[i];
            var original = meta["SummaryUrl"];
            var clean = CleanSummaryUrl(original);

            Console.WriteLine($"\n🧾 [{i + 1}/{targets.Count}] {clean}");

            var ok = await GotoSummaryAsync(page2, clean);
            if (!ok)
            {
                var fallback = clean.Replace("https://configure.bmw.co.uk", "https://www.bmw.co.uk");
                Console.WriteLine("   ↪︎ retrying on www domain…");
                ok = await GotoSummaryAsync(page2, fallback);
                if (!ok)
                {
                    Console.WriteLine("❌ Failed to open summary (both domains). Skipping.");
                    continue;
                }
            }

            await AcceptCookiesIfPresentAsync(page2);
            await ScrollToBottomAsync(page2);     // ensure lazy sections load
            await ExpandAllAccordionsAsync(page2);

            // Wait for spec items to appear
            try { await page2.Locator(UX.SummaryItemValues).First.WaitForAsync(new() { Timeout = 15000 }); } catch { }

            var row = new Dictionary<string, string>();

            // Carry forward metadata
            foreach (var k in new[] { "Car","BodyType","Model","Engine","SeriesCode","LineCode","ModelCode","SummaryUrl" })
                if (meta.TryGetValue(k, out var v)) row[k] = v;

            // Fill missing codes from final URL if needed
            var finalUrl = page2.Url;
            var (s, l, m) = ExtractCodesFromSummary(finalUrl);
            if (!row.ContainsKey("SeriesCode") || string.IsNullOrWhiteSpace(row["SeriesCode"])) row["SeriesCode"] = s;
            if (!row.ContainsKey("LineCode")   || string.IsNullOrWhiteSpace(row["LineCode"]))   row["LineCode"]   = l;
            if (!row.ContainsKey("ModelCode")  || string.IsNullOrWhiteSpace(row["ModelCode"]))  row["ModelCode"]  = m;

            // Key-value spec items from accordions
            var items = page2.Locator(UX.SummaryItemValues);
            int itemCount = 0;
            try { itemCount = await items.CountAsync(); } catch { }

            for (int j = 0; j < itemCount; j++)
            {
                var item = items.Nth(j);
                string key = (await item.GetAttributeAsync("data-valuekey")) ?? "";
                string value = (await item.InnerTextAsync()).Trim();
                if (string.IsNullOrWhiteSpace(key)) continue;

                string label = NormalizeKey(key);
                if (!row.ContainsKey(label)) row[label] = value;
                summaryHeaders.Add(label);
            }

            // Image
            try
            {
                var img = page2.Locator(UX.HeroImage).First;
                row["ImageUrl"] = await img.IsVisibleAsync()
                    ? (await img.GetAttributeAsync("src") ?? "")
                    : (meta.TryGetValue("ImageUrl", out var mi) ? mi : "");
            }
            catch { row["ImageUrl"] = meta.TryGetValue("ImageUrl", out var mi) ? mi : ""; }

            // Price block (Base price, VAT, OTR, etc.)
            await ExtractPriceDetailsAsync(page2, row, summaryHeaders);

            summaryRows.Add(row);
            Console.WriteLine($"✅ Scraped {row.Count - 8} fields."); // minus base meta cols
        }

        // Write Phase 2 CSV (base + dynamic headers; put price fields in a sensible order)
        var baseSummaryOrder = new List<string> {
            "Car","BodyType","Model","Engine","SeriesCode","LineCode","ModelCode","ImageUrl","SummaryUrl",
            "PriceBeforeVAT","SelectedOptionsPrice","SubtotalExVAT","VATAmount","SubtotalInclVAT","OnTheRoadFee","OTRPrice"
        };
        var dynamicHeaders = summaryHeaders
            .Except(baseSummaryOrder, StringComparer.OrdinalIgnoreCase)
            .OrderBy(h => h)
            .ToList();

        var orderedSummaryHeaders = baseSummaryOrder.Concat(dynamicHeaders).ToList();

        using (var writer = new StreamWriter(DATA_CSV, append: false))
        {
            writer.WriteLine(string.Join(",", orderedSummaryHeaders));
            foreach (var row in summaryRows)
            {
                var line = string.Join(",", orderedSummaryHeaders.Select(h => row.ContainsKey(h) ? $"\"{row[h].Replace("\"", "\"\"")}\"" : ""));
                writer.WriteLine(line);
            }
        }
        Perf.EndGroup();
        Console.WriteLine($"✅ Phase 2: wrote {summaryRows.Count} rows to {DATA_CSV}");

        Console.WriteLine("✅ Done. Press Enter to close browser...");
        Console.ReadLine();
    }

    // =================== Phase 1: collect URLs ===================

    private static async Task<int> ScrapeTrimsAndEngines_Phase1_CollectUrls(
        IPage page,
        string carName,
        string bodyType,
        List<Dictionary<string, string>> urlRows,
        Dictionary<string, HashSet<string>> scrapedEnginesByModel)
    {
        // reveal UI
        for (int i = 0; i < 5; i++) { await page.Keyboard.PressAsync("PageDown"); await Task.Delay(120); }
        await WaitForConfiguratorReadyAsync(page);  // readiness wait

        var lineLabels = await ChangeEngine.GetAllLineLabelsAsync(page);

        // Fallback: if list isn't visible, use the currently selected line (if any)
        if (lineLabels.Count == 0)
        {
            var selected = await ChangeEngine.TryGetSelectedLineLabelAsync(page);
            if (!string.IsNullOrWhiteSpace(selected))
                lineLabels = new List<string> { selected };
        }

        Console.WriteLine($"🧩 Lines for {carName} [{bodyType}]: {string.Join(" | ", lineLabels)}");

        int totalSaved = 0;
        foreach (var lineLabel in lineLabels)
        {
            try
            {
                totalSaved += await ScrapeLine_Phase1_CollectUrls(
                    page, carName, bodyType, lineLabel, urlRows, scrapedEnginesByModel);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Line '{lineLabel}' failed: {ex.Message}");
                await ReturnToLineListAsync(page);
            }
        }
        return totalSaved;
    }

    private static async Task<int> ScrapeLine_Phase1_CollectUrls(
        IPage page,
        string carName,
        string bodyType,
        string lineLabel,
        List<Dictionary<string, string>> urlRows,
        Dictionary<string, HashSet<string>> scrapedEnginesByModel)
    {
        await ChangeEngine.CloseEngineModalIfOpenAsync(page);

        var tile = await ChangeEngine.FindLineTileByExactLabelAsync(page, lineLabel);
        if (tile is null) { Console.WriteLine($"⏭️ Line not found: {lineLabel}"); return 0; }

        if (!await ChangeEngine.TrySelectLineStrictAsync(page, tile, lineLabel, timeoutMs: 2500))
        {
            Console.WriteLine($"⏭️ Couldn’t select line (strict): {lineLabel}");
            return 0;
        }

        await ChangeEngine.HandleConfigureInCurrentTabDialogAsync(page);

        int saved = 0;

        if (await ChangeEngine.IsChangeEngineUsableAsync(page))
        {
            saved += await ChangeEngine.ScrapeViaChangeEngineForSelectedLine_Phase1(
                page, carName, bodyType, lineLabel, urlRows, scrapedEnginesByModel);
        }

        if (saved == 0)
        {
            saved += await ScrapeClassicForSelectedLine_Phase1(
                page, carName, bodyType, lineLabel, urlRows, scrapedEnginesByModel);
        }

        if (saved == 0 && await ChangeEngine.IsChangeEngineUsableAsync(page))
        {
            saved += await ChangeEngine.ScrapeViaChangeEngineForSelectedLine_Phase1(
                page, carName, bodyType, lineLabel, urlRows, scrapedEnginesByModel);
        }

        await ReturnToLineListAsync(page);
        Console.WriteLine(saved > 0
            ? $"✅ '{lineLabel}' scraped: {saved} engine(s)."
            : $"⚠️ '{lineLabel}' yielded 0 engines.");

        return saved;
    }

    private static async Task<int> ScrapeClassicForSelectedLine_Phase1(
        IPage page,
        string carName,
        string bodyType,
        string lineLabel,
        List<Dictionary<string, string>> urlRows,
        Dictionary<string, HashSet<string>> scrapedEnginesByModel)
    {
        int saved = 0;
        var engineTiles = page.Locator("div.selection-tile.checkbox-wrapper:has(p.product-name)");
        try { await engineTiles.First.WaitForAsync(new() { Timeout = 6000 }); }
        catch { Console.WriteLine($"⏭️ No classic engines visible for '{lineLabel}'."); return 0; }

        if (!scrapedEnginesByModel.ContainsKey(lineLabel))
            scrapedEnginesByModel[lineLabel] = new HashSet<string>();
        var seenEnginesForLine = scrapedEnginesByModel[lineLabel];

        int count = await engineTiles.CountAsync();
        for (int e = 0; e < count; e++)
        {
            engineTiles = page.Locator("div.selection-tile.checkbox-wrapper:has(p.product-name)");
            var engine = engineTiles.Nth(e);

            string engineName = (await engine.Locator("p.product-name").InnerTextAsync()).Trim();
            await engine.ClickAsync();
            await ChangeEngine.HandleConfigureInCurrentTabDialogAsync(page);

            var (configureUrl, summaryUrl, modelCodeGuess) = await GetConfigAndSummaryUrlsAsync(page);
            var (seriesCode, lineCode) = ExtractSeriesAndLineFromConfigure(configureUrl);

            // De-dupe by LineCode first
            if (!string.IsNullOrWhiteSpace(lineCode) && !SeenLineCodes.Add(lineCode))
            {
                Console.WriteLine($"⏭️ Duplicate line code {lineCode} — skipping.");
                continue;
            }
            // Also de-dupe by URL host+path
            var urlKey = BestUrlKey(configureUrl, summaryUrl);
            if (!string.IsNullOrEmpty(urlKey) && !SeenUrlKeys.Add(urlKey))
            {
                Console.WriteLine($"⏭️ Duplicate URL (skipped): {urlKey}");
                continue;
            }

            var row = new Dictionary<string, string>
            {
                ["Car"] = carName,
                ["BodyType"] = bodyType,
                ["Model"] = lineLabel,
                ["Engine"] = engineName,
                ["SeriesCode"] = seriesCode,
                ["LineCode"] = lineCode,
                ["ModelCode"] = string.IsNullOrWhiteSpace(lineCode) ? modelCodeGuess : lineCode,
                ["ConfigureUrl"] = configureUrl,
                ["SummaryUrl"] = summaryUrl
            };

            try
            {
                var heroImage = page.Locator(UX.HeroImage).First;
                row["ImageUrl"] = await heroImage.IsVisibleAsync() ? (await heroImage.GetAttributeAsync("src") ?? "") : "";
            }
            catch { row["ImageUrl"] = ""; }

            urlRows.Add(row);
            seenEnginesForLine.Add(engineName);
            saved++;

            var printCode = string.IsNullOrWhiteSpace(lineCode) ? modelCodeGuess : lineCode;
            Console.WriteLine($"✅ Saved (classic): {lineLabel} - {engineName} [{printCode}]");
            if (!string.IsNullOrEmpty(configureUrl)) Console.WriteLine($"   ↳ Configure: {configureUrl}");
            if (!string.IsNullOrEmpty(summaryUrl))   Console.WriteLine($"   ↳ Summary:   {summaryUrl}");
        }

        return saved;
    }

    // ===== URL helpers: capture configure URL and forge summary URL =====
    private static async Task<(string configureUrl, string summaryUrl, string modelCode)> GetConfigAndSummaryUrlsAsync(IPage page)
    {
        string found = await page.EvaluateAsync<string>(@"() => {
            const picks = new Set();
            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.getAttribute('href') || '';
                const full = a.href || '';
                if (href.includes('/configure/') || href.includes('/summary/')) picks.add(a.href);
                if (full.includes('/configure/') || full.includes('/summary/')) picks.add(full);
            });
            const can = document.querySelector('link[rel=""canonical""]');
            if (can && can.href) picks.add(can.href);
            const og = document.querySelector('meta[property=""og:url""]');
            if (og && og.content) picks.add(og.content);
            const arr = Array.from(picks);
            let cfg = arr.find(u => u.includes('/configure/')) || '';
            let sum = arr.find(u => u.includes('/summary/')) || '';
            return cfg || sum || '';
        }");

        string configureUrl = "";
        if (!string.IsNullOrEmpty(found))
        {
            if (found.Contains("/summary/")) found = found.Replace("/summary/", "/configure/");
            try { var u = new Uri(found); configureUrl = $"https://configure.bmw.co.uk{u.PathAndQuery}"; }
            catch { configureUrl = found; }
        }
        else
        {
            var cur = page.Url;
            if (cur.Contains("/configure/"))
            {
                try { var u = new Uri(cur); configureUrl = $"https://configure.bmw.co.uk{u.PathAndQuery}"; }
                catch { configureUrl = cur; }
            }
        }

        string summaryUrl = "";
        if (!string.IsNullOrWhiteSpace(configureUrl))
            summaryUrl = configureUrl.Replace("/configure/", "/summary/");
        else
        {
            var m = Regex.Match(page.Url, @"configurator\/[^\/]+\/([^\/]+)\/");
            var mc = m.Success ? m.Groups[1].Value : "";
            if (!string.IsNullOrWhiteSpace(mc))
                summaryUrl = $"https://www.bmw.co.uk/en/configurator/summary/en_GB/{mc}/";
        }

        string guessedModelCode = TryGuessModelCodeFromConfigure(configureUrl);
        if (string.IsNullOrWhiteSpace(guessedModelCode))
        {
            var m2 = Regex.Match(summaryUrl, @"/summary/en_GB/([^/]+)/");
            if (m2.Success) guessedModelCode = m2.Groups[1].Value;
        }

        if (string.IsNullOrEmpty(configureUrl))
            Console.WriteLine("⚠️ No configure URL found in DOM; used fallback (if any).");

        return (configureUrl, summaryUrl, guessedModelCode);
    }

    private static string TryGuessModelCodeFromConfigure(string configureUrl)
    {
        if (string.IsNullOrWhiteSpace(configureUrl)) return "";
        try
        {
            var u = new Uri(configureUrl);
            var segs = u.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (segs.Length >= 6)
            {
                var last = segs[^1];
                if (Regex.IsMatch(last, @"^[A-Z]{2}\d{6}$")) return last; // e.g., SE000001
                var line = segs[3];
                if (Regex.IsMatch(line, @"^[A-Z0-9]{3,6}$")) return line; // e.g., IX22 / IXSC
            }
        }
        catch { }
        return "";
    }

    // =============== Phase 2 helpers (summary page) ===================

    private static string CleanSummaryUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return url;
        try
        {
            var u = new Uri(url);
            return $"{u.Scheme}://{u.Host}{u.AbsolutePath}";
        }
        catch
        {
            var q = url.IndexOf('?');
            return q >= 0 ? url.Substring(0, q) : url;
        }
    }

    private static async Task<bool> GotoSummaryAsync(IPage page, string url)
    {
        try
        {
            await page.GotoAsync(url, new() { WaitUntil = WaitUntilState.DOMContentLoaded, Timeout = 60000 });
            return true;
        }
        catch
        {
            try
            {
                await page.GotoAsync(url, new() { WaitUntil = WaitUntilState.Load, Timeout = 60000 });
                return true;
            }
            catch { return false; }
        }
    }

    private static async Task ExpandAllAccordionsAsync(IPage page)
    {
        try
        {
            var accs = page.Locator(UX.SummaryAccordions);
            int count = 0;
            try { count = await accs.CountAsync(); } catch { }
            for (int i = 0; i < count; i++)
            {
                var acc = accs.Nth(i);
                try { await acc.Locator("[slot='header']").ClickAsync(); await Task.Delay(100); } catch { }
            }
        }
        catch { }
    }

    private static async Task ScrollToBottomAsync(IPage page, int steps = 12, int delayMs = 150)
    {
        for (int i = 0; i < steps; i++)
        {
            try { await page.EvaluateAsync("() => window.scrollBy(0, Math.floor(window.innerHeight*0.9))"); } catch { }
            await Task.Delay(delayMs);
        }
    }

    // --- Price section & derived totals ---
    private static async Task ExtractPriceDetailsAsync(IPage page, Dictionary<string, string> row, HashSet<string> headers)
    {
        var labelLoc = page.Locator("#priceSection .price-label p");
        var valueLoc = page.Locator("#priceSection .price-value");
        int nLabels = 0, nValues = 0;
        try { nLabels = await labelLoc.CountAsync(); } catch { }
        try { nValues = await valueLoc.CountAsync(); } catch { }
        int k = Math.Min(nLabels, nValues);
        if (k == 0) return;

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < k; i++)
        {
            string label = (await labelLoc.Nth(i).InnerTextAsync()).Trim();
            label = Regex.Replace(label, @"\s+", " ");
            string value = (await valueLoc.Nth(i).InnerTextAsync()).Trim();
            map[label] = value;
        }

        string Get(string contains) =>
            map.FirstOrDefault(kv => kv.Key.IndexOf(contains, StringComparison.OrdinalIgnoreCase) >= 0).Value ?? "";

        decimal basePrice = ParseMoney(Get("Base price"));
        decimal selectedOptions = ParseMoney(Get("Selected optional equipment"));
        decimal vat = ParseMoney(Get("VAT"));
        decimal otrFee = ParseMoney(Get("On the Road Fee"));
        decimal otrPrice = ParseMoney(Get("OTR price"));

        decimal subtotalExVat = basePrice + selectedOptions;
        decimal subtotalInclVat = subtotalExVat + vat;

        row["PriceBeforeVAT"]       = FormatGBP(basePrice);
        row["SelectedOptionsPrice"] = FormatGBP(selectedOptions);
        row["SubtotalExVAT"]        = FormatGBP(subtotalExVat);
        row["VATAmount"]            = FormatGBP(vat);
        row["SubtotalInclVAT"]      = FormatGBP(subtotalInclVat);
        row["OnTheRoadFee"]         = FormatGBP(otrFee);
        row["OTRPrice"]             = FormatGBP(otrPrice);

        foreach (var h in new [] { "PriceBeforeVAT","SelectedOptionsPrice","SubtotalExVAT","VATAmount","SubtotalInclVAT","OnTheRoadFee","OTRPrice" })
            headers.Add(h);
    }

    private static decimal ParseMoney(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return 0m;
        var cleaned = Regex.Replace(s, @"[^\d\.\-]", "");
        if (decimal.TryParse(cleaned, NumberStyles.Number | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var v))
            return v;
        return 0m;
    }

    private static string FormatGBP(decimal v)
        => v.ToString("C0", CultureInfo.GetCultureInfo("en-GB"));

    private static async Task AcceptCookiesIfPresentAsync(IPage page)
    {
        try
        {
            var acceptButton = page.Locator(
                "#onetrust-accept-btn-handler, " +
                "button.accept-button:has-text('Accept all'), " +
                "button:has-text('Accept all'), button:has-text('Accept All'), " +
                "#truste-consent-button"
            );
            if (await acceptButton.IsVisibleAsync())
            {
                await acceptButton.ClickAsync();
                Console.WriteLine("✅ Accepted cookies.");
            }
        }
        catch { /* ignore */ }
    }

    private static async Task SafeReturnToGrid(IPage page, string baseCardSel)
    {
        try
        {
            var logo = page.Locator(UX.LogoLink);
            if (await logo.IsVisibleAsync())
            {
                await logo.ClickAsync();
                Console.WriteLine("🔙 Returned via BMW logo.");
            }
        }
        catch { /* ignore */ }

        var cards = page.Locator(baseCardSel);
        try
        {
            await cards.First.WaitForAsync(new() { Timeout = UX.Long });
        }
        catch
        {
            Console.WriteLine("🔁 Light reload of All Models page...");
            await page.GotoAsync("https://www.bmw.co.uk/en/all-models.html",
                new() { WaitUntil = WaitUntilState.DOMContentLoaded, Timeout = UX.XL });
            await page.Locator(baseCardSel).First.WaitForAsync(new() { Timeout = UX.Long });
        }
    }

    // ================== Basic grid parsers & line navigation ====================

    private static async Task<string> ExtractModelNameFromCardAsync(ILocator card)
    {
        var preferredSelectors = new[]
        {
            ".cmp-allmodelscarddetail__series",
            ".cmp-allmodelscarddetail__title",
            ".cmp-title__text",
            "h2", "h3"
        };

        foreach (var sel in preferredSelectors)
        {
            var loc = card.Locator(sel);
            if (await loc.CountAsync() > 0)
            {
                var text = (await loc.First.InnerTextAsync()).Trim();
                if (!string.IsNullOrWhiteSpace(text)) return text;
            }
        }

        var allText = (await card.InnerTextAsync()).Trim();
        var lines = allText.Split('\n').Select(s => s.Trim()).Where(s =>
            !string.IsNullOrWhiteSpace(s) &&
            !Regex.IsMatch(s, @"^(SUV|Saloon|Touring|Gran Coup[eé]|Coup[eé]|Convertible|M Model|New)$", RegexOptions.IgnoreCase) &&
            !Regex.IsMatch(s, @"^From\s*£", RegexOptions.IgnoreCase) &&
            !string.Equals(s, "Electric", StringComparison.OrdinalIgnoreCase)
        );
        return lines.FirstOrDefault() ?? "";
    }

    private static async Task<string> ExtractBodyTypeFromCardAsync(ILocator card)
    {
        var tagSelectors = new[]
        {
            ".cmp-allmodelscarddetail__tags",
            ".cmp-allmodelscarddetail__category",
            ".cmp-tag",
            ".cmp-title__eyebrow"
        };

        foreach (var sel in tagSelectors)
        {
            var loc = card.Locator(sel);
            if (await loc.CountAsync() > 0)
            {
                var t = (await loc.First.InnerTextAsync()).Trim();
                var bt = FindKnownBodyTypeInText(t);
                if (!string.IsNullOrWhiteSpace(bt)) return bt;
            }
        }

        var full = (await card.InnerTextAsync()).Trim();
        return FindKnownBodyTypeInText(full) ?? "";
    }

    private static string? FindKnownBodyTypeInText(string text)
    {
        var known = new[] { "SUV", "Saloon", "Touring", "Gran Coupé", "Gran Coupe", "Coupé", "Coupe", "Convertible" };
        foreach (var candidate in known)
        {
            if (Regex.IsMatch(text, $@"\b{Regex.Escape(candidate)}\b", RegexOptions.IgnoreCase))
                return NormalizeBodyTypeLabel(candidate);
        }
        return null;
    }

    private static string NormalizeBodyTypeLabel(string label)
        => label switch { "Gran Coupe" => "Gran Coupé", "Coupe" => "Coupé", _ => label };

    private static async Task ReturnToLineListAsync(IPage page)
    {
        try { await page.Locator(UX.LinesList).First.WaitForAsync(new() { Timeout = 1500 }); return; } catch { }

        var back = page.Locator(UX.BackToConfig);
        if (await back.IsVisibleAsync())
        {
            await back.ClickAsync();
            try { await page.Locator(UX.LinesList).First.WaitForAsync(new() { Timeout = 4000 }); } catch { }
            return;
        }

        try { await page.GoBackAsync(); } catch { }
        try { await page.Locator(UX.LinesList).First.WaitForAsync(new() { Timeout = 4000 }); } catch { }
    }

    private static string NormalizeKey(string key)
    {
        var keyMap = new Dictionary<string, string>
        {
            { "battery size value", "Battery Capacity" },
            { "charging time ac short value", "AC Charging Time" },
            { "emission wlt", "CO2 Emissions" },
            { "charging time dc short value", "DC Charging Time" },
            { "td_weight_permitted_axle_load_front_rear_value", "Permitted Axle Load (Front/Rear)" }
        };
        string normalizedKey = Regex.Replace(key.ToLowerInvariant(), @"[_\s\-]+", " ").Trim().Replace("  ", " ");
        if (keyMap.TryGetValue(normalizedKey, out string? newHeader)) return newHeader;
        string cleanedKey = normalizedKey.Replace("td ", "").Replace(" value", "");
        var textInfo = new CultureInfo("en-US", false).TextInfo;
        return textInfo.ToTitleCase(cleanedKey);
    }

    private static string Sanitize(string dirty) => Regex.Replace(dirty, @"[\s-]", "").ToLower();
    private static string MakeModelBodyKey(string modelName, string bodyType)
        => $"{Sanitize(modelName)}|{Sanitize(bodyType)}";

    // ---------- Configurator readiness wait ----------
    private static async Task WaitForConfiguratorReadyAsync(IPage page, int timeoutMs = 10000)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            try
            {
                if (await page.Locator(UX.LinesList).CountAsync() > 0) return;
                if (await page.Locator("con-tile-line").CountAsync() > 0) return;
                if (await page.Locator(UX.ChangeEngineBtn).IsVisibleAsync()) return;
                if (await page.Locator("div.selection-tile.checkbox-wrapper:has(p.product-name)").CountAsync() > 0) return;
            }
            catch { /* ignore */ }
            await Task.Delay(150);
        }
    }

    // ====================== ChangeEngine (UI helpers) ==========================
    class ChangeEngine
    {
        public static async Task<List<string>> GetAllLineLabelsAsync(IPage page)
        {
            var labels = new List<string>();

            var candidates = new[]
            {
                "con-lines-list .con-tile-line",
                "con-lines-list con-tile-line",
                ".con-tile-line",
                "con-tile-line",
            };

            ILocator tiles = page.Locator(UX.LinesList);
            int count = await tiles.CountAsync();
            if (count == 0)
            {
                foreach (var sel in candidates)
                {
                    tiles = page.Locator(sel);
                    count = await tiles.CountAsync();
                    if (count > 0) break;
                }
            }

            for (int i = 0; i < count; i++)
            {
                var tile = tiles.Nth(i);
                string text = "";
                try
                {
                    var name = tile.Locator("p.line-name, .line-name, [data-test-id='line-name']");
                    if (await name.CountAsync() > 0)
                        text = (await name.First.InnerTextAsync())?.Trim() ?? "";
                    if (string.IsNullOrWhiteSpace(text))
                        text = (await tile.InnerTextAsync())?.Trim() ?? "";
                }
                catch { }

                if (!string.IsNullOrWhiteSpace(text))
                {
                    text = text.Split('\n').Select(s => s.Trim()).FirstOrDefault(s => !string.IsNullOrWhiteSpace(s)) ?? "";
                    if (!string.IsNullOrWhiteSpace(text)) labels.Add(text);
                }
            }

            return labels;
        }

        public static async Task<ILocator?> FindLineTileByExactLabelAsync(IPage page, string label)
        {
            var selectors = new[]
            {
                UX.LinesList,
                "con-lines-list con-tile-line",
                ".con-tile-line",
                "con-tile-line"
            };

            foreach (var sel in selectors)
            {
                var tiles = page.Locator(sel);
                int count = await tiles.CountAsync();
                for (int i = 0; i < count; i++)
                {
                    var tile = tiles.Nth(i);
                    try
                    {
                        var nameEl = tile.Locator("p.line-name, .line-name, [data-test-id='line-name']");
                        string? text = null;
                        if (await nameEl.CountAsync() > 0) text = (await nameEl.First.InnerTextAsync())?.Trim();
                        if (string.IsNullOrWhiteSpace(text)) text = (await tile.InnerTextAsync())?.Split('\n').FirstOrDefault()?.Trim();

                        if (!string.IsNullOrWhiteSpace(text) &&
                            string.Equals(text, label, StringComparison.OrdinalIgnoreCase))
                        {
                            return tile;
                        }
                    }
                    catch { }
                }
            }
            return null;
        }

        public static async Task<string?> TryGetSelectedLineLabelAsync(IPage page)
        {
            var selCandidates = new[]
            {
                ".con-tile-line.selected",
                ".con-tile-line[aria-pressed='true']",
                "con-tile-line.selected",
            };

            foreach (var sc in selCandidates)
            {
                var el = page.Locator(sc).First;
                if (await el.CountAsync() > 0)
                {
                    try
                    {
                        var name = el.Locator("p.line-name, .line-name, [data-test-id='line-name']");
                        if (await name.CountAsync() > 0)
                            return (await name.First.InnerTextAsync())?.Trim();
                        return (await el.InnerTextAsync())?.Split('\n').FirstOrDefault()?.Trim();
                    }
                    catch { }
                }
            }
            return null;
        }

        public static async Task<bool> IsLineSelectedAsync(ILocator lineTile)
        {
            try
            {
                var cls = (await lineTile.GetAttributeAsync("class")) ?? "";
                if (cls.Contains("selected", StringComparison.OrdinalIgnoreCase)) return true;

                var ariaPressed = await lineTile.GetAttributeAsync("aria-pressed");
                if (string.Equals(ariaPressed, "true", StringComparison.OrdinalIgnoreCase)) return true;
            }
            catch { }
            return false;
        }

        public static async Task<bool> TrySelectLineStrictAsync(
            IPage page,
            ILocator lineTile,
            string label,
            int timeoutMs = 1250)
        {
            if (await IsTileObviouslyUnselectableAsync(lineTile))
            {
                Console.WriteLine($"⏭️ '{label}' looks unselectable (disabled) — skipping.");
                return false;
            }

            var sw = Stopwatch.StartNew();

            for (int attempt = 1; attempt <= 4; attempt++)
            {
                if (await IsLineSelectedAsync(lineTile))
                {
                    Console.WriteLine($"✅ '{label}' already selected.");
                    return true;
                }

                try { await lineTile.ScrollIntoViewIfNeededAsync(); } catch { }

                bool clicked = false;

                try
                {
                    var hotspot = lineTile.Locator(".price-click-area, .tile-detail-focus-area, [role='button']");
                    if (await hotspot.IsVisibleAsync())
                    {
                        await hotspot.ClickAsync();
                        clicked = true;
                    }
                }
                catch { }

                if (!clicked) { try { await lineTile.ClickAsync(); clicked = true; } catch { } }
                if (!clicked) { try { await lineTile.ClickAsync(new() { Force = true }); clicked = true; } catch { } }

                if (!clicked)
                {
                    try
                    {
                        await lineTile.FocusAsync();
                        await page.Keyboard.PressAsync("Enter");
                        await page.Keyboard.PressAsync("Space");
                        clicked = true;
                    }
                    catch { }
                }

                await HandleConfigureInCurrentTabDialogAsync(page);
                await Task.Delay(150);

                if (await IsLineSelectedAsync(lineTile))
                {
                    Console.WriteLine($"✅ Selected line: {label}");
                    return true;
                }

                if (sw.ElapsedMilliseconds >= timeoutMs)
                {
                    Console.WriteLine($"⏭️ '{label}' did not become selected within {timeoutMs}ms — skipping.");
                    return false;
                }

                await Task.Delay(150);
            }

            Console.WriteLine($"⏭️ Could not select '{label}' — skipping.");
            return false;
        }

        public static async Task<bool> IsTileObviouslyUnselectableAsync(ILocator lineTile)
        {
            try
            {
                var cls = (await lineTile.GetAttributeAsync("class")) ?? "";
                if (cls.Contains("disabled", StringComparison.OrdinalIgnoreCase)) return true;

                var btn = lineTile.Locator(".price-click-area, .tile-detail-focus-area, [role='button']");
                if (await btn.IsVisibleAsync())
                {
                    var ariaDisabled = await btn.GetAttributeAsync("aria-disabled");
                    var disabled = await btn.GetAttributeAsync("disabled");
                    if (string.Equals(ariaDisabled, "true", StringComparison.OrdinalIgnoreCase)) return true;
                    if (!string.IsNullOrEmpty(disabled)) return true;
                }
            }
            catch { }
            return false;
        }

        public static async Task<bool> IsChangeEngineUsableAsync(IPage page)
        {
            try
            {
                var btn = page.Locator(UX.ChangeEngineBtn + ", button:has-text('Change engine'), button:has-text('Change powertrain')");
                if (!await btn.IsVisibleAsync()) return false;
                var disabled = await btn.GetAttributeAsync("disabled");
                return string.IsNullOrEmpty(disabled);
            }
            catch { return false; }
        }

        private static async Task<bool> IsEngineModalOpenAsync(IPage page)
        {
            try
            {
                if (await page.Locator(UX.EngineModalTile).First.IsVisibleAsync()) return true;
                if (await page.Locator("con-modal-logic").IsVisibleAsync()) return true;
            }
            catch { }
            return false;
        }

        public static async Task CloseEngineModalIfOpenAsync(IPage page)
        {
            if (!await IsEngineModalOpenAsync(page)) return;

            Console.WriteLine("🛑 Engine chooser open — closing before selecting a line.");
            try
            {
                var closeBtn = page.Locator("button[aria-label='Close'], con-modal-logic button:has-text('Close')");
                if (await closeBtn.IsVisibleAsync()) await closeBtn.ClickAsync();
                else await page.Keyboard.PressAsync("Escape");
            }
            catch { }

            try { await page.Locator(UX.EngineModalTile).First.WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 1000 }); } catch { }
            try { await page.Locator("con-modal-logic").WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 500 }); } catch { }
        }

        public static async Task HandleConfigureInCurrentTabDialogAsync(IPage page)
        {
            var btn = page.Locator(UX.ConfigureInTab);

            try
            {
                if (await btn.IsVisibleAsync())
                {
                    Console.WriteLine("✅ Clicked 'Configure in current tab' dialog (instant).");
                    try { await btn.ClickAsync(); } catch { }
                    try { await page.Locator("con-modal-logic").WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 1200 }); } catch { }
                    return;
                }
            }
            catch { }

            var readySelectors = new[] { UX.LinesList, UX.EngineModalTile, UX.SummaryBtn, UX.ChangeEngineBtn };
            var deadline = Stopwatch.StartNew();
            while (deadline.ElapsedMilliseconds < 2000)
            {
                bool clicked = false;
                try
                {
                    if (await btn.IsVisibleAsync())
                    {
                        Console.WriteLine("✅ Clicked 'Configure in current tab' dialog.");
                        try { await btn.ClickAsync(); } catch { }
                        try { await page.Locator("con-modal-logic").WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 1200 }); } catch { }
                        clicked = true;
                    }
                }
                catch { }
                try
                {
                    if (await WaitForAnyVisibleAsync(page, readySelectors, 0)) return;
                }
                catch { }
                if (clicked) return;
                await Task.Delay(100);
            }
            try { if (await WaitForAnyVisibleAsync(page, readySelectors, 250)) return; } catch { }
        }

        // ============ Phase 1 modal scrapers (collect URLs only) ==============

        public static async Task<int> ScrapeViaChangeEngineForSelectedLine_Phase1(
            IPage page,
            string carName,
            string bodyType,
            string lineLabel,
            List<Dictionary<string, string>> urlRows,
            Dictionary<string, HashSet<string>> scrapedEnginesByModel)
        {
            int saved = 0;

            var changeEngineBtn = page.Locator(UX.ChangeEngineBtn + ", button:has-text('Change engine'), button:has-text('Change powertrain')");
            if (!await changeEngineBtn.IsVisibleAsync()) return 0;

            await changeEngineBtn.ClickAsync();
            await page.WaitForSelectorAsync(UX.EngineModalTile, new() { Timeout = 2000 });
            await EnsureEngineModalStableAsync(page);

            if (!scrapedEnginesByModel.ContainsKey(lineLabel))
                scrapedEnginesByModel[lineLabel] = new HashSet<string>();
            var seenEnginesForLine = scrapedEnginesByModel[lineLabel];

            var engineCards = page.Locator(UX.EngineModalTile);
            int engineCount = await engineCards.CountAsync();
            Console.WriteLine($"🔧 Modal shows {engineCount} engines for '{lineLabel}'.");

            int dupeStreak = 0;
            for (int e = 0; e < engineCount; e++)
            {
                engineCards = page.Locator(UX.EngineModalTile);
                var card = engineCards.Nth(e);

                string engineName = "Engine";
                try
                {
                    var nameEl = card.Locator("p.product-name, h3.engine-title");
                    if (await nameEl.IsVisibleAsync()) engineName = (await nameEl.InnerTextAsync()).Trim();
                }
                catch { }

                if (seenEnginesForLine.Contains(engineName))
                {
                    Console.WriteLine($"⏭️ Already scraped in this line: {engineName}");
                    dupeStreak++; if (dupeStreak >= 2) { Console.WriteLine("🧹 Mostly duplicates — moving on."); break; }
                    continue;
                }

                await card.ScrollIntoViewIfNeededAsync();
                bool clicked = await ClickWhenTopmostAsync(page, card, 1750);
                if (!clicked)
                {
                    Console.WriteLine("🧯 Couldn’t click engine — hard reset chooser.");
                    await HardResetEngineChooserAsync(page);
                    continue;
                }

                var continueBtn = page.Locator(UX.ContinueBtn);
                if (await continueBtn.IsVisibleAsync())
                {
                    await continueBtn.ClickAsync();
                    await HandleConfigureInCurrentTabDialogAsync(page);
                }

                var (configureUrl, summaryUrl, modelCode) = await BMWCARS.GetConfigAndSummaryUrlsAsync(page);
                var (seriesCode, lineCode) = BMWCARS.ExtractSeriesAndLineFromConfigure(configureUrl);

                if (!string.IsNullOrWhiteSpace(lineCode) && !BMWCARS.SeenLineCodes.Add(lineCode))
                {
                    Console.WriteLine($"⏭️ Duplicate line code {lineCode} — skipping.");
                    dupeStreak++; await TryReopenChooserAsync(page); if (dupeStreak >= 2) break; else continue;
                }

                var urlKey = BMWCARS.BestUrlKey(configureUrl, summaryUrl);
                if (!string.IsNullOrEmpty(urlKey) && !BMWCARS.SeenUrlKeys.Add(urlKey))
                {
                    Console.WriteLine($"⏭️ Duplicate URL (skipped): {urlKey}");
                    dupeStreak++; await TryReopenChooserAsync(page); if (dupeStreak >= 2) break; else continue;
                }

                var row = new Dictionary<string, string>
                {
                    ["Car"] = carName,
                    ["BodyType"] = bodyType,
                    ["Model"] = lineLabel,
                    ["Engine"] = engineName,
                    ["SeriesCode"] = seriesCode,
                    ["LineCode"] = lineCode,
                    ["ModelCode"] = string.IsNullOrWhiteSpace(lineCode) ? modelCode : lineCode,
                    ["ConfigureUrl"] = configureUrl,
                    ["SummaryUrl"] = summaryUrl
                };

                try
                {
                    var heroImage = page.Locator(UX.HeroImage).First;
                    row["ImageUrl"] = await heroImage.IsVisibleAsync() ? (await heroImage.GetAttributeAsync("src") ?? "") : "";
                }
                catch { row["ImageUrl"] = ""; }

                urlRows.Add(row);
                seenEnginesForLine.Add(engineName);
                saved++;
                dupeStreak = 0;

                var codeForPrint = string.IsNullOrWhiteSpace(lineCode) ? modelCode : lineCode;
                Console.WriteLine($"✅ Saved: {lineLabel} - {engineName} [{codeForPrint}]");
                if (!string.IsNullOrEmpty(configureUrl)) Console.WriteLine($"   ↳ Configure: {configureUrl}");
                if (!string.IsNullOrEmpty(summaryUrl))   Console.WriteLine($"   ↳ Summary:   {summaryUrl}");

                await TryReopenChooserAsync(page);
            }

            await CloseEngineModalIfOpenAsync(page);
            return saved;
        }

        // ------------------- shared tiny helpers -------------------
        private static async Task<string?> TryGetChildDataAttr(ILocator root, string selector, string attr)
        {
            try
            {
                var el = root.Locator(selector).First;
                if (await el.IsVisibleAsync())
                {
                    var v = await el.GetAttributeAsync(attr);
                    if (!string.IsNullOrWhiteSpace(v)) return v;
                }
            }
            catch { }
            return null;
        }

        public static async Task EnsureEngineModalStableAsync(IPage page)
        {
            try
            {
                int prev = await page.Locator(UX.EngineModalTile).CountAsync();
                var sw = Stopwatch.StartNew();
                while (sw.ElapsedMilliseconds < 1000)
                {
                    await Task.Delay(200);
                    int cur = await page.Locator(UX.EngineModalTile).CountAsync();
                    if (cur > 0 && cur == prev) return;
                    prev = cur;
                }
            }
            catch { }
        }

        public static async Task<bool> WaitForAnyVisibleAsync(IPage page, string[] selectors, int timeoutMs)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                try
                {
                    foreach (var sel in selectors)
                    {
                        var loc = page.Locator(sel);
                        if (await loc.IsVisibleAsync()) return true;
                    }
                }
                catch { }
                await Task.Delay(150);
            }
            return false;
        }

        private static async Task<bool> IsTopmostAsync(IPage page, ILocator el)
        {
            var box = await el.BoundingBoxAsync();
            if (box == null) return false;

            float cx = box.X + box.Width / 2f;
            float cy = box.Y + box.Height / 2f;

            return await page.EvaluateAsync<bool>(@"({x,y}) => {
                const top = document.elementFromPoint(x, y);
                if (!top) return false;
                return !!top.closest('con-tile-engine.engine-tile');
            }", new { x = (double)cx, y = (double)cy });
        }

        public static async Task<bool> ClickWhenTopmostAsync(IPage page, ILocator tile, int maxWaitMs)
        {
            var sw = Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds < maxWaitMs)
            {
                await EnsureEngineModalStableAsync(page);
                await HandleConfigureInCurrentTabDialogAsync(page);

                if (await IsTopmostAsync(page, tile))
                {
                    try { await tile.ClickAsync(); return true; }
                    catch (PlaywrightException ex) when (ex.Message.Contains("intercepts pointer events", StringComparison.OrdinalIgnoreCase)) { }
                }

                await Task.Delay(120);
            }

            var box = await tile.BoundingBoxAsync();
            if (box != null)
            {
                try
                {
                    float cx = box.X + box.Width / 2f;
                    float cy = box.Y + box.Height / 2f;

                    await page.Mouse.MoveAsync(cx, cy);
                    await page.Mouse.DownAsync();
                    await page.Mouse.UpAsync();
                    return true;
                }
                catch { }
            }
            return false;
        }

        public static async Task HardResetEngineChooserAsync(IPage page)
        {
            await CloseEngineModalIfOpenAsync(page);
            var changeEngineBtn = page.Locator(UX.ChangeEngineBtn + ", button:has-text('Change engine'), button:has-text('Change powertrain')");
            if (await changeEngineBtn.IsVisibleAsync())
            {
                await changeEngineBtn.ClickAsync();
                await page.WaitForSelectorAsync(UX.EngineModalTile, new() { Timeout = 3000 });
                await EnsureEngineModalStableAsync(page);
                await HandleConfigureInCurrentTabDialogAsync(page);
            }
        }

        public static async Task TryReopenChooserAsync(IPage page)
        {
            var reopenBtn = page.Locator(UX.ChangeEngineBtn + ", button:has-text('Change engine'), button:has-text('Change powertrain')");
            if (await reopenBtn.IsVisibleAsync())
            {
                await reopenBtn.ClickAsync();
                await page.WaitForSelectorAsync(UX.EngineModalTile, new() { Timeout = 3000 });
                await EnsureEngineModalStableAsync(page);
                await HandleConfigureInCurrentTabDialogAsync(page);
            }
            else
            {
                Console.WriteLine("⚠️ Couldn’t reopen engine chooser; ending modal loop.");
            }
        }
    }
}
