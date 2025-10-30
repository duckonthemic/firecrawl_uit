$ErrorActionPreference = 'SilentlyContinue'

$pages = @('https://free-proxy-list.net/','https://www.us-proxy.org/','https://www.sslproxies.org/')
$proxies = @{}

foreach ($p in $pages) {
    try {
    Write-Host ("Fetching {0}" -f $p)
        $r = Invoke-WebRequest -Uri $p -UseBasicParsing -TimeoutSec 15
        $html = $r.Content
        # Extract table rows with IP and port
        $rx = [regex]'<td>(\d+\.\d+\.\d+\.\d+)</td>\s*<td>(\d+)</td>'
        foreach ($m in $rx.Matches($html)) {
            $ip = $m.Groups[1].Value
            $port = $m.Groups[2].Value
            $key = $ip + ':' + $port
            if (-not $proxies.ContainsKey($key)) { $proxies[$key] = @{ip=$ip;port=$port} }
        }
    } catch {
        Write-Host ("Failed fetch {0}: {1}" -f $p, $_)
    }
}

# fallback: try proxy-list.download format
try {
    $r2 = Invoke-WebRequest -Uri 'https://www.proxy-list.download/api/v1/get?type=http' -UseBasicParsing -TimeoutSec 10
    if ($r2.StatusCode -eq 200) {
        $lines = $r2.Content -split "\r?\n"
        foreach ($l in $lines) {
            if ($l -match '^(\d+\.\d+\.\d+\.\d+):(\d+)$') {
                $proxies[$l] = @{ip=$matches[1]; port=$matches[2]}
            }
        }
    }
} catch { }

$proxies_list = $proxies.Keys | Select-Object -First 40

$results = @()

foreach ($p in $proxies_list) {
    Write-Host ("Testing proxy {0}" -f $p)
    try {
        $start = Get-Date
        $resp = Invoke-RestMethod -Uri 'https://httpbin.org/ip' -Proxy ("http://{0}" -f $p) -TimeoutSec 10
        $elapsed = (Get-Date) - $start
        $results += [pscustomobject]@{proxy=$p; ok=$true; ms=[int]$elapsed.TotalMilliseconds; origin=($resp.origin -join ',')}
    } catch {
        $results += [pscustomobject]@{proxy=$p; ok=$false; ms=-1; origin=''}
    }
}

$valid = $results | Where-Object {$_.ok -eq $true} | Sort-Object ms | Select-Object -First 10

$valid | ConvertTo-Json -Depth 4 | Out-File -FilePath 'c:\\Users\\hoang\\Downloads\\uit_firecrawl_new\\firecrawl\\proxy_test_results.json' -Encoding utf8
$results | ConvertTo-Json -Depth 4 | Out-File -FilePath 'c:\\Users\\hoang\\Downloads\\uit_firecrawl_new\\firecrawl\\proxy_test_results_full.json' -Encoding utf8

Write-Host "Done. Results written to proxy_test_results.json and proxy_test_results_full.json" 
