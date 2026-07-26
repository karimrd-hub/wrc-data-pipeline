# Benchmark — `baseline`

- Range: `2025-05-01` → `2025-05-31`
- Bodies: `all four`

| phase | wall (mm:ss.ms) | records |
|---|---|---|
| `scraper_cold` | 04:16.92 | found=339, inserted=339, scraped=339 |
| `scraper_warm` | 01:17.76 | found=339, scraped=339, unchanged=339 |
| `transform_cold` | 00:08.17 | transformed=339 |
| `transform_warm` | 00:07.08 | unchanged=339 |
