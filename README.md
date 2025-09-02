# Waki Charts

## Plugin Purpose

Waki Charts is a WordPress plugin that ingests chart data from Spotify and
renders the results on your site.  It creates custom post types for weekly
charts and provides templates and assets for displaying them on the front end.

## Installation

1. Upload the plugin files to `wp-content/plugins/wakilisha-charts` or clone the
   repository into that directory.
2. Activate **Waki Charts** from the WordPress admin Plugins screen.
3. Visit the plugin settings page to finish the configuration.

## Spotify Credentials Configuration

The plugin requires a Spotify client to make API requests.

1. Create an application at [Spotify for Developers](https://developer.spotify.com).
2. Copy the **Client ID** and **Client Secret** provided by Spotify.
3. In WordPress, open **Settings → Waki Charts** and paste the credentials into
   the corresponding fields.
4. Save your changes.  The plugin will use these credentials to request an
   access token from Spotify.

## Running Updates

Chart data is fetched daily through the WordPress cron event
`waki_chart_ingest_daily`.  WordPress will schedule this automatically when the
plugin is activated.

To run an update manually, use WP‑CLI:

```bash
wp cron event run waki_chart_ingest_daily --due-now
```

## Dependencies

- WordPress 6.0 or later
- PHP 7.4+ with the cURL and OpenSSL extensions enabled
- A Spotify developer account and credentials
- WP‑CLI (optional, for manual updates)

## Common Error Resolutions

### Missing Spotify Credentials

If you see `Client ID/secret not configured`, ensure the Spotify credentials are
entered correctly on the plugin settings page.

### Connection Failures

`WP_Error` responses during updates usually indicate that the server cannot
reach Spotify. Confirm that outbound HTTPS requests are allowed and that the
PHP cURL and OpenSSL extensions are installed.

### Scheduled Updates Not Running

If charts are not updating, verify that WordPress cron is enabled. You can run
the update manually with the WP‑CLI command shown above or set up a server cron
job to trigger `wp cron event run` regularly.

