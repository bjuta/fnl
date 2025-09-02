<?php
/**
 * Description: Multi-chart ingest via Playlists or Release Window search; scoring driven by normalized track.popularity; debut/peak (with dates), weekly deltas, recent-weeks viewer, per-track position history, dry-run pipeline with validation & transparency, archive UI, manual Artist Origin (ISO-2) mapping with per-chart origin filter, full artist meta storage, single-chart hero, and Artists admin with filters/CSV import/export/sync.
 * Plugin Name: Wakilisha Charts
 * Version:     2.3
 * Author:      WAKILISHA MUSIC GROUP
 * License:     GPLv2 or later
 * Text Domain: wakilisha-charts
 * Domain Path: /languages
 */

if (!defined('ABSPATH')) exit;

define('WAKI_CHARTS_PLUGIN_FILE', __FILE__);
define('WAKI_CHARTS_DIR', plugin_dir_path(__FILE__));

add_action('init', function(){
    load_plugin_textdomain('wakilisha-charts', false, basename(__DIR__).'/languages');
});

require_once WAKI_CHARTS_DIR . 'includes/class-waki-charts.php';

Waki_Charts::instance();
