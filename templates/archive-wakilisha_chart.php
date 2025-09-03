<?php
if (!defined('ABSPATH')) exit;

get_header();

$format = sanitize_title(get_query_var('waki_chart_format'));
echo Waki_Charts::instance()->shortcode_charts_archive(['format' => $format]);
get_footer();
