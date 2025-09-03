<?php
if (!defined('ABSPATH')) exit;

get_header();

$format = sanitize_title(get_query_var('waki_chart_format'));
$date   = sanitize_text_field(get_query_var('waki_chart_date'));
$has_chart = have_posts();

if ($has_chart) {
    the_post();
    $week   = sanitize_text_field(get_post_meta(get_the_ID(), '_waki_week_start', true));
    $format = sanitize_title(get_post_meta(get_the_ID(), '_waki_format', true)) ?: $format;
} else {
    $week = $date;
}

function waki_chart_exact($format, $week) {
    $posts = get_posts([
        'post_type'      => Waki_Charts::CPT,
        'post_status'    => 'publish',
        'posts_per_page' => 1,
        'fields'         => 'ids',
        'meta_query'     => [
            ['key' => '_waki_format', 'value' => $format],
            ['key' => '_waki_week_start', 'value' => $week],
        ],
    ]);
    return $posts ? $posts[0] : 0;
}

function waki_chart_adjacent($format, $week, $compare, $order) {
    $posts = get_posts([
        'post_type'      => Waki_Charts::CPT,
        'post_status'    => 'publish',
        'posts_per_page' => 1,
        'fields'         => 'ids',
        'meta_key'       => '_waki_week_start',
        'orderby'        => 'meta_value',
        'order'          => $order,
        'meta_query'     => [
            ['key' => '_waki_format', 'value' => $format],
            ['key' => '_waki_week_start', 'value' => $week, 'compare' => $compare, 'type' => 'DATE'],
        ],
    ]);
    return $posts ? $posts[0] : 0;
}

$prev_link = $next_link = '';
if ($format && $week) {
    $ts = strtotime($week);
    if ($has_chart && $ts) {
        $prev_id = waki_chart_exact($format, date('Y-m-d', $ts - WEEK_IN_SECONDS));
        $next_id = waki_chart_exact($format, date('Y-m-d', $ts + WEEK_IN_SECONDS));
    } else {
        $prev_id = waki_chart_adjacent($format, $week, '<', 'DESC');
        $next_id = waki_chart_adjacent($format, $week, '>', 'ASC');
    }
    $prev_link = $prev_id ? get_permalink($prev_id) : '';
    $next_link = $next_id ? get_permalink($next_id) : '';
}
?>
<section class="waki-wrap waki-fw">
    <?php if ($has_chart) : ?>
        <?php the_content(); ?>
    <?php else : ?>
        <p><?php esc_html_e('No edition this week.', 'wakilisha-charts'); ?></p>
    <?php endif; ?>

    <nav class="waki-week-nav">
        <?php if ($prev_link) : ?>
            <a class="waki-prev" href="<?php echo esc_url($prev_link); ?>">&laquo; <?php esc_html_e('Previous Week', 'wakilisha-charts'); ?></a>
        <?php endif; ?>
        <?php if ($next_link) : ?>
            <a class="waki-next" href="<?php echo esc_url($next_link); ?>"><?php esc_html_e('Next Week', 'wakilisha-charts'); ?> &raquo;</a>
        <?php endif; ?>
    </nav>
</section>
<?php get_footer();
