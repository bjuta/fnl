<?php
if (!defined('ABSPATH')) exit;
get_header();
$format = sanitize_title(get_query_var('waki_chart_format'));
if (!$format) {
    get_footer();
    return;
}
$year  = isset($_GET['year']) ? intval($_GET['year']) : 0;
$paged = max(1, get_query_var('paged'));

global $wpdb;
$years = $wpdb->get_col(
    $wpdb->prepare(
        "SELECT DISTINCT YEAR(pm.meta_value)
         FROM {$wpdb->postmeta} pm
         INNER JOIN {$wpdb->postmeta} pf ON pm.post_id = pf.post_id
         INNER JOIN {$wpdb->posts} p ON p.ID = pm.post_id
         WHERE pm.meta_key = '_waki_week_start'
           AND pf.meta_key = '_waki_format'
           AND pf.meta_value = %s
           AND p.post_type = %s AND p.post_status = 'publish'
         ORDER BY YEAR(pm.meta_value) DESC",
        $format,
        Waki_Charts::CPT
    )
);

$args = [
    'post_type'      => Waki_Charts::CPT,
    'posts_per_page' => 20,
    'paged'          => $paged,
    'post_status'    => 'publish',
    'meta_key'       => '_waki_week_start',
    'orderby'        => 'meta_value',
    'order'          => 'DESC',
    'meta_query'     => [
        [ 'key' => '_waki_format', 'value' => $format ],
    ],
];
if ($year) {
    $args['meta_query'][] = [
        'key'     => '_waki_week_start',
        'value'   => [$year . '-01-01', $year . '-12-31'],
        'compare' => 'BETWEEN',
        'type'    => 'DATE',
    ];
}
$q = new WP_Query($args);
?>
<section class="waki-wrap waki-fw">
    <header class="waki-term-header">
        <h1><?php echo esc_html(ucwords(str_replace('-', ' ', $format))); ?></h1>
        <?php if ($years) : ?>
            <form method="get" id="waki-year-filter">
                <label for="waki-year-select"><?php esc_html_e('Year:', 'wakilisha-charts'); ?></label>
                <select id="waki-year-select" name="year" onchange="document.getElementById('waki-year-filter').submit();">
                    <option value=""><?php esc_html_e('All Years', 'wakilisha-charts'); ?></option>
                    <?php foreach ($years as $y) : ?>
                        <option value="<?php echo esc_attr($y); ?>"<?php selected($year, (int) $y); ?>><?php echo esc_html($y); ?></option>
                    <?php endforeach; ?>
                </select>
            </form>
        <?php endif; ?>
    </header>

    <?php waki_chart_calendar($format, $year ?: date('Y')); ?>

    <?php if ($q->have_posts()) : ?>
        <ul class="waki-term-list">
            <?php while ($q->have_posts()) : $q->the_post(); ?>
                <li><a href="<?php the_permalink(); ?>"><?php the_title(); ?></a></li>
            <?php endwhile; ?>
        </ul>

        <div class="waki-pager">
            <?php
            echo paginate_links([
                'total'   => $q->max_num_pages,
                'current' => $paged,
                'prev_text' => '«',
                'next_text' => '»',
            ]);
            ?>
        </div>
    <?php else : ?>
        <p><?php esc_html_e('No charts found.', 'wakilisha-charts'); ?></p>
    <?php endif; wp_reset_postdata(); ?>
</section>
<?php get_footer();
