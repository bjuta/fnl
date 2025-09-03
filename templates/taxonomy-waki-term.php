<?php
/**
 * Generic template for WAKI taxonomy archives.
 * Lists chart editions for the queried term with year filter and pagination.
 */

if (!defined('ABSPATH')) exit;

get_header();

$term  = get_queried_object();
if (!$term) {
    get_footer();
    return;
}

$year_param_set = isset($_GET['year']);
$year  = $year_param_set ? intval($_GET['year']) : 0;
$format = isset($_GET['format']) ? sanitize_title($_GET['format']) : '';
$paged = max(1, get_query_var('paged'));

global $wpdb;
$years = $wpdb->get_col(
    $wpdb->prepare(
        "SELECT DISTINCT YEAR(pm.meta_value)
         FROM {$wpdb->postmeta} pm
         INNER JOIN {$wpdb->posts} p ON p.ID = pm.post_id
         INNER JOIN {$wpdb->term_relationships} tr ON p.ID = tr.object_id
         INNER JOIN {$wpdb->term_taxonomy} tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
         WHERE pm.meta_key = '_waki_week_start'
           AND tt.taxonomy = %s AND tt.term_id = %d
           AND p.post_type = %s AND p.post_status = 'publish'
         ORDER BY YEAR(pm.meta_value) DESC",
        $term->taxonomy,
        $term->term_id,
        Waki_Charts::CPT
    )
);

if (!$year_param_set && $years) {
    $year = (int) $years[0];
}

$formats = get_terms([
    'taxonomy'   => 'waki_format',
    'hide_empty' => false,
]);

$meta_query = [
    [ 'key' => '_waki_week_start', 'compare' => 'EXISTS' ],
];
if ($year) {
    $meta_query[] = [
        'key'     => '_waki_week_start',
        'value'   => [$year . '-01-01', $year . '-12-31'],
        'compare' => 'BETWEEN',
        'type'    => 'DATE',
    ];
}
if ($format) {
    $meta_query[] = [ 'key' => '_waki_format', 'value' => $format ];
}

$args = [
    'post_type'      => Waki_Charts::CPT,
    'posts_per_page' => 10,
    'paged'          => $paged,
    'meta_key'       => '_waki_week_start',
    'orderby'        => 'meta_value',
    'order'          => 'DESC',
    'tax_query'      => [
        [
            'taxonomy' => $term->taxonomy,
            'terms'    => $term->term_id,
        ],
    ],
    'meta_query'    => $meta_query,
];

$q = new WP_Query($args);
?>

<?php waki_chart_breadcrumbs($term); ?>
<section class="waki-wrap waki-fw">
    <header class="waki-term-header">
        <h1><?php echo esc_html($term->name); ?></h1>
        <?php if ($years || $formats) : ?>
            <form method="get" id="waki-term-filters">
                <?php if ($years) : ?>
                    <label for="waki-year-select"><?php esc_html_e('Year:', 'wakilisha-charts'); ?></label>
                    <select id="waki-year-select" name="year" onchange="document.getElementById('waki-term-filters').submit();">
                        <option value=""><?php esc_html_e('All Years', 'wakilisha-charts'); ?></option>
                        <?php foreach ($years as $y) : ?>
                            <option value="<?php echo esc_attr($y); ?>"<?php selected($year, (int) $y); ?>><?php echo esc_html($y); ?></option>
                        <?php endforeach; ?>
                    </select>
                <?php endif; ?>
                <?php if ($formats && !is_tax('waki_format')) : ?>
                    <label for="waki-format-select"><?php esc_html_e('Format:', 'wakilisha-charts'); ?></label>
                    <select id="waki-format-select" name="format" onchange="document.getElementById('waki-term-filters').submit();">
                        <option value=""><?php esc_html_e('All Formats', 'wakilisha-charts'); ?></option>
                        <?php foreach ($formats as $f) : ?>
                            <option value="<?php echo esc_attr($f->slug); ?>"<?php selected($format, $f->slug); ?>><?php echo esc_html($f->name); ?></option>
                        <?php endforeach; ?>
                    </select>
                <?php endif; ?>
            </form>
        <?php endif; ?>
    </header>

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

