<?php
if (!defined('ABSPATH')) exit;

/**
 * Render breadcrumbs for Waki Charts posts.
 *
 * Prefers `waki_country` terms and lists ISO-2 codes. When no countries are
 * assigned it falls back to the `waki_region` hierarchy.
 *
 * @param int|\WP_Post|null $post Optional. Post object or ID. Defaults to global post.
 * @return void Outputs HTML navigation list of breadcrumbs.
 */
function waki_chart_breadcrumbs($post = null){
    $post = get_post($post);
    if (!$post) {
        return;
    }

    $crumbs = [];

    // Home
    $crumbs[] = [
        'label' => get_bloginfo('name'),
        'url'   => home_url('/')
    ];

    // Charts archive page
    $archive_url = home_url('/' . Waki_Charts::CPT_SLUG . '/');
    $crumbs[] = [
        'label' => __('Charts', 'wakilisha-charts'),
        'url'   => $archive_url
    ];

    $countries = get_the_terms($post, 'waki_country');
    if (!is_wp_error($countries) && $countries) {
        foreach ($countries as $country) {
            $crumbs[] = [
                'label' => strtoupper($country->slug),
                'url'   => get_term_link($country)
            ];
        }
    } else {
        $regions = get_the_terms($post, 'waki_region');
        if (!is_wp_error($regions) && $regions) {
            $region = $regions[0];
            $ancestors = array_reverse(get_ancestors($region->term_id, 'waki_region'));
            foreach ($ancestors as $ancestor_id) {
                $ancestor = get_term($ancestor_id, 'waki_region');
                if ($ancestor && !is_wp_error($ancestor)) {
                    $crumbs[] = [
                        'label' => $ancestor->name,
                        'url'   => get_term_link($ancestor)
                    ];
                }
            }
            $crumbs[] = [
                'label' => $region->name,
                'url'   => get_term_link($region)
            ];
        }
    }

    // Current post
    $crumbs[] = [
        'label' => get_the_title($post),
        'url'   => get_permalink($post)
    ];

    $parts = [];
    $total = count($crumbs) - 1; // last index
    foreach ($crumbs as $i => $c) {
        $label = esc_html($c['label']);
        if (!empty($c['url']) && $i !== $total) {
            $parts[] = '<a href="' . esc_url($c['url']) . '">' . $label . '</a>';
        } else {
            $parts[] = '<span class="current">' . $label . '</span>';
        }
    }

    echo '<nav class="waki-breadcrumbs">' . implode(' &rsaquo; ', $parts) . '</nav>';
}

/**
 * Output calendar navigation for a chart format and year.
 *
 * @param string $format Format slug.
 * @param int|null $year Year number, defaults to current.
 */
function waki_chart_calendar($format, $year = null){
    echo Waki_Charts::instance()->get_calendar_html($format, $year);
}
