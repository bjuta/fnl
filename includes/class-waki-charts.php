<?php
if (!defined('ABSPATH')) exit;
final class Waki_Charts {
    const SLUG       = 'waki_chart';
    const OPTS       = 'waki_chart_options';
    const CHARTS     = 'waki_charts';
    const CRON_HOOK  = 'waki_chart_ingest_daily';
    const RETRY_HOOK = 'waki_chart_api_retry';
    const TZ         = 'Africa/Nairobi';
    const API_BASE   = 'https://api.spotify.com';
    const AUTH_URL   = 'https://accounts.spotify.com/api/token';
    const VER        = '3.1';
    const ARCHIVE_INTRO = 'waki_archive_intro';

    // CPT
    const CPT        = 'wakilisha_chart';
    const CPT_SLUG   = 'charts';

    private static $instance = null;
    private $table;
    private $artist_table;
    private $resolved_chart = null;

    public static function instance(){ return self::$instance ?: (self::$instance = new self()); }

    private function __construct(){
        global $wpdb;
        $this->table        = $wpdb->prefix . 'waki_chart';
        $this->artist_table = $wpdb->prefix . 'waki_artists';

        register_activation_hook(WAKI_CHARTS_PLUGIN_FILE,  [$this,'activate']);
        register_deactivation_hook(WAKI_CHARTS_PLUGIN_FILE,[$this,'deactivate']);
        // Run upgrade routines after WordPress is fully initialized to avoid early-load issues.
        add_action('init',                   [$this,'maybe_upgrade'], 11);

        // CPT + templates
        add_action('init',                   [$this,'register_cpt']);
        add_action('init',                   [$this,'register_taxonomies']);
        add_action('init',                   [$this,'register_shortcodes']);
        add_action('init',                   [$this,'register_assets']);     // register (enqueue later)
        add_action('init',                   [$this,'register_chart_rewrites']);

        add_action('admin_menu',             [$this,'admin_menu']);
        add_action('admin_init',             [$this,'handle_manual_run_legacy']);
        add_action('admin_init',             [$this,'handle_charts_actions']);
        add_action('admin_init',             [$this,'handle_artist_actions']);
        add_action('admin_init',             [$this,'handle_dry_run_action']);
        add_action('admin_init',             [$this,'handle_archive_reset_action']);
        add_action('rest_api_init',         [$this,'register_rest_routes']);

        add_action(self::CRON_HOOK,          [$this,'cron_run_all_charts']);
        add_action(self::RETRY_HOOK,         [$this,'resume_api_request']);

        // Auto enqueue on CPT single/archive or shortcodes
        add_action('wp',                     [$this,'maybe_enqueue_assets']);

        // Force content on CPT single to be our shortcode (safety)
        add_filter('the_content',            [$this,'force_single_content'], 9);

        // Shortcodes
        add_shortcode('waki_chart',           [$this,'shortcode_latest_chart']);
        add_shortcode('waki_charts_archive',  [$this,'shortcode_charts_archive']);

        add_action('wp_trash_post',        [$this,'remove_chart_rows']);
        add_action('before_delete_post',   [$this,'remove_chart_rows']);

        add_action('wp_head',              [$this,'output_social_meta']);
        add_action('wp_head',              [$this,'output_chart_canonical']);
        add_filter('pre_get_shortlink',    [$this,'pretty_shortlink'], 10, 2);

        // Artist profile routing
        add_filter('query_vars',            [$this,'add_query_vars']);
        add_action('pre_get_posts',        [$this,'resolve_chart_request']);
        add_filter('template_include',      [$this,'load_artist_template']);

        add_action('add_meta_boxes_' . self::CPT, [$this,'add_chart_keys_meta_box']);
        add_action('save_post_' . self::CPT,      [$this,'handle_chart_save'], 10, 3);

        if (defined('WP_CLI') && WP_CLI) {
            \WP_CLI::add_command('waki-charts reset-archive', [$this, 'cli_reset_archive']);
        }
    }

    /* ===== Lifecycle ===== */
    public function activate(){
        $this->create_tables();
        $this->schedule_daily_cron();
        $this->ensure_archive_page();
        $this->register_cpt();
        $this->register_taxonomies();
        $this->register_chart_rewrites();
        $this->ensure_country_terms();
        flush_rewrite_rules();
        add_option(self::ARCHIVE_INTRO, $this->default_archive_intro());
        update_option(self::SLUG . '_ver', self::VER);
    }
    public function deactivate(){
        wp_clear_scheduled_hook(self::CRON_HOOK);
        flush_rewrite_rules();
    }

    private function create_tables(){
        global $wpdb; $charset_collate = $wpdb->get_charset_collate();
        require_once ABSPATH . 'wp-admin/includes/upgrade.php';

        // chart rows (tracks)
        $sql1 = "CREATE TABLE {$this->table} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

            chart_key VARCHAR(64) NOT NULL DEFAULT 'default',
            chart_date DATE NOT NULL,
            snapshot_id VARCHAR(128) DEFAULT '' NOT NULL,

            track_id VARCHAR(64) NOT NULL,
            isrc VARCHAR(32) DEFAULT '' NOT NULL,

            track_name VARCHAR(255) NOT NULL,
            artists TEXT NULL,
            artist_ids TEXT NULL,
            genres TEXT NULL,

            popularity TINYINT UNSIGNED DEFAULT 0 NOT NULL,
            duration_ms INT UNSIGNED DEFAULT 0 NOT NULL,
            album_image_url VARCHAR(512) DEFAULT '' NOT NULL,
            album_release_date VARCHAR(32) DEFAULT '' NOT NULL,
            label VARCHAR(255) DEFAULT '' NOT NULL,
            added_at DATETIME NULL,
            score FLOAT DEFAULT 0 NOT NULL,
            position INT UNSIGNED DEFAULT 0 NOT NULL,
            position_change INT DEFAULT 0 NOT NULL,
            weeks_on_chart INT UNSIGNED DEFAULT 1 NOT NULL,

            peak_position INT UNSIGNED DEFAULT 0 NOT NULL,
            peak_date DATE NULL,
            debut_position INT UNSIGNED DEFAULT 0 NOT NULL,
            debut_date DATE NULL,

            score_change FLOAT DEFAULT 0 NOT NULL,
            popularity_change INT DEFAULT 0 NOT NULL,
            in_playlists INT DEFAULT 1 NOT NULL,

            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_key_date_snapshot_track (chart_key, chart_date, snapshot_id, track_id),
            KEY idx_key_date (chart_key, chart_date),
            KEY idx_track (track_id),
            KEY idx_position (position),
            KEY idx_key_track_date (chart_key, track_id, chart_date),
            KEY idx_key_date_snap_pos (chart_key, chart_date, snapshot_id, position)
        ) $charset_collate;";
        dbDelta($sql1);

        // artists directory (manual origin mapping + profile meta)
        $sql2 = "CREATE TABLE {$this->artist_table} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            artist_id VARCHAR(64) NOT NULL,
            artist_name VARCHAR(255) NOT NULL,
            artist_slug VARCHAR(191) NOT NULL,
            origin_country CHAR(2) NULL,

            followers INT UNSIGNED DEFAULT 0 NOT NULL,
            popularity TINYINT UNSIGNED DEFAULT 0 NOT NULL,
            genres TEXT NULL,
            image_url VARCHAR(512) DEFAULT '' NOT NULL,
            profile_url VARCHAR(512) DEFAULT '' NOT NULL,
            biography TEXT NULL,
            latest_release TEXT NULL,
            top_tracks TEXT NULL,
            discography TEXT NULL,
            chart_stats TEXT NULL,
            video_urls TEXT NULL,
            related_artist_ids TEXT NULL,
            status VARCHAR(20) DEFAULT 'draft' NOT NULL,

            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_artist (artist_id),
            KEY idx_artist_slug (artist_slug(100)),
            KEY idx_country (origin_country),
            KEY idx_artist_name (artist_name(100)),
            KEY idx_followers (followers),
            KEY idx_popularity (popularity)
        ) $charset_collate;";
        dbDelta($sql2);
    }

    private function ensure_country_terms(){
        $countries = [
            'KE' => 'Kenya',
            'UG' => 'Uganda',
            'TZ' => 'Tanzania',
            'RW' => 'Rwanda',
            'ET' => 'Ethiopia',
            'NG' => 'Nigeria',
            'ZA' => 'South Africa',
            'GH' => 'Ghana',
        ];
        foreach($countries as $code => $name){
            $slug = strtolower($code);
            if(!term_exists($slug, 'waki_country')){
                wp_insert_term($name, 'waki_country', ['slug' => $slug]);
            }
        }
    }

    private function migrate_immutable_editions(){
        global $wpdb;
        $table = $this->table;

        $idx = $wpdb->get_results("SHOW INDEX FROM {$table}", ARRAY_A);
        $names = $idx ? array_unique(wp_list_pluck($idx,'Key_name')) : [];

        // Only backfill legacy deterministic snapshot_ids when uniqueness isn't enforced yet.
        if (!in_array('uq_key_date_snapshot_track',$names,true)) {
            $wpdb->query(
                "UPDATE {$table}
                 SET snapshot_id = CONCAT(chart_key,'-',MD5(CONCAT(chart_key,':',chart_date)))
                 WHERE (snapshot_id='' OR snapshot_id IS NULL)"
            );
        }

        if (in_array('uq_key_date_track',$names,true)) {
            $wpdb->query("ALTER TABLE {$table} DROP INDEX uq_key_date_track");
        }
        if (!in_array('uq_key_date_snapshot_track',$names,true)) {
            $wpdb->query("ALTER TABLE {$table}
              ADD UNIQUE KEY uq_key_date_snapshot_track (chart_key, chart_date, snapshot_id, track_id)");
        }
        if (!in_array('idx_key_date_snap_pos',$names,true)) {
            $wpdb->query("ALTER TABLE {$table}
              ADD KEY idx_key_date_snap_pos (chart_key, chart_date, snapshot_id, position)");
        }
    }

    public function maybe_upgrade(){
        // Backfill tables/columns if needed; also ensure archive page exists
        global $wpdb;
        $this->migrate_immutable_editions();
        $cols = $wpdb->get_results("SHOW COLUMNS FROM {$this->table}", ARRAY_A);
        $have = $cols ? wp_list_pluck($cols,'Field') : [];
        $alter = [];
        $add = function($col,$def) use(&$alter,$have){ if(!in_array($col,$have,true)) $alter[]="ADD COLUMN $col $def"; };
        $add('genres','TEXT NULL');
        $add('artist_ids','TEXT NULL');
        $add('isrc',"VARCHAR(32) DEFAULT '' NOT NULL");
        $add('label',"VARCHAR(255) DEFAULT '' NOT NULL");
        if ($alter){ $wpdb->query("ALTER TABLE {$this->table} ".implode(', ',$alter)); }

        // remove legacy streaming link columns
        $drop = ['preview_url','profile_url','apple_url','audiomack_url','youtube_url','deezer_url'];
        foreach($drop as $col){
            if(in_array($col,$have,true)){
                $wpdb->query("ALTER TABLE {$this->table} DROP COLUMN $col");
            }
        }

        // ensure artists table exists
        $exists = $wpdb->get_var($wpdb->prepare("SHOW TABLES LIKE %s", $this->artist_table));
        if (!$exists) $this->create_tables();

        // ensure new indexes exist
        $idx = $wpdb->get_results("SHOW INDEX FROM {$this->table}", ARRAY_A);
        $names = $idx ? array_unique(wp_list_pluck($idx,'Key_name')) : [];
        if (!in_array('idx_key_track_date',$names,true)){
            $wpdb->query("ALTER TABLE {$this->table} ADD KEY idx_key_track_date (chart_key, track_id, chart_date)");
        }

        // artist table columns & indexes (meta fields)
        $colsA = $wpdb->get_results("SHOW COLUMNS FROM {$this->artist_table}", ARRAY_A);
        $haveA = $colsA ? wp_list_pluck($colsA,'Field') : [];
        $alterA = [];
        $addA = function($col,$def) use(&$alterA,$haveA){ if(!in_array($col,$haveA,true)) $alterA[]="ADD COLUMN $col $def"; };
        $added_slug = !in_array('artist_slug',$haveA,true);
        $addA('artist_slug',"VARCHAR(191) NOT NULL");
        $addA('followers','INT UNSIGNED DEFAULT 0 NOT NULL');
        $addA('popularity','TINYINT UNSIGNED DEFAULT 0 NOT NULL');
        $addA('genres','TEXT NULL');
        $addA('image_url',"VARCHAR(512) DEFAULT '' NOT NULL");
        $addA('profile_url',"VARCHAR(512) DEFAULT '' NOT NULL");
        if(!in_array('biography',$haveA,true)){
            if(in_array('bio',$haveA,true)){
                $alterA[] = 'CHANGE bio biography TEXT NULL';
            } else {
                $alterA[] = 'ADD COLUMN biography TEXT NULL';
            }
        }
        $addA('latest_release','TEXT NULL');
        $addA('top_tracks','TEXT NULL');
        $addA('discography','TEXT NULL');
        $addA('chart_stats','TEXT NULL');
        $addA('video_urls','TEXT NULL');
        $addA('related_artist_ids','TEXT NULL');
        $addA('status', "VARCHAR(20) DEFAULT 'draft' NOT NULL");
        if($alterA){ $wpdb->query("ALTER TABLE {$this->artist_table} ".implode(', ',$alterA)); }
        if($added_slug){
            $rows = $wpdb->get_results("SELECT artist_id, artist_name FROM {$this->artist_table}", ARRAY_A);
            foreach($rows as $row){
                $slug = $this->generate_artist_slug($row['artist_name'], $row['artist_id']);
                $wpdb->update($this->artist_table, ['artist_slug'=>$slug], ['artist_id'=>$row['artist_id']]);
            }
        }

        $idx2 = $wpdb->get_results("SHOW INDEX FROM {$this->artist_table}", ARRAY_A);
        $names2 = $idx2 ? array_unique(wp_list_pluck($idx2,'Key_name')) : [];
        if (!in_array('idx_artist_slug',$names2,true)){
            $wpdb->query("ALTER TABLE {$this->artist_table} ADD KEY idx_artist_slug (artist_slug(100))");
        }
        if (!in_array('idx_artist_name',$names2,true)){
            $wpdb->query("ALTER TABLE {$this->artist_table} ADD KEY idx_artist_name (artist_name(100))");
        }
        if (!in_array('idx_followers',$names2,true)){
            $wpdb->query("ALTER TABLE {$this->artist_table} ADD KEY idx_followers (followers)");
        }
        if (!in_array('idx_popularity',$names2,true)){
            $wpdb->query("ALTER TABLE {$this->artist_table} ADD KEY idx_popularity (popularity)");
        }

        // initial artist backfill from existing rows (once)
        if (!get_option(self::SLUG.'_artists_seeded')){
            $this->seed_artists_from_rows();
            update_option(self::SLUG.'_artists_seeded', 1);
        }

        // backfill cover artwork meta for existing chart posts (once)
        if (!get_option(self::SLUG.'_covers_seeded')){
            $this->backfill_cover_meta();
            update_option(self::SLUG.'_covers_seeded', 1);
        }

        // cleanup: rule — remove unnamed artists
        $wpdb->query("DELETE FROM {$this->artist_table} WHERE artist_name IS NULL OR artist_name=''");

        $this->ensure_archive_page();

        // refresh rewrite rules when the plugin version changes to avoid 404s
        $stored_ver = get_option(self::SLUG . '_ver');
        if ($stored_ver !== self::VER) {
            flush_rewrite_rules();
            update_option(self::SLUG . '_ver', self::VER);
        }
    }

    private function schedule_daily_cron(){
        if (!wp_next_scheduled(self::CRON_HOOK)) {
            $tz = new DateTimeZone(self::TZ);
            $now = new DateTime('now', $tz);
            $next = new DateTime('today 02:00', $tz);
            if ($now >= $next) $next->modify('+1 day');
            $utc_ts = $next->getTimestamp() - $tz->getOffset($next);
            wp_schedule_event($utc_ts, 'daily', self::CRON_HOOK);
        }
    }

    private function ensure_archive_page(){
        global $wp_rewrite;
        if (!($wp_rewrite instanceof WP_Rewrite)) {
            error_log('Waki_Charts: WP_Rewrite not initialized; aborting archive page setup.');
            return;
        }
        $wp_rewrite->init();

        $opts = $this->get_options();
        $page_id = intval($opts['charts_archive_page_id'] ?? 0);
        if ($page_id && get_post($page_id)) return;

        $existing = get_page_by_path(self::CPT_SLUG);
        if ($existing) {
            $opts['charts_archive_page_id'] = $existing->ID;
            update_option(self::OPTS, $opts);
            $wp_rewrite->flush_rules(false);
            return;
        }

        $page_id = wp_insert_post([
            'post_title'   => 'Charts',
            'post_name'    => self::CPT_SLUG,
            'post_type'    => 'page',
            'post_status'  => 'publish',
            'post_content' => '[waki_charts_archive]'
        ]);
        if (!is_wp_error($page_id)) {
            $opts['charts_archive_page_id'] = $page_id;
            update_option(self::OPTS, $opts);
            $wp_rewrite->flush_rules(false);
        }
    }

    private function reset_archive_page(){
        global $wp_rewrite;
        if (!($wp_rewrite instanceof WP_Rewrite)) {
            error_log('Waki_Charts: WP_Rewrite not initialized; aborting archive page reset.');
            return;
        }
        $wp_rewrite->init();

        $opts = $this->get_options();
        $page_id = intval($opts['charts_archive_page_id'] ?? 0);
        if ($page_id) {
            wp_delete_post($page_id, true);
            $opts['charts_archive_page_id'] = 0;
            update_option(self::OPTS, $opts);
        }
        $existing = get_page_by_path(self::CPT_SLUG);
        if ($existing && ($existing->ID !== $page_id)) {
            wp_delete_post($existing->ID, true);
        }

        $this->ensure_archive_page();
    }

    /* ===== Options ===== */
    private function get_options(){
        $defaults = [
            'client_id'=>'','client_secret'=>'','market'=>'KE',
            'alpha'=>'1.0','beta'=>'0.5','gamma'=>'0.3','presence_bonus'=>'0.5',
            'auto_make_post'=>'1','post_category'=>'Charts',
            'last_run'=>'','last_snapshot'=>'','last_chart_date'=>'',
            'charts_archive_page_id'=>0,
            'archive_hero_img'=>'',
            'hero_img_size'=>'full',
        ];
        return wp_parse_args(get_option(self::OPTS,[]), $defaults);
    }
    private function get_charts(){ $x=get_option(self::CHARTS,[]); return is_array($x)?$x:[]; }
    private function put_charts($arr){ update_option(self::CHARTS, is_array($arr)?$arr:[]); }
    private function normalize_slug($slug){ $slug = strtolower(sanitize_title($slug ?: '')); return $slug ?: 'default'; }

    private function normalize_playlist_id($input){
        $input = trim((string)$input); if($input==='') return '';
        if(preg_match('/^(?:[a-z]+:playlist:|https?:\/\/[^\/]+\/playlist\/)([A-Za-z0-9]{22})/',$input,$m)){
            return $m[1];
        }
        return preg_match('/^[A-Za-z0-9]{22}$/',$input)?$input:'';
    }
    private function normalize_many($text){
        $ids=[]; foreach(preg_split('/\R+/', (string)$text) as $line){ $line=trim($line); if(!$line) continue; $id=$this->normalize_playlist_id($line); if($id) $ids[]=$id; }
        return array_values(array_unique($ids));
    }

    private function compute_country_key($post_id){
        $terms = get_the_terms($post_id,'waki_country');
        if($terms && !is_wp_error($terms)){
            $slugs = array_unique(array_map(function($t){ return strtolower(sanitize_title($t->slug)); }, $terms));
            sort($slugs, SORT_STRING);
            if (count($slugs) > 10){
                wp_die(__('At most 10 unique countries allowed.', 'wakilisha-charts'));
            }
            $key = implode('-', $slugs);
            if(strlen($key) > 40){
                wp_die(__('Country key cannot exceed 40 characters.', 'wakilisha-charts'));
            }
            update_post_meta($post_id,'_waki_country_key',$key);
            return $key;
        }

        $regions = get_the_terms($post_id,'waki_region');
        if($regions && !is_wp_error($regions) && count($regions) === 1){
            $slug = strtolower(sanitize_title($regions[0]->slug));
            update_post_meta($post_id,'_waki_country_key',$slug);
            return $slug;
        }

        delete_post_meta($post_id,'_waki_country_key');
        return '';
    }

    private function compute_chart_key($post_id, $country_key=''){
        $country_key = $country_key ?: get_post_meta($post_id,'_waki_country_key',true);
        $genre = get_the_terms($post_id,'waki_genre');
        $format = get_the_terms($post_id,'waki_format');
        $genre_slug = '';
        if($genre && !is_wp_error($genre)){ $genre_slug = strtolower(sanitize_title($genre[0]->slug)); }
        $format_slug='';
        if($format && !is_wp_error($format)){ $format_slug = strtolower(sanitize_title($format[0]->slug)); }
        $parts = array_filter([$country_key,$genre_slug,$format_slug]);
        $key = strtolower(implode('-', $parts));
        update_post_meta($post_id,'_waki_chart_key',$key);
        return $key;
    }

    public function handle_chart_save($post_id,$post,$update){
        if(defined('DOING_AUTOSAVE') && DOING_AUTOSAVE) return;
        if($post->post_type!==self::CPT) return;
        if(wp_is_post_revision($post_id)) return;

        $errors=[];
        $countries = get_the_terms($post_id,'waki_country');
        $regions   = get_the_terms($post_id,'waki_region');
        if((!$countries || is_wp_error($countries)) && (!$regions || is_wp_error($regions))){
            $errors[] = __('Assign at least one country or region.','wakilisha-charts');
        }
        if($countries && !is_wp_error($countries)){
            $slugs = wp_list_pluck($countries,'slug');
            if(count($slugs) !== count(array_unique($slugs))){
                $errors[] = __('Countries must be unique.','wakilisha-charts');
            }
        }elseif($regions && !is_wp_error($regions)){
            if(count($regions) !== 1){
                $errors[] = __('Exactly one region is required when no countries are set.','wakilisha-charts');
            }
        }
        foreach(['waki_genre','waki_language'] as $tax){
            $raw = $_POST['tax_input'][$tax] ?? [];
            $vals = is_array($raw) ? $raw : preg_split('/[\s,]+/', (string)$raw);
            foreach($vals as $slug){
                $slug = sanitize_title($slug);
                if($slug === '') continue;
                if(!term_exists($slug,$tax)){
                    $errors[] = sprintf(__('Unknown %s term: %s','wakilisha-charts'), $tax, $slug);
                }
            }
        }
        if($errors){ wp_die(implode('<br>', $errors)); }

        $country_key = $this->compute_country_key($post_id);
        $this->compute_chart_key($post_id,$country_key);
    }

    public function add_chart_keys_meta_box(){
        add_meta_box('waki_chart_keys', __('Chart Keys','wakilisha-charts'), [$this,'render_chart_keys_meta_box'], self::CPT, 'side', 'default');
    }

    public function render_chart_keys_meta_box($post){
        $country_key = get_post_meta($post->ID,'_waki_country_key',true);
        $chart_key   = get_post_meta($post->ID,'_waki_chart_key',true);
        $genre = get_the_terms($post->ID,'waki_genre');
        $format = get_the_terms($post->ID,'waki_format');
        $genre_slug = ($genre && !is_wp_error($genre)) ? strtolower(sanitize_title($genre[0]->slug)) : '';
        $format_slug = ($format && !is_wp_error($format)) ? strtolower(sanitize_title($format[0]->slug)) : '';
        $has_country = has_term('', 'waki_country', $post->ID);
        $base = $has_country ? 'country' : 'region';
        $path_parts = array_filter([$country_key, $genre_slug, $format_slug]);
        $url = $country_key ? home_url('/'.self::CPT_SLUG.'/'.$base.'/'.implode('/', $path_parts).'/') : '';
        echo '<p><strong>'.esc_html__('Country key','wakilisha-charts').":</strong><br>".esc_html($country_key ?: '-').'</p>';
        echo '<p><strong>'.esc_html__('Chart key','wakilisha-charts').":</strong><br>".esc_html($chart_key ?: '-').'</p>';
        if($url){ echo '<p><strong>'.esc_html__('URL preview','wakilisha-charts').":</strong><br><a href='".esc_url($url)."' target='_blank'>".esc_html($url).'</a></p>'; }
    }

    private function generate_artist_slug($name, $current_id = ''){
        $slug = sanitize_title($name ?: '');
        if ($slug === '') return '';
        global $wpdb;
        $base = $slug; $i = 2;
        $current_id = sanitize_text_field($current_id);
        while ($wpdb->get_var($wpdb->prepare("SELECT artist_id FROM {$this->artist_table} WHERE artist_slug=%s AND artist_id<>%s", $slug, $current_id))) {
            $slug = $base . '-' . $i;
            $i++;
        }
        return $slug;
    }

    /* ===== Taxonomies ===== */
    public function register_taxonomies(){
        $common = [
            'show_ui' => true,
            'show_in_rest' => true,
            'show_admin_column' => true,
        ];

        register_taxonomy('waki_genre', [self::CPT], array_merge($common, [
            'labels' => [
                'name' => __('Genres', 'wakilisha-charts'),
                'singular_name' => __('Genre', 'wakilisha-charts'),
            ],
            'rewrite' => ['slug' => 'genre', 'with_front' => false],
            'hierarchical' => false,
        ]));

        register_taxonomy('waki_language', [self::CPT], array_merge($common, [
            'labels' => [
                'name' => __('Languages', 'wakilisha-charts'),
                'singular_name' => __('Language', 'wakilisha-charts'),
            ],
            'rewrite' => ['slug' => 'language', 'with_front' => false],
            'hierarchical' => false,
        ]));

        register_taxonomy('waki_format', [self::CPT], array_merge($common, [
            'labels' => [
                'name' => __('Formats', 'wakilisha-charts'),
                'singular_name' => __('Format', 'wakilisha-charts'),
            ],
            'rewrite' => ['slug' => 'format', 'with_front' => false],
            'hierarchical' => false,
        ]));

        register_taxonomy('waki_country', [self::CPT], array_merge($common, [
            'labels' => [
                'name' => __('Countries', 'wakilisha-charts'),
                'singular_name' => __('Country', 'wakilisha-charts'),
            ],
            'rewrite' => ['slug' => 'country', 'with_front' => false],
            'hierarchical' => false,
        ]));

        register_taxonomy('waki_region', [self::CPT], array_merge($common, [
            'labels' => [
                'name' => __('Regions', 'wakilisha-charts'),
                'singular_name' => __('Region', 'wakilisha-charts'),
            ],
            'rewrite' => ['slug' => 'region', 'with_front' => false],
            'hierarchical' => true,
        ]));
    }

    /* ===== CPT ===== */
    public function register_cpt(){
        register_post_type(self::CPT, [
            'labels'=>[
                'name' => __('Charts', 'wakilisha-charts'),
                'singular_name' => __('Chart', 'wakilisha-charts'),
                'add_new_item' => __('Add Chart Issue', 'wakilisha-charts'),
                'edit_item' => __('Edit Chart Issue', 'wakilisha-charts'),
                'new_item' => __('New Chart Issue', 'wakilisha-charts'),
                'view_item' => __('View Chart', 'wakilisha-charts'),
                'search_items' => __('Search Charts', 'wakilisha-charts'),
                'not_found' => __('No charts found', 'wakilisha-charts'),
            ],
            'public'=>true,
            'has_archive'=>false, // use custom /charts/ page and suppress CPT breadcrumbs
            'rewrite'=>['slug'=>self::CPT_SLUG,'with_front'=>false],
            'show_in_rest'=>true,
            'menu_icon'=>'dashicons-chart-line',
            'supports'=>['title','editor','thumbnail','excerpt','author'],
        ]);
        register_post_meta(
            self::CPT,
            'waki_chart_payload',
            [
                'type'         => 'string',
                'single'       => true,
                'show_in_rest' => true,
                'auth_callback'=> '__return_true',
            ]
        );

        // Rewrite rule for artist profiles
        add_rewrite_tag('%artist_slug%', '([^/]+)');
        add_rewrite_rule('^artist/([^/]+)/?$', 'index.php?artist_slug=$matches[1]', 'top');
    }

    public function register_chart_rewrites(){
        $base = self::CPT_SLUG;
        add_rewrite_tag('%waki_chart_country%', '([^/]+)');
        add_rewrite_tag('%waki_chart_region%', '([^/]+)');
        add_rewrite_tag('%waki_chart_genre%', '([^/]+)');
        add_rewrite_tag('%waki_chart_format%', '([^/]+)');
        add_rewrite_tag('%waki_chart_date%', '([0-9]{4}-[0-9]{2}-[0-9]{2})');
        add_rewrite_tag('%waki_chart_latest%', '1');

        add_rewrite_rule("^{$base}/country/([^/]+)/([^/]+)/([^/]+)/([0-9]{4}-[0-9]{2}-[0-9]{2})/?$", 'index.php?waki_chart_country=$matches[1]&waki_chart_genre=$matches[2]&waki_chart_format=$matches[3]&waki_chart_date=$matches[4]', 'top');
        add_rewrite_rule("^{$base}/country/([^/]+)/([^/]+)/([0-9]{4}-[0-9]{2}-[0-9]{2})/?$", 'index.php?waki_chart_country=$matches[1]&waki_chart_format=$matches[2]&waki_chart_date=$matches[3]', 'top');
        add_rewrite_rule("^{$base}/country/([^/]+)/([^/]+)/([^/]+)/latest/?$", 'index.php?waki_chart_country=$matches[1]&waki_chart_genre=$matches[2]&waki_chart_format=$matches[3]&waki_chart_latest=1', 'top');
        add_rewrite_rule("^{$base}/country/([^/]+)/([^/]+)/latest/?$", 'index.php?waki_chart_country=$matches[1]&waki_chart_format=$matches[2]&waki_chart_latest=1', 'top');

        add_rewrite_rule("^{$base}/region/([^/]+)/([^/]+)/([^/]+)/([0-9]{4}-[0-9]{2}-[0-9]{2})/?$", 'index.php?waki_chart_region=$matches[1]&waki_chart_genre=$matches[2]&waki_chart_format=$matches[3]&waki_chart_date=$matches[4]', 'top');
        add_rewrite_rule("^{$base}/region/([^/]+)/([^/]+)/([0-9]{4}-[0-9]{2}-[0-9]{2})/?$", 'index.php?waki_chart_region=$matches[1]&waki_chart_format=$matches[2]&waki_chart_date=$matches[3]', 'top');
        add_rewrite_rule("^{$base}/region/([^/]+)/([^/]+)/([^/]+)/latest/?$", 'index.php?waki_chart_region=$matches[1]&waki_chart_genre=$matches[2]&waki_chart_format=$matches[3]&waki_chart_latest=1', 'top');
        add_rewrite_rule("^{$base}/region/([^/]+)/([^/]+)/latest/?$", 'index.php?waki_chart_region=$matches[1]&waki_chart_format=$matches[2]&waki_chart_latest=1', 'top');
    }

    public function add_query_vars($vars){
        $vars[] = 'artist_slug';
        $vars[] = 'artist_id';
        $vars[] = 'preview';
        $vars[] = '_wpnonce';
        $vars[] = 'waki_chart_country';
        $vars[] = 'waki_chart_region';
        $vars[] = 'waki_chart_genre';
        $vars[] = 'waki_chart_format';
        $vars[] = 'waki_chart_date';
        $vars[] = 'waki_chart_latest';
        return $vars;
    }

    public function resolve_chart_request($query){
        if (!is_admin() && $query->is_main_query()){
            $country = sanitize_title($query->get('waki_chart_country'));
            $region  = sanitize_title($query->get('waki_chart_region'));
            $genre   = sanitize_title($query->get('waki_chart_genre'));
            $format  = sanitize_title($query->get('waki_chart_format'));
            $date    = sanitize_text_field($query->get('waki_chart_date'));
            $latest  = $query->get('waki_chart_latest');
            if ($country || $region){
                $base = $country ?: $region;
                $parts = array_filter([$base, $genre, $format]);
                $key   = strtolower(implode('-', $parts));
                if ($key){
                    $pid = 0;
                    if ($latest || !$date){
                        $q = new \WP_Query([
                            'post_type'      => self::CPT,
                            'post_status'    => 'publish',
                            'posts_per_page' => 1,
                            'meta_key'       => '_waki_chart_date',
                            'orderby'        => 'meta_value',
                            'order'          => 'DESC',
                            'fields'         => 'ids',
                            'meta_query'     => [[ 'key'=>'_waki_chart_key','value'=>$key ]],
                        ]);
                        if ($q->have_posts()){ $pid = $q->posts[0]; $date = get_post_meta($pid,'_waki_chart_date',true); }
                    } elseif ($date){
                        $q = new \WP_Query([
                            'post_type'      => self::CPT,
                            'post_status'    => 'publish',
                            'posts_per_page' => 1,
                            'fields'         => 'ids',
                            'meta_query'     => [
                                ['key'=>'_waki_chart_key','value'=>$key],
                                ['key'=>'_waki_chart_date','value'=>$date],
                            ],
                        ]);
                        if ($q->have_posts()){ $pid = $q->posts[0]; }
                    }
                    if ($pid){
                        $query->set('post_type', self::CPT);
                        $query->set('p', intval($pid));
                        $query->is_single = true;
                        $query->is_singular = true;
                        $reg_terms = get_the_terms($pid,'waki_region');
                        $reg_slug = '';
                        if($reg_terms && !is_wp_error($reg_terms)){
                            $reg_slug = strtolower(sanitize_title($reg_terms[0]->slug));
                        }
                        $this->resolved_chart = [
                            'key' => $key,
                            'date' => $date,
                            'genre' => $genre,
                            'format' => $format,
                            'region' => $region,
                            'region_term' => $reg_slug,
                            'latest' => !empty($latest) || empty($query->get('waki_chart_date')),
                            'canonical_country' => get_post_meta($pid, '_waki_country_key', true),
                        ];
                    } else {
                        $query->set_404();
                    }
                } else {
                    $query->set_404();
                }
            }
        }
    }

    public function output_chart_canonical(){
        if (empty($this->resolved_chart)) return;
        $rc = $this->resolved_chart;
        if (empty($rc['canonical_country'])) return;
        $pid = get_queried_object_id();
        $has_country = $pid ? has_term('', 'waki_country', $pid) : false;
        $base = $has_country ? 'country' : 'region';
        $parts = array_filter([$rc['canonical_country'], $rc['genre'], $rc['format']]);
        $url = home_url('/' . self::CPT_SLUG . '/' . $base . '/' . implode('/', $parts) . '/');
        $url .= $rc['latest'] ? 'latest' : $rc['date'] . '/';
        echo '<link rel="canonical" href="' . esc_url($url) . '" />';
    }

    public function load_artist_template($template){
        $slug = get_query_var('artist_slug');
        $artist_id = get_query_var('artist_id');
        if ($slug || $artist_id) {
            global $wpdb;
            $artist = null;
            $preview = intval(get_query_var('preview')) === 1;
            $nonce = $_GET['_wpnonce'] ?? '';
            $field = $slug ? 'artist_slug' : 'artist_id';
            $value = $slug ?: $artist_id;
            if ($preview && $artist_id && $nonce && wp_verify_nonce($nonce, 'preview_artist_'.$artist_id) && current_user_can('manage_options')) {
                $artist = $wpdb->get_row(
                    $wpdb->prepare(
                        "SELECT artist_id, artist_slug, artist_name, image_url, genres, followers, biography, latest_release, top_tracks, discography, chart_stats, video_urls, related_artist_ids FROM {$this->artist_table} WHERE {$field}=%s",
                        $value
                    ),
                    ARRAY_A
                );
            } else {
                $artist = $wpdb->get_row(
                    $wpdb->prepare(
                        "SELECT artist_id, artist_slug, artist_name, image_url, genres, followers, biography, latest_release, top_tracks, discography, chart_stats, video_urls, related_artist_ids FROM {$this->artist_table} WHERE {$field}=%s AND status='publish'",
                        $value
                    ),
                    ARRAY_A
                );
                if (!$artist && $slug) {
                    $artist = $wpdb->get_row(
                        $wpdb->prepare(
                            "SELECT artist_id, artist_slug, artist_name, image_url, genres, followers, biography, latest_release, top_tracks, discography, chart_stats, video_urls, related_artist_ids FROM {$this->artist_table} WHERE artist_id=%s AND status='publish'",
                            $slug
                        ),
                        ARRAY_A
                    );
                }
            }
            if ($artist) {
                // Normalize $artist and resolve conflict (safe for both branches)
                $artistArr = is_object($artist) ? get_object_vars($artist) : (array) $artist;

                $splitCsv = function ($value) {
                    if (is_array($value)) return $value;
                    if (!is_string($value) || trim($value) === '') return [];
                    $parts = array_map('trim', explode(',', $value));
                    return array_values(array_filter($parts, static fn($v) => $v !== ''));
                };

                $decodeJson = function ($value) {
                    if (is_array($value)) return $value;
                    if (!is_string($value) || trim($value) === '') return [];
                    $decoded = json_decode($value, true);
                    return is_array($decoded) ? $decoded : [];
                };

                // Map DB fields to template expectations
                $artistArr['top_tracks']   = $splitCsv($artistArr['top_tracks']   ?? []);
                $artistArr['videos']       = $splitCsv($artistArr['video_urls']   ?? []);
                $ids                       = $splitCsv($artistArr['related_artist_ids'] ?? []);
                $artistArr['discography']  = $decodeJson($artistArr['discography'] ?? []);
                $artistArr['chart_history'] = $decodeJson($artistArr['chart_stats']  ?? []);
                unset($artistArr['video_urls'], $artistArr['related_artist_ids'], $artistArr['chart_stats']);

                // Lookup related artist names
                $artistArr['related_artists'] = [];
                if (!empty($ids)) {
                    $placeholders = implode(',', array_fill(0, count($ids), '%s'));
                    $artistArr['related_artists'] = $wpdb->get_col(
                        $wpdb->prepare(
                            "SELECT artist_name FROM {$this->artist_table} WHERE artist_id IN ($placeholders)",
                            ...$ids
                        )
                    );
                }

                set_query_var('waki_artist', (object) $artistArr);

                wp_enqueue_style(self::SLUG);
                wp_enqueue_script(self::SLUG);
                return WAKI_CHARTS_DIR . 'templates/artist-profile.php';
            }
        }
        return $template;
    }

    /* ===== Assets ===== */
    public function register_assets(){
        $base = plugin_dir_url(WAKI_CHARTS_PLUGIN_FILE);
        wp_register_style(self::SLUG, $base . 'assets/css/wakilisha-charts.css', [], self::VER);
        wp_register_script(self::SLUG, $base . 'assets/js/wakilisha-charts.js', [], self::VER, true);
    }

    public function maybe_enqueue_assets(){
        $enqueue = false;
        if (is_singular(self::CPT) || is_post_type_archive(self::CPT)) $enqueue = true;
        $p = get_post();
        if (get_query_var('artist_slug') || get_query_var('artist_id')) $enqueue = true;
        if (
            $p && (
                has_shortcode($p->post_content ?? '', 'waki_chart') ||
                has_shortcode($p->post_content ?? '', 'waki_charts_archive') ||
                has_shortcode($p->post_content ?? '', 'waki_artist')
            )
        ) {
            $enqueue = true;
        }

        if ($enqueue){
            wp_enqueue_style(self::SLUG);
            wp_enqueue_script(self::SLUG);
        }
    }

    /* ===== Admin Menus ===== */
    public function admin_menu(){
        add_menu_page(__('WAKILISHA Charts', 'wakilisha-charts'), __('WAKI Charts', 'wakilisha-charts'), 'manage_options', self::SLUG, [$this,'render_charts_page'], 'dashicons-chart-line',56);
        add_menu_page(__('WAKI Artists', 'wakilisha-charts'), __('WAKI Artists', 'wakilisha-charts'), 'manage_options', self::SLUG.'_artists', [$this,'render_artists_page'], 'dashicons-admin-users',57);
        add_submenu_page(self::SLUG, __('Charts', 'wakilisha-charts'), __('Charts', 'wakilisha-charts'), 'manage_options', self::SLUG, [$this,'render_charts_page']);
        add_submenu_page(self::SLUG, __('Settings (API & Global)', 'wakilisha-charts'), __('Settings (API & Global)', 'wakilisha-charts'), 'manage_options', self::SLUG.'_settings', [$this,'render_settings_page']);
    }

    /* ===== Settings (API/global) ===== */
    public function render_settings_page(){
        if (!current_user_can('manage_options')) return;
        wp_enqueue_media();
        if (!empty($_POST['clear_last_error'])){
            check_admin_referer(self::SLUG.'_clear_last_error');
            delete_option(self::SLUG.'_last_error');
        }
        $saved=false; $purged=false; $purge_msg='';
        if (!empty($_POST[self::SLUG.'_save'])){
            check_admin_referer(self::SLUG.'_settings');
            $opts = $this->get_options();
            $opts['client_id']       = sanitize_text_field($_POST['client_id'] ?? '');
            $opts['client_secret']   = sanitize_text_field($_POST['client_secret'] ?? '');
            $opts['market']          = strtoupper(sanitize_text_field($_POST['market'] ?? 'KE'));
            $opts['alpha']           = (string)floatval($_POST['alpha'] ?? '1.0');
            $opts['beta']            = (string)floatval($_POST['beta'] ?? '0.5');
            $opts['gamma']           = (string)floatval($_POST['gamma'] ?? '0.3');
            $opts['presence_bonus']  = (string)floatval($_POST['presence_bonus'] ?? '0.5');
            $opts['auto_make_post']  = isset($_POST['auto_make_post']) ? '1' : '0';
            $opts['post_category']   = sanitize_text_field($_POST['post_category'] ?? 'Charts');
            $opts['archive_hero_img']= esc_url_raw($_POST['archive_hero_img'] ?? '');
            $opts['hero_img_size']   = sanitize_key($_POST['hero_img_size'] ?? 'full');
            $intro                   = sanitize_textarea_field($_POST['archive_intro'] ?? '');
            update_option(self::OPTS,$opts);
            update_option(self::ARCHIVE_INTRO, $intro);
            $saved=true;
        }

        if (!empty($_POST[self::SLUG.'_purge'])){
            check_admin_referer(self::SLUG.'_purge');
            global $wpdb; $wpdb->query("TRUNCATE {$this->table}");
            $purged=true; $purge_msg=__('All chart data flushed.', 'wakilisha-charts');
        }

        if (!empty($_POST[self::SLUG.'_purge_one']) && !empty($_POST['purge_combo'])){
            check_admin_referer(self::SLUG.'_purge_one');
            list($p_slug,$p_date,$p_sid) = array_pad(explode('|', sanitize_text_field($_POST['purge_combo']),3),3,'');
            if($p_slug && $p_date && $p_sid){
                global $wpdb; $wpdb->delete($this->table,['chart_key'=>$p_slug,'chart_date'=>$p_date,'snapshot_id'=>$p_sid],['%s','%s','%s']);
                $purged=true; $purge_msg=sprintf(__('Chart rows for %1$s (%2$s) flushed.', 'wakilisha-charts'), $p_slug, $p_date);
            }
        }

        $opts = $this->get_options();
        $last_error = get_option(self::SLUG.'_last_error','');
        $reset_done = !empty($_GET[self::SLUG.'_reset_done']);
        ?>
        <div class="wrap">
          <h1><?php esc_html_e('WAKILISHA — Settings (API & Global)', 'wakilisha-charts'); ?></h1>
          <?php if($saved): ?><div class="updated"><p><?php esc_html_e('Settings saved.', 'wakilisha-charts'); ?></p></div><?php endif; ?>
          <?php if($purged): ?><div class="updated"><p><?php echo esc_html($purge_msg); ?></p></div><?php endif; ?>
          <?php if($reset_done): ?><div class="updated"><p><?php esc_html_e('Archive page reset.', 'wakilisha-charts'); ?></p></div><?php endif; ?>
          <?php if($last_error): ?>
          <div class="notice notice-error is-dismissible">
            <p><strong><?php esc_html_e('Last error:', 'wakilisha-charts'); ?></strong> <?php echo esc_html($last_error);?></p>
            <form method="post">
              <?php wp_nonce_field(self::SLUG.'_clear_last_error'); ?>
              <input type="hidden" name="clear_last_error" value="1">
              <button class="button"><?php esc_html_e('Clear', 'wakilisha-charts'); ?></button>
            </form>
          </div>
          <?php endif; ?>

          <form method="post"><?php wp_nonce_field(self::SLUG.'_settings'); ?>
            <h2><?php esc_html_e('API', 'wakilisha-charts'); ?></h2>
            <table class="form-table">
              <tr><th><?php esc_html_e('Client ID', 'wakilisha-charts'); ?></th><td><input class="regular-text" name="client_id" value="<?php echo esc_attr($opts['client_id']);?>" required></td></tr>
              <tr><th><?php esc_html_e('Client Secret', 'wakilisha-charts'); ?></th><td><input class="regular-text" type="password" name="client_secret" value="<?php echo esc_attr($opts['client_secret']);?>" required></td></tr>
              <tr><th><?php esc_html_e('Default Market', 'wakilisha-charts'); ?></th><td><input class="regular-text" name="market" value="<?php echo esc_attr($opts['market']);?>" maxlength="2"> <span class="description"><?php esc_html_e('ISO 3166-1 alpha-2 (availability only)', 'wakilisha-charts'); ?></span></td></tr>
            </table>

            <h2><?php esc_html_e('Scoring (Popularity-centric)', 'wakilisha-charts'); ?></h2>
            <table class="form-table">
              <tr><th><?php esc_html_e('α (popularity weight)', 'wakilisha-charts'); ?></th><td><input name="alpha" value="<?php echo esc_attr($opts['alpha']);?>" class="small-text"></td></tr>
              <tr><th><?php esc_html_e('β (playlist rank weight)', 'wakilisha-charts'); ?></th><td><input name="beta" value="<?php echo esc_attr($opts['beta']);?>" class="small-text"></td></tr>
              <tr><th><?php esc_html_e('γ (recency weight)', 'wakilisha-charts'); ?></th><td><input name="gamma" value="<?php echo esc_attr($opts['gamma']);?>" class="small-text"></td></tr>
              <tr><th><?php esc_html_e('Presence bonus', 'wakilisha-charts'); ?></th><td><input name="presence_bonus" value="<?php echo esc_attr($opts['presence_bonus']);?>" class="small-text"></td></tr>
            </table>

            <h2><?php esc_html_e('Posts (cron default)', 'wakilisha-charts'); ?></h2>
            <table class="form-table">
              <tr><th><?php esc_html_e('Auto publish', 'wakilisha-charts'); ?></th><td><label><input type="checkbox" name="auto_make_post" <?php checked($opts['auto_make_post'],'1');?> > <?php esc_html_e('Yes', 'wakilisha-charts'); ?></label></td></tr>
              <tr><th><?php esc_html_e('Category', 'wakilisha-charts'); ?></th><td><input class="regular-text" name="post_category" value="<?php echo esc_attr($opts['post_category']);?>"></td></tr>
            </table>

            <h2><?php esc_html_e('Archive', 'wakilisha-charts'); ?></h2>
            <table class="form-table">
              <tr>
                <th><?php esc_html_e('Intro Text', 'wakilisha-charts'); ?></th>
                <td>
                  <?php $intro = get_option(self::ARCHIVE_INTRO, $this->default_archive_intro()); ?>
                  <textarea name="archive_intro" class="large-text" rows="3"><?php echo esc_textarea($intro); ?></textarea>
                </td>
              </tr>
              <tr>
                <th><?php esc_html_e('Archive Hero Image', 'wakilisha-charts'); ?></th>
                <td>
                  <input id="waki_archive_hero_img" class="regular-text" name="archive_hero_img" value="<?php echo esc_attr($opts['archive_hero_img']);?>">
                  <button class="button waki-upload-hero"><?php esc_html_e('Select Image', 'wakilisha-charts'); ?></button>
                  <p class="description"><?php esc_html_e('Use a 16:9 image to fill the hero area.', 'wakilisha-charts'); ?></p>
                </td>
              </tr>
              <tr>
                <th><?php esc_html_e('Hero Image Size', 'wakilisha-charts'); ?></th>
                <td>
                  <?php $sizes = get_intermediate_image_sizes(); $sizes[] = 'full'; ?>
                  <select name="hero_img_size">
                    <?php foreach($sizes as $s): ?>
                      <option value="<?php echo esc_attr($s); ?>" <?php selected($opts['hero_img_size'],$s); ?>><?php echo esc_html($s); ?></option>
                    <?php endforeach; ?>
                  </select>
                  <span class="description"><?php esc_html_e('Applied to archive and single chart heroes.', 'wakilisha-charts'); ?></span>
                </td>
              </tr>
            </table>

            <p>
              <button class="button button-primary" name="<?php echo esc_attr(self::SLUG.'_save');?>" value="1"><?php esc_html_e('Save Settings', 'wakilisha-charts'); ?></button>
              &nbsp;<a class="button" href="<?php echo esc_url( admin_url('admin.php?page='.self::SLUG) ); ?>"><?php esc_html_e('Go to Charts', 'wakilisha-charts'); ?></a>
            </p>
          </form>

          <hr>
          <h2><?php esc_html_e('Danger Zone', 'wakilisha-charts'); ?></h2>
          <?php $reset_url = wp_nonce_url(add_query_arg(self::SLUG.'_reset_archive','1'), self::SLUG.'_reset_archive'); ?>
          <p><a class="button waki-reset-archive" href="<?php echo esc_url($reset_url); ?>"><?php esc_html_e('Reset Archive Page', 'wakilisha-charts'); ?></a></p>
          <form method="post" class="waki-purge-form">
            <?php wp_nonce_field(self::SLUG.'_purge_one'); ?>
            <p>
              <select name="purge_combo">
                <option value=""><?php esc_html_e('Select chart edition…', 'wakilisha-charts'); ?></option>
                <?php
                  global $wpdb;
                  $editions = $wpdb->get_results("SELECT DISTINCT chart_key, chart_date, snapshot_id FROM {$this->table} ORDER BY chart_date DESC", ARRAY_A);
                  foreach($editions as $ed){
                    $val = $ed['chart_key'].'|'.$ed['chart_date'].'|'.$ed['snapshot_id'];
                    echo '<option value="'.esc_attr($val).'">'.esc_html($ed['chart_key'].' — '.$ed['chart_date'])."</option>";
                  }
                ?>
              </select>
              <button class="button waki-purge-one" name="<?php echo esc_attr(self::SLUG.'_purge_one'); ?>" value="1"><?php esc_html_e('Flush Selected Chart Data', 'wakilisha-charts'); ?></button>
            </p>
          </form>
          <form method="post" class="waki-purge-all-form">
            <?php wp_nonce_field(self::SLUG.'_purge'); ?>
            <p><button class="button waki-purge-all" name="<?php echo esc_attr(self::SLUG.'_purge'); ?>" value="1"><?php esc_html_e('Flush All Chart Data', 'wakilisha-charts'); ?></button></p>
          </form>
        </div>
        <script>
        jQuery(function($){
          var frame;
          $('.waki-upload-hero').on('click',function(e){
            e.preventDefault();
            if(frame){ frame.open(); return; }
            frame = wp.media({title:'<?php echo esc_js(__('Select Image', 'wakilisha-charts')); ?>', library:{type:'image'}, multiple:false});
            frame.on('select', function(){
              var url = frame.state().get('selection').first().toJSON().url;
              $('#waki_archive_hero_img').val(url);
            });
            frame.open();
          });
          $('.waki-purge-all').on('click',function(e){
            if(!confirm('<?php echo esc_js(__('This will permanently delete all chart data. This action cannot be undone. Continue?', 'wakilisha-charts')); ?>')) e.preventDefault();
          });
          $('.waki-purge-one').on('click',function(e){
            var sel = $(this).closest('form').find('select[name=purge_combo]');
            if(!sel.val()){ e.preventDefault(); alert('<?php echo esc_js(__('Please select a chart edition to flush.', 'wakilisha-charts')); ?>'); return; }
            if(!confirm('<?php echo esc_js(__('This will permanently delete the selected chart data. Continue?', 'wakilisha-charts')); ?>')) e.preventDefault();
          });
          $('.waki-reset-archive').on('click',function(e){
            if(!confirm('<?php echo esc_js(__('This will recreate the archive page. Continue?', 'wakilisha-charts')); ?>')) e.preventDefault();
          });
        });
        </script>
        <?php
    }

    /* ===== Charts page (add/edit/run) ===== */
    public function render_charts_page(){
        if (!current_user_can('manage_options')) return;
        if (!empty($_POST['clear_last_error'])){
            check_admin_referer(self::SLUG.'_clear_last_error');
            delete_option(self::SLUG.'_last_error');
        }
        $charts = $this->get_charts();
        $opts   = $this->get_options();
        $last_error = get_option(self::SLUG.'_last_error','');

        $editing = null;
        if (isset($_GET['edit_chart'])) { $slug=$this->normalize_slug($_GET['edit_chart']); if(isset($charts[$slug])) $editing=$charts[$slug]; }

        // Save/create chart
        if (!empty($_POST[self::SLUG.'_save_chart'])) {
            check_admin_referer(self::SLUG.'_save_chart');
            $slug_in  = $this->normalize_slug($_POST['chart_slug'] ?? '');
            $source   = in_array(($_POST['chart_source'] ?? 'playlists'), ['playlists','release_window'], true) ? $_POST['chart_source'] : 'playlists';

            // New: album release date filter (applies to playlists & release_window)
            $filter_from = $this->safe_date($_POST['filter_release_from'] ?? '');
            $filter_to   = $this->safe_date($_POST['filter_release_to'] ?? '');

            $charts[$slug_in] = [
                'slug'             => $slug_in,
                'title'            => sanitize_text_field($_POST['chart_title'] ?? 'Untitled Chart'),
                'market'           => strtoupper(sanitize_text_field($_POST['chart_market'] ?? ($opts['market'] ?: 'KE'))),
                'source_type'      => $source,
                // playlists mode
                'playlist_multi'   => ($source==='playlists') ? wp_kses_post($_POST['chart_playlists'] ?? '') : '',
                'playlist_weights' => ($source==='playlists') ? sanitize_text_field($_POST['chart_weights'] ?? '') : '',
                'fallback_playlists' => ($source==='playlists') ? wp_kses_post($_POST['chart_fallback_playlists'] ?? '') : '',
                'fallback_weights'   => ($source==='playlists') ? sanitize_text_field($_POST['chart_fallback_weights'] ?? '') : '',
                // release-window mode
                'release_from'     => ($source==='release_window') ? $this->safe_date($_POST['chart_from'] ?? '') : '',
                'release_to'       => ($source==='release_window') ? $this->safe_date($_POST['chart_to'] ?? '')   : '',
                'origin_filter'    => $this->valid_iso($_POST['origin_filter'] ?? ''),

                // common filters
                'filter_release_from' => $filter_from,
                'filter_release_to'   => $filter_to,

                // manual chart date
                'chart_date'       => $this->safe_date($_POST['chart_date'] ?? ''),

                // common
                'chart_limit'      => max(10, min(200, intval($_POST['chart_limit'] ?? 100))),
                'auto_make_post'   => isset($_POST['auto_make_post'])?'1':'0',
                'post_category'    => sanitize_text_field($_POST['post_category'] ?? $opts['post_category']),
                // meta
                'last_run'         => $charts[$slug_in]['last_run'] ?? '',
                'last_snapshot'    => $charts[$slug_in]['last_snapshot'] ?? '',
                'last_chart_date'  => $charts[$slug_in]['last_chart_date'] ?? '',
            ];
            $this->put_charts($charts);
            echo '<div class="updated"><p>Chart '.esc_html($slug_in).' saved.</p></div>';
            $editing = $charts[$slug_in];
        }

        $fv = [
            'slug'   => $editing['slug']   ?? '',
            'title'  => $editing['title']  ?? '',
            'market' => $editing['market'] ?? ($opts['market'] ?? 'KE'),
            'src'    => $editing['source_type'] ?? 'playlists',
            'pl'     => $editing['playlist_multi']   ?? '',
            'wt'     => $editing['playlist_weights'] ?? '',
            'fbpl'   => $editing['fallback_playlists'] ?? '',
            'fbwt'   => $editing['fallback_weights'] ?? '',
            'from'   => $editing['release_from']     ?? '',
            'to'     => $editing['release_to']       ?? '',
            'of'     => $editing['origin_filter']    ?? '',
            'chart_date' => $editing['chart_date']   ?? '',
            'limit'  => $editing['chart_limit']      ?? 100,
            'auto'   => $editing['auto_make_post']   ?? '1',
            'cat'    => $editing['post_category']    ?? ($opts['post_category'] ?? 'Charts'),
            'f_rel_from' => $editing['filter_release_from'] ?? '',
            'f_rel_to'   => $editing['filter_release_to']   ?? '',
        ];

        // Pre-validate for UI buttons (blocks actions if invalid)
        $precheck = $this->validate_chart_config($fv);

        $weights_preview = $this->parse_weights_map($fv['wt']);
        $weights_err     = $this->validate_weights_syntax($fv['wt'], $this->normalize_many($fv['pl']));
        $fb_weights_preview = $this->parse_weights_map($fv['fbwt']);
        $fb_weights_err     = $this->validate_weights_syntax($fv['fbwt'], $this->normalize_many($fv['fbpl']));
        ?>
        <div class="wrap">
          <h1>WAKILISHA — Charts</h1>
          <?php if($last_error): ?>
          <div class="notice notice-error is-dismissible">
            <p><strong>Last error:</strong> <?php echo esc_html($last_error); ?></p>
            <form method="post">
              <?php wp_nonce_field(self::SLUG.'_clear_last_error'); ?>
              <input type="hidden" name="clear_last_error" value="1">
              <button class="button">Clear</button>
            </form>
          </div>
          <?php endif; ?>

          <h2 style="display:flex;align-items:center;gap:10px"><?php echo $editing ? 'Edit Chart: '.esc_html($fv['slug']) : 'Add a Chart'; ?>
            <span class="waki-badge <?php echo $precheck['ok']?'ok':'bad'; ?>"><?php echo $precheck['ok']?'Ready':'Needs fixes'; ?></span>
          </h2>

          <style>
            .waki-section{background:#fff;border:1px solid #e5e7eb;border-radius:12px;margin:12px 0;padding:16px}
            .waki-grid{display:grid;grid-template-columns:1.4fr 1fr;gap:18px}
            .waki-help{font-size:12px;opacity:.8;margin-top:4px}
            .waki-badge{border-radius:999px;padding:4px 8px;font-size:12px;font-weight:700}
            .waki-badge.ok{background:#ecfdf5;color:#065f46;border:1px solid #a7f3d0}
            .waki-badge.bad{background:#fff7ed;color:#9a3412;border:1px solid #fed7aa}
            .waki-inline-err{color:#b91c1c;font-weight:700}
            .waki-pills{display:flex;flex-wrap:wrap;gap:6px}
            .waki-pill{border:1px solid #e5e7eb;background:#f8fafc;border-radius:999px;padding:2px 8px;font-size:12px}
            .waki-st{display:flex;align-items:center;gap:8px;margin:6px 0}
            .waki-st .dot{width:8px;height:8px;border-radius:999px;background:#d1d5db}
            .waki-st.ok .dot{background:#10b981}
            .waki-st.bad .dot{background:#ef4444}
            .waki-box{background:#f9fafb;border:1px dashed #e5e7eb;padding:10px;border-radius:8px}
          </style>

          <form method="post">
            <?php wp_nonce_field(self::SLUG.'_save_chart'); ?>

            <div class="waki-section">
              <h3>Basics</h3>
              <div class="waki-grid">
                <div>
                  <table class="form-table">
                    <tr><th>Chart Slug</th><td><input name="chart_slug" class="regular-text" value="<?php echo esc_attr($fv['slug']);?>" <?php echo $editing?'readonly':''; ?> placeholder="kenya-top-50" required></td></tr>
                    <tr><th>Chart Title</th><td><input name="chart_title" class="regular-text" value="<?php echo esc_attr($fv['title']);?>" placeholder="WAKILISHA — Kenya Top 50" required></td></tr>
                    <tr><th>Market</th><td><input name="chart_market" class="regular-text" maxlength="2" value="<?php echo esc_attr($fv['market']);?>"><div class="waki-help">ISO 3166-1 alpha-2 (availability only)</div></td></tr>
                    <tr><th>Chart Date</th><td><input name="chart_date" class="regular-text" value="<?php echo esc_attr($fv['chart_date']);?>" placeholder="YYYY-MM-DD"><div class="waki-help">Optional; defaults to today</div></td></tr>
                    <tr>
                      <th>Source Type</th>
                      <td>
                        <label><input type="radio" name="chart_source" value="playlists" <?php checked($fv['src'],'playlists');?> > Playlists</label>&nbsp;
                        <label><input type="radio" name="chart_source" value="release_window" <?php checked($fv['src'],'release_window');?> > Release Window (Search)</label>
                      </td>
                    </tr>
                  </table>
                </div>
                <div>
                  <div class="waki-box">
                    <strong>Inline examples</strong>
                    <ul style="margin-top:6px">
                      <li>Playlist ID formats: <code>37i9dQZF1DX0XUsuxWHRQd</code>, <code>platform:playlist:…</code>, or full open URL.</li>
                      <li>Per-playlist weights: <code>37i9dQZF…=1.2, 6xkWQ2…=0.8</code></li>
                      <li>Release date range: <code>2024-01-01</code> → <code>2024-12-31</code></li>
                    </ul>
                  </div>
                </div>
              </div>
            </div>

            <div class="waki-section">
              <h3>Sources & Filters</h3>
              <table class="form-table">
                <tr class="row-pl"><th>Playlists (one per line)</th><td><textarea name="chart_playlists" rows="5" class="large-text" placeholder="37i9dQZF1DWZdKbfDnTWVN&#10;37i9dQZF1DWYkaDif7Ztbp"><?php echo esc_textarea($fv['pl']);?></textarea><div class="waki-help">Each line: playlist ID, platform URI, or open URL.</div></td></tr>
                <tr class="row-pl"><th>Fallback Playlists</th><td><textarea name="chart_fallback_playlists" rows="3" class="large-text" placeholder="37i9dQZF1DX4Wsb4d7NKfP"><?php echo esc_textarea($fv['fbpl']);?></textarea><div class="waki-help">Used if primary playlists yield too few tracks or invalid positions.</div></td></tr>
                <tr class="row-pl"><th>Fallback weights</th>
                  <td>
                    <input name="chart_fallback_weights" class="regular-text" value="<?php echo esc_attr($fv['fbwt']);?>" placeholder="ID1=1.0,ID2=0.5">
                    <?php if($fb_weights_err): ?><div class="waki-inline-err">Weights syntax: <?php echo esc_html($fb_weights_err);?></div><?php endif; ?>
                    <?php if($fb_weights_preview): ?>
                      <div class="waki-pills" style="margin-top:6px">
                        <?php foreach($fb_weights_preview as $pid=>$w){ echo '<span class="waki-pill">'.esc_html($pid).' = '.esc_html($w).'</span>'; } ?>
                      </div>
                    <?php endif; ?>
                  </td>
                </tr>
                <tr class="row-pl"><th>Per-playlist weights</th>
                  <td>
                    <input name="chart_weights" class="regular-text" value="<?php echo esc_attr($fv['wt']);?>" placeholder="ID1=1.2,ID2=0.8">
                    <?php if($weights_err): ?><div class="waki-inline-err">Weights syntax: <?php echo esc_html($weights_err);?></div><?php endif; ?>
                    <?php if($weights_preview): ?>
                      <div class="waki-pills" style="margin-top:6px">
                        <?php foreach($weights_preview as $pid=>$w){ echo '<span class="waki-pill">'.esc_html($pid).' = '.esc_html($w).'</span>'; } ?>
                      </div>
                    <?php endif; ?>
                  </td>
                </tr>

                <tr class="row-rw"><th>Release Window — From</th><td><input name="chart_from" class="regular-text" value="<?php echo esc_attr($fv['from']);?>" placeholder="2025-01-01"></td></tr>
                <tr class="row-rw"><th>Release Window — To</th><td><input name="chart_to" class="regular-text" value="<?php echo esc_attr($fv['to']);?>" placeholder="2025-01-31"></td></tr>

                <tr><th>Artist Origin Filter (ISO-2)</th><td><?php echo $this->render_iso_dropdown('origin_filter',$fv['of']); ?></td></tr>

                <tr><th>Album Release Date Filter</th>
                  <td>
                    <input name="filter_release_from" class="regular-text" value="<?php echo esc_attr($fv['f_rel_from']);?>" placeholder="YYYY-MM-DD"> →
                    <input name="filter_release_to" class="regular-text" value="<?php echo esc_attr($fv['f_rel_to']);?>" placeholder="YYYY-MM-DD">
                    <div class="waki-help">Applied to tracks gathered from playlists or search.</div>
                  </td>
                </tr>
              </table>
            </div>

            <div class="waki-section">
              <h3>Output</h3>
              <table class="form-table">
                <tr><th>Chart Size (entries)</th><td><input name="chart_limit" class="small-text" type="number" min="10" max="200" value="<?php echo intval($fv['limit']);?>"></td></tr>
                <tr><th>Auto-publish (cron)</th><td><label><input type="checkbox" name="auto_make_post" <?php checked($fv['auto'],'1');?> > Yes</label></td></tr>
                <tr><th>Category</th><td><input name="post_category" class="regular-text" value="<?php echo esc_attr($fv['cat']);?>"></td></tr>
              </table>
            </div>

            <p>
              <button class="button button-primary" name="<?php echo esc_attr(self::SLUG.'_save_chart');?>" value="1"><?php echo $editing?'Save Changes':'Save Chart';?></button>
              <?php if($editing): ?>&nbsp;<a class="button" href="<?php echo esc_url( remove_query_arg(['edit_chart']) ); ?>">Cancel</a><?php endif; ?>
            </p>
          </form>

          <?php if($editing): ?>
          <div class="waki-section">
            <h3>Pipeline — State & Actions</h3>
            <?php
              $state = [
                ['label'=>'Idle','ok'=>true],
                ['label'=>'Validating','ok'=>$precheck['ok']],
                ['label'=>'Inspecting','ok'=>false], // becomes true after Dry Run
                ['label'=>'Ready','ok'=>false],
              ];
            ?>
            <div>
              <?php foreach($state as $i=>$st): ?>
                <div class="waki-st <?php echo $st['ok']?'ok':'bad'; ?>"><span class="dot"></span><?php echo esc_html(($i+1).'. '.$st['label']); ?></div>
              <?php endforeach; ?>
            </div>
            <p style="margin-top:8px">
              <?php
                $run_disabled_attr = $precheck['ok'] ? '' : 'disabled';
                $run_title = $precheck['ok'] ? '' : 'title="Fix validation errors first"';
              ?>
              <a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_dry_run'=>$fv['slug']]), self::SLUG.'_dry_run' ) ); ?>">Dry Run (Validate & Preview)</a>
              <a class="button" <?php echo $run_disabled_attr.' '.$run_title; ?> href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_run_chart'=>$fv['slug'],'draft'=>'1']), self::SLUG.'_run_chart' ) ); ?>">Run now (Draft)</a>
              <a class="button" <?php echo $run_disabled_attr.' '.$run_title; ?> href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_run_chart'=>$fv['slug'],'draft'=>'0']), self::SLUG.'_run_chart' ) ); ?>">Run now (Publish)</a>
            </p>
            <?php if(!$precheck['ok']): ?>
              <div class="waki-box">
                <strong>Why validation fails:</strong>
                <ul>
                  <?php foreach($precheck['errors'] as $e) echo '<li class="waki-inline-err">'.esc_html($e).'</li>'; ?>
                  <?php if($weights_err) echo '<li class="waki-inline-err">'.esc_html($weights_err).'</li>'; ?>
                </ul>
              </div>
            <?php endif; ?>
          </div>
          <?php endif; ?>

<?php if ($editing && isset($_GET['dry'])):
    $tkey = self::SLUG.'_dry_'.$fv['slug'].'_'.get_current_user_id();
    $report = get_transient($tkey);
    if ($report){
        $this->render_dry_run_html($fv['slug'], $report);
        delete_transient($tkey);
    } else {
        echo '<div class="notice notice-warning"><p>Dry run report not found or has expired. Please run again.</p></div>';
    }
endif; ?>

          <hr>
          <h2>Existing Charts</h2>
          <?php $chartsAll=$this->get_charts();
          if(!$chartsAll): ?>
            <p>No charts yet. Add one above.</p>
          <?php else: ?>
            <table class="widefat striped">
              <thead><tr><th>Slug</th><th>Title</th><th>Source</th><th>Market</th><th>Limit</th><th>Last Run</th><th>Chart Date</th><th>Actions</th></tr></thead>
              <tbody>
              <?php foreach($chartsAll as $c): ?>
                <tr>
                  <td><code><?php echo esc_html($c['slug']);?></code></td>
                  <td><?php echo esc_html($c['title']);?></td>
                  <td><?php echo esc_html($c['source_type'] ?? 'playlists');?></td>
                  <td><?php echo esc_html($c['market']);?></td>
                  <td><?php echo intval($c['chart_limit'] ?? 100);?></td>
                  <td><?php echo esc_html($c['last_run'] ?: '—'); ?></td>
                  <td><?php echo esc_html($c['last_chart_date'] ?: '—'); ?></td>
                  <td>
                    <a class="button" href="<?php echo esc_url( add_query_arg(['edit_chart'=>$c['slug']]) ); ?>">Edit</a>
                    <a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_dry_run'=>$c['slug']]), self::SLUG.'_dry_run' ) ); ?>">Dry Run</a>
                    <a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_run_chart'=>$c['slug'],'draft'=>'1']), self::SLUG.'_run_chart' ) ); ?>">Run (Draft)</a>
                    <a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_run_chart'=>$c['slug'],'draft'=>'0']), self::SLUG.'_run_chart' ) ); ?>">Run (Publish)</a>
                    <a class="button button-link-delete" href="<?php echo esc_url( wp_nonce_url( add_query_arg([self::SLUG.'_delete_chart'=>$c['slug']]), self::SLUG.'_delete_chart' ) ); ?>" onclick="return confirm('Delete this chart definition? Existing data remains.')">Delete</a>
                  </td>
                </tr>
              <?php endforeach; ?>
              </tbody>
            </table>
          <?php endif; ?>
        </div>
        <script>
        (function(){
          const form = document.querySelector('form');
          if(!form) return;
          function toggle(){
            const src = form.querySelector('[name="chart_source"]:checked')?.value || 'playlists';
            form.querySelectorAll('.row-pl').forEach(n=>n.style.display = (src==='playlists'?'table-row':'none'));
            form.querySelectorAll('.row-rw').forEach(n=>n.style.display = (src==='release_window'?'table-row':'none'));
          }
          form.addEventListener('change', e=>{
            if(e.target.name==='chart_source') toggle();
          });
          toggle();
        })();
        </script>
        <?php
    }

    /* ===== Dry Run (validate & preview) ===== */
    public function handle_dry_run_action(){
        if (!current_user_can('manage_options')) return;
        if (!isset($_GET[self::SLUG.'_dry_run'])) return;

        check_admin_referer(self::SLUG.'_dry_run');
        $slug   = $this->normalize_slug($_GET[self::SLUG.'_dry_run']);
        $charts = $this->get_charts();
        if (!isset($charts[$slug])) {
            wp_safe_redirect(remove_query_arg([self::SLUG.'_dry_run','_wpnonce'])); exit;
        }

        $report = $this->dry_run_chart($slug, $charts[$slug]);
        $key = self::SLUG.'_dry_'.$slug.'_'.get_current_user_id();
        set_transient($key, $report, 10 * MINUTE_IN_SECONDS);

        $url = add_query_arg(['page'=>self::SLUG,'edit_chart'=>$slug,'dry'=>1], admin_url('admin.php'));
        wp_safe_redirect($url); exit;
    }

    private function validate_chart_config($fv){
        $errors = [];
        if (!$fv['slug']) $errors[]='Missing slug';
        if (!$fv['title']) $errors[]='Missing title';
        if (!preg_match('/^[A-Z]{2}$/',$fv['market'])) $errors[]='Market must be ISO-2';
        if ($fv['src']==='playlists'){
            $pls = $this->normalize_many($fv['pl']);
            if (count($pls)<1) $errors[]='Add at least one playlist';
        } else {
            if (!$this->safe_date($fv['from']) || !$this->safe_date($fv['to'])) $errors[]='Provide release window (from/to)';
        }
        if (($fv['f_rel_from'] && !$this->safe_date($fv['f_rel_from'])) || ($fv['f_rel_to'] && !$this->safe_date($fv['f_rel_to']))) {
            $errors[]='Album release date filter must be YYYY-MM-DD';
        }
        if ($fv['f_rel_from'] && $fv['f_rel_to'] && $fv['f_rel_from'] > $fv['f_rel_to']) $errors[]='Album release filter: From must be ≤ To';
        if ($fv['chart_date']) {
            $sd = $this->safe_date($fv['chart_date']);
            if (!$sd) {
                $errors[]='Chart Date must be YYYY-MM-DD';
            } elseif ($sd > $this->nairobi_today_date()) {
                $errors[]='Chart Date cannot be in the future';
            }
        }
        return ['ok'=>empty($errors), 'errors'=>$errors];
    }

    private function validate_weights_syntax($csv,$playlist_ids){
        if (trim($csv)==='') return '';
        $pairs = array_filter(array_map('trim', explode(',',$csv)));
        $ids = array_flip((array)$playlist_ids);
        foreach($pairs as $p){
            if (strpos($p,'=')===false) return "Bad pair “$p” (expected ID=weight)";
            [$k,$v] = array_map('trim', explode('=',$p,2));
            $kid = $this->normalize_playlist_id($k);
            if (!$kid) return "Invalid playlist id in “$p”";
            if (!isset($ids[$kid])) return "Weight for unknown playlist “$kid”";
            if (!is_numeric($v)) return "Non-numeric weight in “$p”";
        }
        return '';
    }

    /* ===== Artists Admin ===== */
    public function render_artists_page(){
        if (!current_user_can('manage_options')) return;

        $notice = '';

        $edit_id = isset($_GET['edit_artist']) ? sanitize_text_field($_GET['edit_artist']) : '';
        $edit_row = null;
        if($edit_id){
            global $wpdb;
            $edit_row = $wpdb->get_row($wpdb->prepare("SELECT * FROM {$this->artist_table} WHERE artist_id=%s", $edit_id), ARRAY_A);
        }

        // Query params / filters
        $q             = isset($_GET['q']) ? sanitize_text_field($_GET['q']) : '';
        $only_missing  = isset($_GET['only_missing']) ? 1 : 0; // missing origin
        $origin        = $this->valid_iso($_GET['origin'] ?? '');
        $min_pop       = isset($_GET['min_pop']) ? max(0, min(100, intval($_GET['min_pop']))) : '';
        $min_followers = isset($_GET['min_followers']) ? max(0, intval($_GET['min_followers'])) : '';
        $has_image     = isset($_GET['has_image']) ? 1 : 0;
        $has_bio       = isset($_GET['has_bio']) ? 1 : 0;

        // Actions: seed from rows (legacy helper)
        if (!empty($_GET['seed_from_rows']) && check_admin_referer(self::SLUG.'_seed')){
            $this->seed_artists_from_rows();
            $notice='Backfilled from chart rows.';
        }

        // Manual sync of all artists present in charts (full profile meta)
        if (!empty($_GET['sync_all']) && check_admin_referer(self::SLUG.'_sync')){
            $n = $this->sync_all_artists_from_charts();
            $notice = 'Synced profile meta for '.intval($n).' artists present in charts.';
        }

        // Fetch from external APIs for enriched fields
        if (!empty($_GET['fetch_api']) && check_admin_referer(self::SLUG.'_fetch_api')){
            $n = $this->fetch_api_for_all_artists();
            $notice = 'Fetched API data for '.intval($n).' artists.';
        }

        // Pagination
        $paged = max(1, intval($_GET['paged'] ?? 1));
        $per_page = 40;
        $offset = ($paged-1)*$per_page;

        // Build WHERE from filters
        global $wpdb;
        $where = "WHERE status <> 'trash'";
        $params = [];
        if ($q){
            $where .= " AND (artist_name LIKE %s OR artist_id LIKE %s OR genres LIKE %s)";
            $like = '%'.$wpdb->esc_like($q).'%';
            $params[]=$like; $params[]=$like; $params[]=$like;
        }
        if ($only_missing){
            $where .= " AND (origin_country IS NULL OR origin_country='')";
        }
        if ($origin){
            $where .= " AND origin_country = %s";
            $params[] = $origin;
        }
        if ($min_pop !== ''){
            $where .= " AND popularity >= %d";
            $params[] = intval($min_pop);
        }
        if ($min_followers !== ''){
            $where .= " AND followers >= %d";
            $params[] = intval($min_followers);
        }
        if ($has_image){
            $where .= " AND image_url <> ''";
        }
        if ($has_bio){
            $where .= " AND biography IS NOT NULL AND biography <> ''";
        }

        $sql_base = "FROM {$this->artist_table} $where";
        $total = $params ? intval($wpdb->get_var($wpdb->prepare("SELECT COUNT(*) $sql_base",$params))) : intval($wpdb->get_var("SELECT COUNT(*) $sql_base"));
        $missing = intval($wpdb->get_var("SELECT COUNT(*) FROM {$this->artist_table} WHERE (origin_country IS NULL OR origin_country='') AND status<>'trash'"));

        // Fetch rows
        $rows = $params
                ? $wpdb->get_results($wpdb->prepare("SELECT * $sql_base ORDER BY artist_name ASC LIMIT %d OFFSET %d", array_merge($params, [$per_page,$offset])), ARRAY_A)
                : $wpdb->get_results($wpdb->prepare("SELECT * $sql_base ORDER BY artist_name ASC LIMIT %d OFFSET %d", $per_page,$offset), ARRAY_A);

        $total_pages = max(1, intval(ceil($total/$per_page)));

        ?>
        <div class="wrap">
          <h1>Artists Directory</h1>
          <?php if($notice): ?><div class="updated"><p><?php echo esc_html($notice);?></p></div><?php endif; ?>

          <?php if($edit_row):
            $preview_link = add_query_arg([
              'artist_id' => $edit_row['artist_id'],
              'preview' => 1,
              '_wpnonce' => wp_create_nonce('preview_artist_'.$edit_row['artist_id'])
            ], home_url('/artist/'.($edit_row['artist_slug'] ?: $edit_row['artist_id']).'/'));
          ?>
          <h2>Edit Artist</h2>
          <p><a href="<?php echo esc_url($preview_link); ?>" target="_blank">Preview</a></p>
          <form method="post" style="margin:15px 0">
            <?php wp_nonce_field(self::SLUG.'_edit_artist'); ?>
            <input type="hidden" name="artist_id" value="<?php echo esc_attr($edit_row['artist_id']); ?>">
            <table class="form-table"><tbody>
              <tr><th scope="row">Name</th><td><input type="text" name="artist_name" value="<?php echo esc_attr($edit_row['artist_name']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Origin</th><td><?php echo $this->render_iso_dropdown('origin_country',$edit_row['origin_country']); ?></td></tr>
              <tr><th scope="row">Genres</th><td><input type="text" name="genres" value="<?php echo esc_attr($edit_row['genres']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Image URL</th><td><input type="text" name="image_url" value="<?php echo esc_url($edit_row['image_url']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Profile URL</th><td><input type="text" name="profile_url" value="<?php echo esc_url($edit_row['profile_url']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Biography</th><td><textarea name="biography" rows="5" class="large-text"><?php echo esc_textarea($edit_row['biography']); ?></textarea></td></tr>
              <tr><th scope="row">Latest Release</th><td><input type="text" name="latest_release" value="<?php echo esc_attr($edit_row['latest_release']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Top Tracks (CSV)</th><td><input type="text" name="top_tracks" value="<?php echo esc_attr($edit_row['top_tracks']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Discography (JSON)</th><td><textarea name="discography" rows="3" class="large-text"><?php echo esc_textarea($edit_row['discography']); ?></textarea></td></tr>
              <tr><th scope="row">Chart Stats (JSON)</th><td><textarea name="chart_stats" rows="3" class="large-text"><?php echo esc_textarea($edit_row['chart_stats']); ?></textarea></td></tr>
              <tr><th scope="row">Video URLs (CSV)</th><td><input type="text" name="video_urls" value="<?php echo esc_attr($edit_row['video_urls']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Related Artist IDs (CSV)</th><td><input type="text" name="related_artist_ids" value="<?php echo esc_attr($edit_row['related_artist_ids']); ?>" class="regular-text"></td></tr>
              <tr><th scope="row">Status</th><td><select name="status">
                <option value="draft" <?php selected($edit_row['status'],'draft'); ?>>Draft</option>
                <option value="publish" <?php selected($edit_row['status'],'publish'); ?>>Published</option>
                <option value="trash" <?php selected($edit_row['status'],'trash'); ?>>Trashed</option>
              </select></td></tr>
            </tbody></table>
            <p>
              <button class="button button-primary" name="<?php echo esc_attr(self::SLUG.'_save_artist'); ?>" value="1">Save Artist</button>
              <a class="button" href="<?php echo esc_url(remove_query_arg('edit_artist')); ?>">Cancel</a>
            </p>
          </form>
          <?php endif; ?>

          <div class="notice notice-info"><p>
            <strong>Totals:</strong> <?php echo number_format_i18n($total); ?> artists;
            <strong>Missing origin:</strong> <?php echo number_format_i18n($missing); ?>
          </p></div>

          <p>
            <a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg(['seed_from_rows'=>1]), self::SLUG.'_seed' ) ); ?>" onclick="return confirm('Re-scan chart rows and upsert artists?')">Backfill from chart rows</a>
            <a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg(['fetch_api'=>1]), self::SLUG.'_fetch_api' ) ); ?>" onclick="return confirm('Fetch data from APIs for all artists?')">Fetch from API</a>
            <a class="button button-primary" href="<?php echo esc_url( wp_nonce_url( add_query_arg(['sync_all'=>1]), self::SLUG.'_sync' ) ); ?>" onclick="return confirm('Fetch and store profile meta for all artists present in any chart rows?')">Sync all artists from charts (profile meta)</a>
          </p>

          <form method="get" style="margin:10px 0">
            <input type="hidden" name="page" value="<?php echo esc_attr(self::SLUG.'_artists'); ?>">
            <input type="search" name="q" placeholder="Search name, ID or genre…" value="<?php echo esc_attr($q); ?>">
            &nbsp;Origin: <?php echo $this->render_iso_dropdown('origin',$origin); ?>
            &nbsp;Min Pop: <input type="number" name="min_pop" min="0" max="100" value="<?php echo esc_attr($min_pop); ?>" class="small-text">
            &nbsp;Min Followers: <input type="number" name="min_followers" min="0" value="<?php echo esc_attr($min_followers); ?>" class="small-text">
            &nbsp;<label><input type="checkbox" name="only_missing" value="1" <?php checked($only_missing,1); ?>> Missing origin</label>
            &nbsp;<label><input type="checkbox" name="has_image" value="1" <?php checked($has_image,1); ?>> Has image</label>
            &nbsp;<label><input type="checkbox" name="has_bio" value="1" <?php checked($has_bio,1); ?>> Has biography</label>
            <button class="button">Filter</button>
            &nbsp;<a class="button" href="<?php echo esc_url(remove_query_arg(['q','origin','min_pop','min_followers','only_missing','has_image','has_bio','paged'])); ?>">Reset</a>
            &nbsp;<a class="button" href="<?php echo esc_url( wp_nonce_url( add_query_arg(array_merge($_GET, [self::SLUG.'_export'=>1])), self::SLUG.'_export' ) ); ?>">Export CSV (filtered)</a>
          </form>

          <form method="post" enctype="multipart/form-data" style="margin:10px 0">
            <?php wp_nonce_field(self::SLUG.'_import_data'); ?>
            <label>Import file (CSV or JSON): <input type="file" name="waki_file" accept=".csv,.json" required></label>
            <button class="button">Import</button>
            <span class="description">Fields: artist_id (required) plus artist_name, origin_country, biography, latest_release, top_tracks, discography, chart_stats, video_urls, related_artist_ids.</span>
          </form>

          <form method="post" id="waki-artists-bulk">
            <?php wp_nonce_field(self::SLUG.'_bulk_artists'); ?>
            <table class="widefat striped">
              <thead>
                <tr>
                  <th style="width:26px"><input type="checkbox" onclick="document.querySelectorAll('.waki-chk').forEach(c=>c.checked=this.checked)"></th>
                  <th>Artist</th><th>Artist ID</th><th>Origin</th><th>Status</th><th>Followers</th><th>Pop</th><th>Genres</th><th>Image</th>
                </tr>
              </thead>
              <tbody>
              <?php if(!$rows): ?>
                <tr><td colspan="9">No artists found. Try syncing or backfilling.</td></tr>
              <?php else: foreach($rows as $r): ?>
                <tr>
                  <td><input type="checkbox" class="waki-chk" name="sel[]" value="<?php echo esc_attr($r['artist_id']); ?>"></td>
                  <td>
                    <?php $preview_link = add_query_arg([
                        'artist_id' => $r['artist_id'],
                        'preview' => 1,
                        '_wpnonce' => wp_create_nonce('preview_artist_'.$r['artist_id'])
                    ], home_url('/artist/'.($r['artist_slug'] ?: $r['artist_id']).'/')); ?>
                    <strong><?php echo esc_html($r['artist_name']); ?></strong> <a href="<?php echo esc_url(add_query_arg('edit_artist',$r['artist_id'])); ?>">Edit</a> | <a href="<?php echo esc_url($preview_link); ?>" target="_blank">Preview</a>
                    <?php if(!empty($r['biography'])): ?>
                      <div style="color:#666;font-size:12px;max-width:480px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><?php echo esc_html($r['biography']); ?></div>
                    <?php endif; ?>
                  </td>
                  <td><code><?php echo esc_html($r['artist_id']); ?></code></td>
                  <td><?php echo $this->render_iso_dropdown("origin[{$r['artist_id']}]", $r['origin_country']); ?></td>
                  <td><?php echo esc_html($r['status']); ?></td>
                  <td><?php echo number_format_i18n(intval($r['followers'])); ?></td>
                  <td><?php echo intval($r['popularity']); ?></td>
                  <td><?php echo esc_html($r['genres']); ?></td>
                  <td><?php if(!empty($r['image_url'])) echo '<img src="'.esc_url($r['image_url']).'" style="width:36px;height:36px;object-fit:cover;border-radius:4px" alt="">'; ?></td>
                </tr>
              <?php endforeach; endif; ?>
              </tbody>
            </table>
            <p>
              <button class="button button-primary" name="<?php echo esc_attr(self::SLUG.'_save_artists');?>" value="1">Save Origin Changes</button>
              <button class="button" name="<?php echo esc_attr(self::SLUG.'_publish_artists');?>" value="1">Publish selected</button>
              <button class="button" name="<?php echo esc_attr(self::SLUG.'_trash_artists');?>" value="1" onclick="return confirm('Trash selected artist pages?');">Trash selected</button>
            </p>
          </form>

          <?php if($total_pages>1): ?>
            <div class="tablenav"><div class="tablenav-pages">
              <?php
                echo paginate_links([
                    'base' => add_query_arg('paged','%#%'),
                    'format' => '',
                    'prev_text' => '«',
                    'next_text' => '»',
                    'total' => $total_pages,
                    'current' => $paged,
                ]);
              ?>
            </div></div>
          <?php endif; ?>
        </div>
        <?php
    }

    public function handle_artist_actions(){
        if (!current_user_can('manage_options')) return;

        // Export CSV (filtered via GET)
        if (!empty($_GET[self::SLUG.'_export'])){
            check_admin_referer(self::SLUG.'_export');
            $this->export_artists_csv();
            exit;
        }

        // Import file (CSV or JSON)
        if (!empty($_FILES['waki_file']) && isset($_POST['_wpnonce']) && wp_verify_nonce($_POST['_wpnonce'], self::SLUG.'_import_data')){
            $this->import_artists_data($_FILES['waki_file']);
            wp_safe_redirect(remove_query_arg([self::SLUG.'_export','_wpnonce'], add_query_arg(['page'=>self::SLUG.'_artists'], admin_url('admin.php')))); exit;
        }

        // Save single artist metadata edits
        if (!empty($_POST[self::SLUG.'_save_artist'])){
            check_admin_referer(self::SLUG.'_edit_artist');
            $aid = sanitize_text_field($_POST['artist_id'] ?? '');
            if($aid){
                global $wpdb;
                $name = sanitize_text_field($_POST['artist_name'] ?? '');
                $data = [
                    'artist_name'       => $name,
                    'artist_slug'       => $this->generate_artist_slug($name, $aid),
                    'origin_country'    => $this->valid_iso($_POST['origin_country'] ?? '') ?: null,
                    'genres'            => sanitize_text_field($_POST['genres'] ?? ''),
                    'image_url'         => esc_url_raw($_POST['image_url'] ?? ''),
                    'profile_url'       => esc_url_raw($_POST['profile_url'] ?? ''),
                    'biography'         => wp_kses_post($_POST['biography'] ?? ''),
                    'latest_release'    => sanitize_text_field($_POST['latest_release'] ?? ''),
                    'top_tracks'        => sanitize_text_field($_POST['top_tracks'] ?? ''),
                    'discography'       => wp_kses_post($_POST['discography'] ?? ''),
                    'chart_stats'       => wp_kses_post($_POST['chart_stats'] ?? ''),
                    'video_urls'        => sanitize_text_field($_POST['video_urls'] ?? ''),
                    'related_artist_ids'=> sanitize_text_field($_POST['related_artist_ids'] ?? ''),
                    'status'            => in_array($_POST['status'] ?? 'draft', ['draft','publish','trash'], true) ? $_POST['status'] : 'draft',
                    'updated_at'        => current_time('mysql',1),
                ];
                $wpdb->update(
                    $this->artist_table,
                    $data,
                    ['artist_id'=>$aid],
                    ['%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'],
                    ['%s']
                );
            }
            wp_safe_redirect(remove_query_arg('edit_artist', add_query_arg(['page'=>self::SLUG.'_artists'], admin_url('admin.php')))); exit;
        }

        // Save origins bulk
        if (!empty($_POST[self::SLUG.'_save_artists'])){
            check_admin_referer(self::SLUG.'_bulk_artists');
            $origin = $_POST['origin'] ?? [];
            if (is_array($origin)){
                global $wpdb;
                foreach($origin as $aid=>$code){
                    $aid  = sanitize_text_field($aid);
                    $code = $this->valid_iso($code);
                    if(!$aid) continue;
                    $wpdb->update($this->artist_table,
                        ['origin_country' => ($code?:null), 'updated_at'=>current_time('mysql',1)],
                        ['artist_id'=>$aid],
                        ['%s','%s'], ['%s']
                    );
                }
            }
            wp_safe_redirect(add_query_arg(['page'=>self::SLUG.'_artists'], admin_url('admin.php'))); exit;
        }

        // Publish selected
        if (!empty($_POST[self::SLUG.'_publish_artists'])){
            check_admin_referer(self::SLUG.'_bulk_artists');
            $sel = isset($_POST['sel']) && is_array($_POST['sel']) ? array_filter(array_map('sanitize_text_field', $_POST['sel'])) : [];
            if ($sel){
                global $wpdb;
                $in = implode(',', array_fill(0, count($sel), '%s'));
                $wpdb->query($wpdb->prepare("UPDATE {$this->artist_table} SET status='publish', updated_at=NOW() WHERE artist_id IN ($in)", $sel));
            }
            wp_safe_redirect(add_query_arg(['page'=>self::SLUG.'_artists'], admin_url('admin.php'))); exit;
        }

        // Trash selected
        if (!empty($_POST[self::SLUG.'_trash_artists'])){
            check_admin_referer(self::SLUG.'_bulk_artists');
            $sel = isset($_POST['sel']) && is_array($_POST['sel']) ? array_filter(array_map('sanitize_text_field', $_POST['sel'])) : [];
            if ($sel){
                global $wpdb;
                $in = implode(',', array_fill(0, count($sel), '%s'));
                $wpdb->query($wpdb->prepare("UPDATE {$this->artist_table} SET status='trash', updated_at=NOW() WHERE artist_id IN ($in)", $sel));
            }
            wp_safe_redirect(add_query_arg(['page'=>self::SLUG.'_artists'], admin_url('admin.php'))); exit;
        }

    }

    private function export_artists_csv(){
        if (!current_user_can('manage_options')) return;
        global $wpdb;

        // rebuild WHERE from current GET (same as render)
        $q             = isset($_GET['q']) ? sanitize_text_field($_GET['q']) : '';
        $only_missing  = isset($_GET['only_missing']) ? 1 : 0;
        $origin        = $this->valid_iso($_GET['origin'] ?? '');
        $min_pop       = isset($_GET['min_pop']) ? max(0, min(100, intval($_GET['min_pop']))) : '';
        $min_followers = isset($_GET['min_followers']) ? max(0, intval($_GET['min_followers'])) : '';
        $has_image     = isset($_GET['has_image']) ? 1 : 0;
        $has_bio       = isset($_GET['has_bio']) ? 1 : 0;

        $where = "WHERE status <> 'trash'"; $params=[];
        if ($q){
            $like = '%'.$wpdb->esc_like($q).'%';
            $where .= " AND (artist_name LIKE %s OR artist_id LIKE %s OR genres LIKE %s)";
            $params[]=$like; $params[]=$like; $params[]=$like;
        }
        if ($only_missing){ $where .= " AND (origin_country IS NULL OR origin_country='')"; }
        if ($origin){ $where .= " AND origin_country=%s"; $params[]=$origin; }
        if ($min_pop!==''){ $where .= " AND popularity >= %d"; $params[] = intval($min_pop); }
        if ($min_followers!==''){ $where .= " AND followers >= %d"; $params[] = intval($min_followers); }
        if ($has_image){ $where .= " AND image_url <> ''"; }
        if ($has_bio){ $where .= " AND biography IS NOT NULL AND biography <> ''"; }

        $sql = "SELECT artist_id, artist_name, origin_country, followers, popularity, genres, image_url, profile_url, biography, latest_release, top_tracks, discography, chart_stats, video_urls, related_artist_ids FROM {$this->artist_table} $where ORDER BY artist_name ASC";
        $rows = $params ? $wpdb->get_results($wpdb->prepare($sql, $params), ARRAY_A) : $wpdb->get_results($sql, ARRAY_A);

        nocache_headers();
        header('Content-Type: text/csv; charset=utf-8');
        header('Content-Disposition: attachment; filename=wakilisha_artists_'.date('Ymd_His').'.csv');

        $out = fopen('php://output', 'w');
        fputcsv($out, ['artist_id','artist_name','origin_country','followers','popularity','genres','image_url','profile_url','biography','latest_release','top_tracks','discography','chart_stats','video_urls','related_artist_ids']);
        foreach($rows as $r){
            fputcsv($out, [
                $r['artist_id'], $r['artist_name'], $r['origin_country'], $r['followers'], $r['popularity'],
                $r['genres'], $r['image_url'], $r['profile_url'], $r['biography'], $r['latest_release'], $r['top_tracks'], $r['discography'], $r['chart_stats'], $r['video_urls'], $r['related_artist_ids']
            ]);
        }
        fclose($out);
    }

    private function import_artists_data($file){
        if (!current_user_can('manage_options')) return 0;
        if (empty($file['tmp_name']) || !is_uploaded_file($file['tmp_name'])) return 0;

        $ext = strtolower(pathinfo($file['name'] ?? '', PATHINFO_EXTENSION));
        $rows = [];
        if($ext === 'json'){
            $json = file_get_contents($file['tmp_name']);
            $rows = json_decode($json, true);
            if(!is_array($rows)) return 0;
        } else {
            $handle = fopen($file['tmp_name'], 'r'); if(!$handle) return 0;
            $header = fgetcsv($handle); if(!$header){ fclose($handle); return 0; }
            $map = [];
            foreach($header as $i=>$h){ $map[strtolower(trim($h))] = $i; }
            while(($row = fgetcsv($handle)) !== false){
                $rows[] = [
                    'artist_id'        => $row[$map['artist_id'] ?? -1] ?? '',
                    'artist_name'      => $row[$map['artist_name'] ?? -1] ?? '',
                    'origin_country'   => $row[$map['origin_country'] ?? -1] ?? '',
                    'biography'        => $row[$map['biography'] ?? -1] ?? '',
                    'latest_release'   => $row[$map['latest_release'] ?? -1] ?? '',
                    'top_tracks'       => $row[$map['top_tracks'] ?? -1] ?? '',
                    'discography'      => $row[$map['discography'] ?? -1] ?? '',
                    'chart_stats'      => $row[$map['chart_stats'] ?? -1] ?? '',
                    'video_urls'       => $row[$map['video_urls'] ?? -1] ?? '',
                    'related_artist_ids'=> $row[$map['related_artist_ids'] ?? -1] ?? '',
                ];
            }
            fclose($handle);
        }

        $count = 0; global $wpdb;
        foreach($rows as $row){
            $artist_id = sanitize_text_field($row['artist_id'] ?? '');
            if(!$artist_id) continue;
            $artist_name = sanitize_text_field($row['artist_name'] ?? '');
            $origin      = $this->valid_iso($row['origin_country'] ?? '') ?: null;
            $slug        = $artist_name !== '' ? $this->generate_artist_slug($artist_name, $artist_id) : '';

            $data = [
                'updated_at'        => current_time('mysql',1),
            ];
            if($artist_name !== ''){ $data['artist_name'] = $artist_name; $data['artist_slug'] = $slug; }
            if($origin) $data['origin_country'] = $origin;
            foreach(['biography','latest_release','top_tracks','discography','chart_stats','video_urls','related_artist_ids'] as $f){
                if(isset($row[$f]) && $row[$f] !== '') $data[$f] = $f==='biography' || in_array($f,['discography','chart_stats']) ? wp_kses_post($row[$f]) : sanitize_text_field($row[$f]);
            }

            $exists = $wpdb->get_var($wpdb->prepare("SELECT COUNT(*) FROM {$this->artist_table} WHERE artist_id=%s", $artist_id));
            if($exists){
                $wpdb->update($this->artist_table, $data, ['artist_id'=>$artist_id]);
            } else {
                $insert = array_merge([
                    'artist_id'=>$artist_id,
                    'artist_slug'=>$slug,
                    'followers'=>0,'popularity'=>0,'genres'=>null,'image_url'=>'','profile_url'=>'',
                    'status'=>'draft',
                    'created_at'=>current_time('mysql',1)
                ], $data);
                $wpdb->insert($this->artist_table, $insert);
            }
            $count++;
        }
        return $count;
    }

    private function fetch_api_for_all_artists(){
        global $wpdb;
        $ids = $wpdb->get_col("SELECT artist_id FROM {$this->artist_table} WHERE status<>'trash'");
        $count = 0;
        foreach($ids as $id){
            $spotify = $this->fetch_spotify_data($id);
            $youtube = $this->fetch_youtube_videos($id);
            $data = [];
            if(is_array($spotify)){
                if(isset($spotify['biography'])) $data['biography'] = $spotify['biography'];
                if(isset($spotify['latest_release'])) $data['latest_release'] = $spotify['latest_release'];
                if(!empty($spotify['top_tracks'])) $data['top_tracks'] = implode(',', (array)$spotify['top_tracks']);
                if(!empty($spotify['discography'])) $data['discography'] = wp_json_encode($spotify['discography']);
                if(!empty($spotify['chart_stats'])) $data['chart_stats'] = wp_json_encode($spotify['chart_stats']);
                if(!empty($spotify['related_artist_ids'])) $data['related_artist_ids'] = implode(',', (array)$spotify['related_artist_ids']);
            }
            if(is_array($youtube) && !empty($youtube['video_urls'])){
                $data['video_urls'] = implode(',', (array)$youtube['video_urls']);
            }
            if($data){
                $data['updated_at'] = current_time('mysql',1);
                $wpdb->update($this->artist_table, $data, ['artist_id'=>$id]);
                $count++;
            }
        }
        return $count;
    }

    private function fetch_spotify_data($artist_id){
        $cache_key = 'waki_spotify_'.$artist_id;
        $cached = get_transient($cache_key);
        if($cached !== false) return $cached;
        $data = [];
        $artist = $this->api_request('GET', $this->api_base().'/v1/artists/'.rawurlencode($artist_id));
        if(!is_wp_error($artist)){
            $data['biography'] = $artist['bio'] ?? $artist['biography'] ?? '';
            $albums = $this->api_request('GET', $this->api_base().'/v1/artists/'.rawurlencode($artist_id).'/albums', ['limit'=>1,'include_groups'=>'album,single']);
            if(!is_wp_error($albums) && !empty($albums['items'][0])){
                $data['latest_release'] = $albums['items'][0]['name'] ?? '';
            }
            $top = $this->api_request('GET', $this->api_base().'/v1/artists/'.rawurlencode($artist_id).'/top-tracks', ['market'=>'US']);
            if(!is_wp_error($top)){
                $data['top_tracks'] = array_map(fn($t)=>$t['id'], $top['tracks'] ?? []);
            }
            $related = $this->api_request('GET', $this->api_base().'/v1/artists/'.rawurlencode($artist_id).'/related-artists');
            if(!is_wp_error($related)){
                $data['related_artist_ids'] = array_map(fn($a)=>$a['id'], $related['artists'] ?? []);
            }
        }
        set_transient($cache_key, $data, DAY_IN_SECONDS);
        return $data;
    }

    private function fetch_youtube_videos($artist_id){
        $cache_key = 'waki_youtube_'.$artist_id;
        $cached = get_transient($cache_key);
        if($cached !== false) return $cached;
        $data = [];
        $resp = wp_remote_get('https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=3&q='.urlencode($artist_id));
        if(!is_wp_error($resp)){
            $body = json_decode(wp_remote_retrieve_body($resp), true);
            if(!empty($body['items'])){
                $urls = [];
                foreach($body['items'] as $item){
                    $vid = $item['id']['videoId'] ?? '';
                    if($vid) $urls[] = 'https://www.youtube.com/watch?v='.$vid;
                }
                if($urls) $data['video_urls'] = $urls;
            }
        }
        set_transient($cache_key, $data, DAY_IN_SECONDS);
        return $data;
    }

    private function seed_artists_from_rows(){
        global $wpdb;
        $offset = 0;
        $limit = 20000;
        $map = [];
        do{
            $rows = $wpdb->get_results($wpdb->prepare(
                "SELECT artist_ids, artists FROM {$this->table}
                 WHERE artist_ids IS NOT NULL AND artist_ids<>'' LIMIT %d OFFSET %d",
                $limit, $offset
            ), ARRAY_A);
            if(!$rows) break;
            foreach($rows as $r){
                $ids = array_map('trim', explode(',', $r['artist_ids']));
                $ns  = array_map('trim', explode(',', $r['artists']));
                $len = max(count($ids), count($ns));
                for($i=0;$i<$len;$i++){
                    $aid = $ids[$i] ?? ''; $name = trim((string)($ns[$i] ?? ''));
                    if(!$aid) continue;
                    if($name && !isset($map[$aid])) $map[$aid] = $name; // rule: skip unnamed
                }
            }
            if($map){
                foreach(array_chunk($map, 200, true) as $chunk){
                    foreach($chunk as $aid=>$name){
                        if(!$name) continue; // rule
                        $slug = $this->generate_artist_slug($name, $aid);
                        $wpdb->query($wpdb->prepare(
                            "INSERT INTO {$this->artist_table} (artist_id, artist_name, artist_slug, origin_country, status, created_at, updated_at)
                             VALUES (%s,%s,%s,NULL,'draft',%s,%s)
                             ON DUPLICATE KEY UPDATE artist_name=VALUES(artist_name), artist_slug=VALUES(artist_slug)",
                            $aid, $name, $slug, current_time('mysql',1), current_time('mysql',1)
                        ));
                    }
                }
                $map = [];
            }
            $offset += $limit;
        } while(true);

        // cleanup unnamed (safety)
        $wpdb->query("DELETE FROM {$this->artist_table} WHERE artist_name IS NULL OR artist_name=''");
    }

    private function backfill_cover_meta(){
        global $wpdb;
        $posts = get_posts([
            'post_type'      => self::CPT,
            'post_status'    => 'any',
            'posts_per_page' => -1,
            'meta_query'     => [
                ['key' => '_waki_cover_url', 'compare' => 'NOT EXISTS'],
            ],
        ]);
        if(!$posts) return;
        $table = $this->table;
        foreach($posts as $p){
            $cid = $p->ID;
            $key = get_post_meta($cid,'_waki_chart_key',true);
            if(!$key) continue;
            $date = get_post_meta($cid,'_waki_chart_date',true);
            $sid  = get_post_meta($cid,'_waki_snapshot_id',true);
            $sql='';
            if($date && $sid){
                $sql = $wpdb->prepare("SELECT album_image_url FROM {$table} WHERE chart_key=%s AND chart_date=%s AND snapshot_id=%s AND position=1 LIMIT 1", $key, $date, $sid);
            } elseif($date){
                $sql = $wpdb->prepare("SELECT album_image_url FROM {$table} WHERE chart_key=%s AND chart_date=%s ORDER BY snapshot_id DESC, position ASC LIMIT 1", $key, $date);
            } else {
                $sql = $wpdb->prepare("SELECT album_image_url FROM {$table} WHERE chart_key=%s ORDER BY chart_date DESC, snapshot_id DESC, position ASC LIMIT 1", $key);
            }
            if($sql){
                $row = $wpdb->get_row($sql, ARRAY_A);
                if($row && !empty($row['album_image_url'])){
                    update_post_meta($cid,'_waki_cover_url',$row['album_image_url']);
                }
            }
        }
    }

    private function sync_all_artists_from_charts(){
        global $wpdb;
        $ids = $wpdb->get_col("SELECT DISTINCT TRIM(aid) FROM (
            SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(artist_ids, ',', n.n), ',', -1) AS aid
            FROM {$this->table}
            JOIN (
                SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL
                SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
            ) n ON n.n <= 1 + LENGTH(artist_ids) - LENGTH(REPLACE(artist_ids, ',', ''))
            WHERE artist_ids IS NOT NULL AND artist_ids <> ''
        ) x WHERE x.aid <> ''");
        if(!$ids) return 0;

        $meta = $this->fetch_artists_meta($ids);
        if (is_wp_error($meta)) return 0;
        $count=0;
        foreach($meta as $m){
            $this->upsert_artist_meta($m);
            $count++;
        }
        // cleanup unnamed just in case
        $wpdb->query("DELETE FROM {$this->artist_table} WHERE artist_name IS NULL OR artist_name=''");
        return $count;
    }

    /* ===== Utilities: ISO dropdown ===== */
    private function iso_list(){
        return [
            ''=>'— Not set —',
            'KE'=>'Kenya','UG'=>'Uganda','TZ'=>'Tanzania','RW'=>'Rwanda','BI'=>'Burundi','ET'=>'Ethiopia','SO'=>'Somalia','SS'=>'South Sudan',
            'NG'=>'Nigeria','GH'=>'Ghana','CI'=>'Côte d’Ivoire','SN'=>'Senegal','CM'=>'Cameroon','MZ'=>'Mozambique','ZA'=>'South Africa','ZW'=>'Zimbabwe','ZM'=>'Zambia','BW'=>'Botswana','NA'=>'Namibia','AO'=>'Angola','GA'=>'Gabon','CD'=>'Congo (DRC)','CG'=>'Congo (Rep.)','DZ'=>'Algeria','MA'=>'Morocco','TN'=>'Tunisia','EG'=>'Egypt',
            'US'=>'United States','CA'=>'Canada','MX'=>'Mexico','BR'=>'Brazil','AR'=>'Argentina','CL'=>'Chile','CO'=>'Colombia',
            'GB'=>'United Kingdom','IE'=>'Ireland','DE'=>'Germany','FR'=>'France','ES'=>'Spain','IT'=>'Italy','NL'=>'Netherlands','BE'=>'Belgium','SE'=>'Sweden','NO'=>'Norway','DK'=>'Denmark','FI'=>'Finland','PL'=>'Poland','PT'=>'Portugal','GR'=>'Greece','HU'=>'Hungary','CZ'=>'Czechia','AT'=>'Austria',
            'TR'=>'Türkiye','RU'=>'Russia','UA'=>'Ukraine',
            'SA'=>'Saudi Arabia','AE'=>'United Arab Emirates','QA'=>'Qatar','BH'=>'Bahrain','KW'=>'Kuwait','IL'=>'Israel','PS'=>'Palestine','LB'=>'Lebanon','JO'=>'Jordan',
            'IN'=>'India','PK'=>'Pakistan','BD'=>'Bangladesh','LK'=>'Sri Lanka','NP'=>'Nepal',
            'CN'=>'China','JP'=>'Japan','KR'=>'Korea (Rep.)','ID'=>'Indonesia','PH'=>'Philippines','TH'=>'Thailand','MY'=>'Malaysia','SG'=>'Singapore','VN'=>'Viet Nam','KH'=>'Cambodia',
            'AU'=>'Australia','NZ'=>'New Zealand'
        ];
    }
    private function render_iso_dropdown($name,$value){
        $list=$this->iso_list();
        $value = $this->valid_iso($value);
        $out = '<select name="'.esc_attr($name).'">';
        foreach($list as $code=>$label){
            $out .= '<option value="'.esc_attr($code).'"'.selected($value,$code,false).'>'.esc_html($label).'</option>';
        }
        $out .= '</select>';
        return $out;
    }

    private function render_dry_run_html($slug, $report){
        ?>
        <div class="waki-section">
          <h3>Dry Run — <?php echo esc_html($slug); ?></h3>

          <h4>Pipeline</h4>
          <ol>
            <?php foreach(($report['state_log'] ?? []) as $line): ?>
              <li><?php echo esc_html($line); ?></li>
            <?php endforeach; ?>
          </ol>

          <?php if(!empty($report['playlist_checks'])): ?>
            <h4>Playlist Validation</h4>
            <table class="widefat striped">
              <thead><tr><th>ID</th><th>Name</th><th>Owner</th><th>Public</th><th>Tracks</th><th>Status</th></tr></thead>
              <tbody>
              <?php foreach($report['playlist_checks'] as $row): ?>
                <tr>
                  <td><code><?php echo esc_html($row['id']); ?></code></td>
                  <td><?php echo esc_html($row['name'] ?: '—'); ?></td>
                  <td><?php echo esc_html($row['owner'] ?: '—'); ?></td>
                  <td><?php echo !empty($row['public']) ? 'Yes' : 'No'; ?></td>
                  <td><?php echo intval($row['tracks'] ?? 0); ?></td>
                  <td><?php echo esc_html($row['status'] ?? ''); ?></td>
                </tr>
              <?php endforeach; ?>
              </tbody>
            </table>
          <?php endif; ?>

          <h4>Applied Rules</h4>
          <ul>
            <?php foreach(($report['rules'] ?? []) as $k=>$v): ?>
              <?php if (is_array($v)): ?>
                <li><strong><?php echo esc_html($k); ?>:</strong>
                  <?php
                    $parts=[]; foreach($v as $kk=>$vv){ $parts[] = esc_html($kk.'='.$vv); }
                    echo $parts ? implode(', ',$parts) : '—';
                  ?>
                </li>
              <?php else: ?>
                <li><strong><?php echo esc_html($k); ?>:</strong> <?php echo esc_html($v === '' ? '—' : $v); ?></li>
              <?php endif; ?>
            <?php endforeach; ?>
          </ul>

          <h4>Aggregate Counts</h4>
          <table class="widefat striped">
            <tbody>
              <?php foreach(($report['counts'] ?? []) as $k=>$v): ?>
                <tr><th style="width:40%"><?php echo esc_html($k); ?></th><td><?php echo esc_html(is_numeric($v)? (string)intval($v) : (string)$v); ?></td></tr>
              <?php endforeach; ?>
            </tbody>
          </table>
          <?php if(!empty($report['parsed_meta'])): ?>
            <h4>Parsed Metadata</h4>
            <table class="widefat striped">
              <tbody>
                <tr><th>Countries</th><td><?php echo esc_html(implode(', ', $report['parsed_meta']['countries'] ?? [])); ?></td></tr>
                <tr><th>Region</th><td><?php echo esc_html($report['parsed_meta']['region'] ?: '—'); ?></td></tr>
                <tr><th>Genres</th><td><?php echo esc_html(implode(', ', $report['parsed_meta']['genres'] ?? [])); ?></td></tr>
                <tr><th>Languages</th><td><?php echo esc_html(implode(', ', $report['parsed_meta']['languages'] ?? [])); ?></td></tr>
                <tr><th>Format</th><td><?php echo esc_html($report['parsed_meta']['format'] ?: '—'); ?></td></tr>
                <tr><th>Country key</th><td><?php echo esc_html($report['parsed_meta']['country_key'] ?: '—'); ?></td></tr>
                <tr><th>Chart key</th><td><?php echo esc_html($report['parsed_meta']['chart_key'] ?: '—'); ?></td></tr>
                <tr><th>URL</th><td><?php if(!empty($report['parsed_meta']['url'])) echo '<a href="'.esc_url($report['parsed_meta']['url']).'" target="_blank">'.esc_html($report['parsed_meta']['url']).'</a>'; else echo '—'; ?></td></tr>
              </tbody>
            </table>
          <?php endif; ?>

          <?php if(!empty($report['dedupe_sample'])): ?>
            <h4>Deduplication Sample</h4>
            <p>Example of merged duplicates (same track across multiple playlists):</p>
            <ul>
              <?php foreach(array_slice($report['dedupe_sample'],0,5) as $ex): ?>
                <li><code><?php echo esc_html($ex['track_id']); ?></code> — playlists: <?php echo esc_html(implode(', ', $ex['playlists'])); ?></li>
              <?php endforeach; ?>
            </ul>
          <?php endif; ?>

          <?php if(!empty($report['top_preview'])): ?>
            <h4>Top 5 Preview</h4>
            <ol style="line-height:1.55">
              <?php foreach($report['top_preview'] as $r): ?>
                <li><strong><?php echo esc_html($r['track_name'] ?? ''); ?></strong> — <?php echo esc_html($r['artists'] ?? ''); ?>
                  <em>(score <?php echo esc_html(number_format((float)($r['score'] ?? 0), 3)); ?>)</em></li>
              <?php endforeach; ?>
            </ol>
          <?php endif; ?>

          <p><a class="button" href="<?php echo esc_url(remove_query_arg(['dry'])); ?>">Dismiss</a></p>
        </div>
        <?php
    }

    private function valid_iso($code){
        $code = strtoupper(trim((string)$code));
        return preg_match('/^[A-Z]{2}$/',$code) ? $code : '';
    }
    private function safe_date($y){
        $y = trim($y); if(!$y) return '';
        $dt = DateTime::createFromFormat('!Y-m-d', $y, new DateTimeZone(self::TZ));
        return $dt ? $dt->format('Y-m-d') : '';
    }

    private function parse_chart_meta($row){
        $split = function($str){
            return array_filter(array_map('trim', preg_split('/[\s,|]+/', (string)$str)));
        };
        $errors = [];

        $countries = [];
        foreach($split($row['countries'] ?? '') as $code){
            $code = strtolower($code);
            if(preg_match('/^[a-z]{2}$/',$code) && term_exists($code,'waki_country')){
                $countries[] = $code;
            }
        }
        $countries = array_values(array_unique($countries));
        if(count($countries) > 10){
            $errors[] = __('At most 10 unique countries allowed.','wakilisha-charts');
        }

        $region    = sanitize_title($row['region'] ?? '');
        $genres    = array_values(array_unique(array_map('sanitize_title', $split($row['genres'] ?? ''))));
        foreach($genres as $slug){
            if(!term_exists($slug,'waki_genre')){
                $errors[] = sprintf(__('Unknown genre “%s”.','wakilisha-charts'), $slug);
            }
        }
        $languages = array_values(array_unique(array_map('sanitize_title', $split($row['languages'] ?? ''))));
        foreach($languages as $slug){
            if(!term_exists($slug,'waki_language')){
                $errors[] = sprintf(__('Unknown language “%s”.','wakilisha-charts'), $slug);
            }
        }
        $format    = sanitize_title($row['format'] ?? '');

        $country_key = '';
        if($countries){
            $slugs = $countries; sort($slugs, SORT_STRING);
            $country_key = implode('-', $slugs);
            if(strlen($country_key) > 40){
                $errors[] = __('Country key cannot exceed 40 characters.','wakilisha-charts');
            }
        }elseif($region){
            $country_key = $region;
        }

        if($errors){ wp_die(implode('<br>', $errors)); }

        $base = $country_key ?: $region;
        $first_genre = $genres[0] ?? '';
        $parts = array_filter([$base, $first_genre, $format]);
        $chart_key = strtolower(implode('-', $parts));

        $date = $this->safe_date($row['chart_date'] ?? '');
        if($countries){
            $path = implode('/', array_filter([$country_key, $first_genre, $format]));
            $url = home_url('/'.self::CPT_SLUG.'/country/'.$path.'/'.($date ?: 'latest').'/');
        }elseif($region){
            $path = implode('/', array_filter([$region, $first_genre, $format]));
            $url = home_url('/'.self::CPT_SLUG.'/region/'.$path.'/'.($date ?: 'latest').'/');
        }else{
            $url = '';
        }

        return [
            'countries'=>$countries,
            'region'=>$region,
            'genres'=>$genres,
            'languages'=>$languages,
            'format'=>$format,
            'country_key'=>$country_key,
            'chart_key'=>$chart_key,
            'date'=>$date,
            'url'=>$url,
        ];
    }

    /* ===== Manual run shims ===== */
    public function handle_manual_run_legacy(){
        if (!current_user_can('manage_options')) return;
        if (isset($_GET[self::SLUG.'_run'])){
            check_admin_referer(self::SLUG.'_runnow');
            $charts = $this->get_charts();
            if (!isset($charts['default'])) {
                $opts = $this->get_options();
                $charts['default'] = [
                    'slug'=>'default',
                    'title'=>'WAKILISHA — Kenya Top 50',
                    'market'=>$opts['market'] ?: 'KE',
                    'source_type'=>'playlists',
                    'playlist_multi'=>'',
                    'playlist_weights'=>'',
                    'fallback_playlists'=>'',
                    'fallback_weights'=>'',
                    'chart_date'=>'',
                    'chart_limit'=>100,
                    'auto_make_post'=>$opts['auto_make_post'] ?: '1',
                    'post_category'=>$opts['post_category'] ?: 'Charts',
                ];
                $this->put_charts($charts);
            }
            $this->ingest_and_compute_chart('default',$charts['default'], true, true);
            wp_safe_redirect(remove_query_arg([self::SLUG.'_run','_wpnonce'])); exit;
        }
    }

    public function handle_archive_reset_action(){
        if (!current_user_can('manage_options')) return;
        if (isset($_GET[self::SLUG.'_reset_archive'])) {
            check_admin_referer(self::SLUG.'_reset_archive');
            $this->reset_archive_page();
            $redirect = remove_query_arg([self::SLUG.'_reset_archive','_wpnonce']);
            $redirect = add_query_arg(self::SLUG.'_reset_done', '1', $redirect);
            wp_safe_redirect($redirect); exit;
        }
    }

    public function cli_reset_archive($args, $assoc_args){
        $this->reset_archive_page();
        \WP_CLI::success('Archive page reset.');
    }
    public function handle_charts_actions(){
        if (!current_user_can('manage_options')) return;
        if (isset($_GET[self::SLUG.'_run_chart'])) {
            check_admin_referer(self::SLUG.'_run_chart');
            $slug = $this->normalize_slug($_GET[self::SLUG.'_run_chart']);
            $charts = $this->get_charts();
            if (isset($charts[$slug])) {
                // Gate: validate before running
                $c = $charts[$slug];
                $pre = $this->validate_chart_config([
                    'slug'=>$c['slug'],'title'=>$c['title'],'market'=>$c['market'],
                    'src'=>$c['source_type'],
                    'pl'=>$c['playlist_multi'] ?? '',
                    'from'=>$c['release_from'] ?? '',
                    'to'=>$c['release_to'] ?? '',
                    'f_rel_from'=>$c['filter_release_from'] ?? '',
                    'f_rel_to'=>$c['filter_release_to'] ?? '',
                    'chart_date'=>$c['chart_date'] ?? ''
                ]);
                if(!$pre['ok']){
                    update_option(self::SLUG.'_last_error','Cannot run — fix validation errors in chart config first.');
                    wp_safe_redirect(remove_query_arg([self::SLUG.'_run_chart','draft','_wpnonce'])); exit;
                }
                // run
                $as_draft = (isset($_GET['draft']) && $_GET['draft']==='1');
                $this->ingest_and_compute_chart($slug, $charts[$slug], true, $as_draft);
                $charts[$slug]['last_run'] = current_time('mysql'); // local (option/UI)
                $charts[$slug]['last_chart_date'] = $this->get_latest_chart_date($slug) ?: '';
                $charts[$slug]['last_snapshot'] = get_option(self::OPTS)['last_snapshot'] ?? '';
                $this->put_charts($charts);
            }
            wp_safe_redirect(remove_query_arg([self::SLUG.'_run_chart','draft','_wpnonce'])); exit;
        }
        if (isset($_GET[self::SLUG.'_delete_chart'])) {
            check_admin_referer(self::SLUG.'_delete_chart');
            $slug = $this->normalize_slug($_GET[self::SLUG.'_delete_chart']);
            $charts = $this->get_charts();
            if (isset($charts[$slug])) { unset($charts[$slug]); $this->put_charts($charts); }
            wp_safe_redirect(remove_query_arg([self::SLUG.'_delete_chart','_wpnonce'])); exit;
        }
    }

    /* ===== Cron driver ===== */
    public function cron_run_all_charts(){
        $charts = $this->get_charts();
        foreach($charts as $slug=>$conf){
            $this->ingest_and_compute_chart($slug, $conf, false, false);
        }
    }

    /* ===== API ===== */
    private function api_base(){
        return apply_filters('waki_chart_api_base', self::API_BASE);
    }
    private function get_access_token(){
        $cached = get_transient('waki_chart_access_token');
        if ($cached) return $cached;
        $opts = $this->get_options();
        if (!$opts['client_id'] || !$opts['client_secret']){
            add_action('admin_notices',function(){ echo '<div class="error"><p>'.esc_html__('Client ID/secret not configured.', 'wakilisha-charts').'</p></div>'; });
            error_log('[WAKI Charts] Missing Client ID/secret for authentication');
            return new WP_Error('missing_creds',__('Client ID/secret not configured.', 'wakilisha-charts'));
        }
        if(!$this->maybe_throttle()) return new WP_Error('throttle','Rate limited');
        $res = wp_remote_post(self::AUTH_URL, [
            'headers'=>[
                'Authorization'=>'Basic '.base64_encode($opts['client_id'].':'.$opts['client_secret']),
                'Content-Type'=>'application/x-www-form-urlencoded'
            ],
            'body'=>['grant_type'=>'client_credentials'],
            'timeout'=>20
        ]);
        if (is_wp_error($res)){
            error_log('[WAKI Charts] Auth request error: '.self::AUTH_URL.' — '.$res->get_error_message());
            add_action('admin_notices',function() use ($res){ echo '<div class="error"><p>'.sprintf(esc_html__('Authentication request failed: %s', 'wakilisha-charts'), esc_html($res->get_error_message())).'</p></div>'; });
            return $res;
        }
        $code = wp_remote_retrieve_response_code($res);
        $body_raw = wp_remote_retrieve_body($res);
        $body = json_decode($body_raw, true);
        if ($code==429){
            $ra=intval(wp_remote_retrieve_header($res,'retry-after'));
            $this->maybe_throttle(max(1,$ra));
            return new WP_Error('throttle','Rate limited');
        }
        if ($code!==200 || empty($body['access_token'])){
            error_log('[WAKI Charts] Auth failed: '.self::AUTH_URL.' — '.$code.' — '.substr((string)$body_raw,0,300));
            add_action('admin_notices',function(){ echo '<div class="error"><p>'.esc_html__('Authentication with Spotify failed.', 'wakilisha-charts').'</p></div>'; });
            return new WP_Error('auth_fail','Auth failed');
        }
        $token = $body['access_token'];
        $ttl = max(60, intval($body['expires_in'] ?? 3600) - 30);
        set_transient('waki_chart_access_token',$token,$ttl);
        return $token;
    }
    private function maybe_throttle($retry_after = 0){
        $bucket = get_transient('waki_chart_tokens');
        $now    = time();
        $capacity = 10; // max tokens
        $rate     = 10; // refill rate per second
        if(!is_array($bucket)){
            $bucket = ['tokens' => $capacity, 'ts' => $now];
        }else{
            $elapsed = $now - intval($bucket['ts']);
            if($elapsed > 0){
                $bucket['tokens'] = min($capacity, $bucket['tokens'] + $elapsed * $rate);
                $bucket['ts']     = $now;
            }
        }
        if($retry_after > 0){
            $bucket['tokens'] = 0;
            set_transient('waki_chart_tokens', $bucket, MINUTE_IN_SECONDS);
            wp_schedule_single_event(time() + max(1, intval($retry_after)), self::CRON_HOOK);
            return false;
        }
        if($bucket['tokens'] < 1){
            set_transient('waki_chart_tokens', $bucket, MINUTE_IN_SECONDS);
            wp_schedule_single_event(time() + 1, self::CRON_HOOK);
            return false;
        }
        $bucket['tokens'] -= 1;
        set_transient('waki_chart_tokens', $bucket, MINUTE_IN_SECONDS);
        return true;
    }
    private function api_request($method,$url,$query=[],$retry=3){
        $token = $this->get_access_token(); if(is_wp_error($token)) return $token;
        if(!empty($query)){ $qs = http_build_query($query); $url .= (strpos($url,'?')===false?'?':'&').$qs; }
        $args=['method'=>$method,'headers'=>['Authorization'=>'Bearer '.$token,'Accept'=>'application/json'],'timeout'=>25];
        $attempts=0;
        while($attempts<$retry){
            $attempts++;
            if(!$this->maybe_throttle()) return new WP_Error('throttle','Rate limited');
            $res = wp_remote_request($url,$args);
            if(is_wp_error($res)){
                if($attempts>=$retry){
                    error_log('[WAKI Charts] API request error: '.$url.' — '.$res->get_error_message());
                    add_action('admin_notices',function() use ($res){ echo '<div class="error"><p>'.sprintf(esc_html__('Spotify API request failed: %s', 'wakilisha-charts'), esc_html($res->get_error_message())).'</p></div>'; });
                    return $res;
                }
                $this->maybe_throttle(1);
                return new WP_Error('throttle','Temporary request failure');
            }
            $code = wp_remote_retrieve_response_code($res);
            $body_raw = wp_remote_retrieve_body($res);
            $body = json_decode($body_raw,true);

            if ($code==429){
                $ra = wp_remote_retrieve_header($res,'retry-after');
                $delay = is_numeric($ra) ? (int)$ra : max(1, strtotime($ra) - time());
                $delay = max(1,$delay);
                $this->maybe_throttle($delay);
                $key = 'waki_chart_api_retry_'.md5($method.$url.serialize($query).microtime(true));
                set_transient($key, ['method'=>$method,'url'=>$url,'query'=>$query,'retry'=>$retry], $delay + MINUTE_IN_SECONDS);
                wp_schedule_single_event(time()+$delay, self::RETRY_HOOK, [$key]);
                error_log('[WAKI Charts] Deferred API request due to 429: '.$url.'; retry in '.$delay.'s (key: '.$key.')');
                return new WP_Error('throttle','Rate limited');
            }
            if ($code==401 && $attempts<$retry){ delete_transient('waki_chart_access_token'); $token=$this->get_access_token();if(is_wp_error($token)) return $token; $args['headers']['Authorization']='Bearer '.$token; continue; }
            if ($code>=200 && $code<300) return is_array($body) ? $body : [];
            error_log('[WAKI Charts] API error: '.$url.' — '.$code.' — '.substr((string)$body_raw,0,300));
            add_action('admin_notices',function() use ($code){ echo '<div class="error"><p>'.sprintf(esc_html__('Spotify API returned an error (HTTP %d).', 'wakilisha-charts'), intval($code)).'</p></div>'; });
            return new WP_Error('api_error', sprintf('API error: %d — %s', $code, substr((string)$body_raw,0,300)));
        }
        error_log('[WAKI Charts] API request failed after retries: '.$url);
        add_action('admin_notices',function(){ echo '<div class="error"><p>'.esc_html__('Spotify API request failed after multiple attempts.', 'wakilisha-charts').'</p></div>'; });
        return new WP_Error('api_error','API request failed after retries.');
    }

    public function resume_api_request($key){
        $payload = get_transient($key);
        if(!$payload){
            error_log('[WAKI Charts] No deferred payload for key '.$key);
            return;
        }
        delete_transient($key);
        error_log('[WAKI Charts] Resuming deferred API request: '.$payload['url']);
        $this->api_request($payload['method'], $payload['url'], $payload['query'], $payload['retry']);
    }
    private function fetch_playlist_tracks($playlist_id,$market){
        $pid = $this->normalize_playlist_id($playlist_id);
        if(!$pid) return new WP_Error('bad_playlist','Invalid playlist id');

        $items=[]; $base=$this->api_base().'/v1/playlists/'.rawurlencode($pid).'/tracks';
        $params=['limit'=>100,'offset'=>0,'market'=>$market ?: 'KE'];

        do{
            $data=$this->api_request('GET',$base,$params);
            if(is_wp_error($data)) return $data;
            foreach(($data['items'] ?? []) as $it){
                $t=$it['track'] ?? null; if(!$t || empty($t['id']) || !empty($t['is_local'])) continue;
                $artists=[]; $artist_ids=[];
                foreach(($t['artists'] ?? []) as $a){
                    if(!empty($a['name'])) $artists[]=$a['name'];
                    if(!empty($a['id'])) $artist_ids[]=$a['id'];
                }
                $img=''; if(!empty($t['album']['images']) && is_array($t['album']['images'])){
                    usort($t['album']['images'], fn($a,$b)=>intval($b['width']??0)<=>intval($a['width']??0));
                    $img=$t['album']['images'][0]['url'] ?? '';
                }
                $isrc = $t['external_ids']['isrc'] ?? '';
                $alb_id = $t['album']['id'] ?? '';
                $items[]=[
                    'playlist_id'=>$pid,
                    'rank_in_src'=>count($items)+1,
                    'track_id'=>$t['id'],
                    'album_id'=>$alb_id,
                    'track_name'=>$t['name'] ?? '',
                    'artists'=>implode(', ',$artists),
                    'artist_ids'=>implode(',', array_values(array_unique($artist_ids))),
                    'duration_ms'=>intval($t['duration_ms'] ?? 0),
                    'album_image_url'=>$img,'album_release_date'=>$t['album']['release_date'] ?? '',
                    'added_at'=>!empty($it['added_at'])?gmdate('Y-m-d H:i:s', strtotime($it['added_at'].' UTC')):null,
                    'popularity'=>0,'isrc'=>$isrc,
                    'label'=>'',
                ];
            }
            $next=$data['next'] ?? null;
            if($next){ $base=$next; $params=[]; }
        } while(!empty($next));

        return $items;
    }

    private function fetch_albums_labels($album_ids){
        $album_ids = array_values(array_unique(array_filter($album_ids)));
        if(!$album_ids) return [];
        $map = [];
        foreach(array_chunk($album_ids, 20) as $batch){ // API max 20 per /albums
            $data = $this->api_request('GET', $this->api_base().'/v1/albums', ['ids'=>implode(',',$batch)]);
            if(is_wp_error($data)) return $data;
            foreach(($data['albums'] ?? []) as $a){
                if(empty($a['id'])) continue;
                $map[$a['id']] = sanitize_text_field($a['label'] ?? '');
            }
        }
        return $map;
    }

    private function validate_playlist_meta($playlist_id){
        $pid = $this->normalize_playlist_id($playlist_id);
        if(!$pid) return ['id'=>$playlist_id,'name'=>'','owner'=>'','public'=>false,'tracks'=>0,'status'=>'Invalid ID'];
        $data = $this->api_request('GET', $this->api_base().'/v1/playlists/'.rawurlencode($pid), ['fields'=>'name,public,owner(display_name),tracks(total)']);
        if(is_wp_error($data)) return ['id'=>$pid,'name'=>'','owner'=>'','public'=>false,'tracks'=>0,'status'=>'API error'];
        return [
            'id'=>$pid,
            'name'=>$data['name'] ?? '',
            'owner'=>$data['owner']['display_name'] ?? '',
            'public'=>!empty($data['public']),
            'tracks'=>intval($data['tracks']['total'] ?? 0),
            'status'=>'OK'
        ];
    }

    /* ===== NEW: fetch artists meta (followers, popularity, genres, image, url, biography) ===== */
    private function fetch_artists_meta($artist_ids){
        $ids = array_values(array_unique(array_filter((array)$artist_ids)));
        if(!$ids) return [];
        $out = [];
        foreach(array_chunk($ids, 50) as $chunk){
            $data = $this->api_request('GET', $this->api_base().'/v1/artists', ['ids'=>implode(',',$chunk)]);
            if(is_wp_error($data)) return $data;
            foreach(($data['artists'] ?? []) as $a){
                $id = $a['id'] ?? ''; if(!$id) continue;
                $name = trim((string)($a['name'] ?? ''));
                if($name==='') continue; // rule: skip unnamed
                $genres = implode(', ', array_map('strtolower', $a['genres'] ?? []));
                $img = '';
                if(!empty($a['images']) && is_array($a['images'])){
                    usort($a['images'], fn($x,$y)=>intval($y['width']??0)<=>intval($x['width']??0));
                    $img = $a['images'][0]['url'] ?? '';
                }
                $bio = $a['bio'] ?? $a['biography'] ?? ($a['profile']['biography']['text'] ?? '');
                $out[$id] = [
                    'artist_id'   => $id,
                    'artist_name' => $name,
                    'genres'      => $genres,
                    'followers'   => intval($a['followers']['total'] ?? 0),
                    'popularity'  => intval($a['popularity'] ?? 0),
                    'image_url'   => $img,
                    'profile_url' => $a['external_urls']['spotify'] ?? '',
                    'biography'   => is_string($bio) ? $bio : '',
                ];
            }
        }
        return $out;
    }

    private function upsert_artist_meta($meta){
        global $wpdb;
        if(empty($meta['artist_id']) || trim((string)($meta['artist_name'] ?? ''))==='') return;
        $slug = $this->generate_artist_slug($meta['artist_name'], $meta['artist_id']);
        $wpdb->query($wpdb->prepare(
            "INSERT INTO {$this->artist_table}
             (artist_id, artist_name, artist_slug, origin_country, followers, popularity, genres, image_url, profile_url, biography, status, created_at, updated_at)
             VALUES (%s,%s,%s,NULL,%d,%d,%s,%s,%s,%s,'draft',%s,%s)
             ON DUPLICATE KEY UPDATE
                artist_name=VALUES(artist_name),
                artist_slug=VALUES(artist_slug),
                followers=VALUES(followers),
                popularity=VALUES(popularity),
                genres=VALUES(genres),
                image_url=VALUES(image_url),
                profile_url=VALUES(profile_url),
                biography=VALUES(biography),
                updated_at=VALUES(updated_at)",
            $meta['artist_id'],
            $meta['artist_name'],
            $slug,
            intval($meta['followers'] ?? 0),
            intval($meta['popularity'] ?? 0),
            (string)($meta['genres'] ?? ''),
            (string)($meta['image_url'] ?? ''),
            (string)($meta['profile_url'] ?? ''),
            (string)($meta['biography'] ?? ''),
            current_time('mysql',1),
            current_time('mysql',1)
        ));
    }

    private function fetch_artists_genres($artist_ids){
        // kept for backward comps; not used directly now
        $artist_ids = array_values(array_unique(array_filter($artist_ids)));
        if(!$artist_ids) return [];
        $map=[];
        foreach(array_chunk($artist_ids,50) as $batch){
            $data=$this->api_request('GET', $this->api_base().'/v1/artists', ['ids'=>implode(',',$batch)]);
            if(is_wp_error($data)) return $data;
            foreach(($data['artists'] ?? []) as $a){
                $map[$a['id']] = array_map('strtolower', $a['genres'] ?? []);
            }
        }
        return $map;
    }

    private function enrich_tracks(&$items){
        $ids = array_values(array_unique(array_map(fn($r)=>$r['track_id'],$items)));
        if(!$ids) return true;

        // collect artist IDs once for meta
        $all_artist_ids = [];
        foreach($items as $it){
            foreach(explode(',', $it['artist_ids'] ?? '') as $aid){
                $aid = trim($aid); if($aid) $all_artist_ids[] = $aid;
            }
        }
        $all_artist_ids = array_values(array_unique($all_artist_ids));

        // Fetch and persist artist meta
        $artist_meta_map = $all_artist_ids ? $this->fetch_artists_meta($all_artist_ids) : [];
        if(is_wp_error($artist_meta_map)) return $artist_meta_map;
        foreach($artist_meta_map as $meta){ $this->upsert_artist_meta($meta); }

        // 1) /v1/tracks — popularity, preview, isrc, duration, album_id, etc.
        foreach(array_chunk($ids,50) as $batch){
            $data = $this->api_request('GET', $this->api_base().'/v1/tracks', ['ids'=>implode(',',$batch)]);
            if(is_wp_error($data)) return $data;

            $map = [];
            foreach(($data['tracks'] ?? []) as $t){
                if(empty($t['id'])) continue;
                $map[$t['id']] = [
                    'popularity'  => intval($t['popularity'] ?? 0),
                    'duration_ms' => intval($t['duration_ms'] ?? 0),
                    'isrc'        => $t['external_ids']['isrc'] ?? '',
                    'artists'     => $t['artists'] ?? [],
                    'name'        => $t['name'] ?? '',
                    'album_id'    => $t['album']['id'] ?? '',
                ];
            }

            foreach($items as &$it){
                $id = $it['track_id'];
                if(isset($map[$id])){
                    $it['popularity']   = $map[$id]['popularity'];
                    $it['duration_ms']  = $map[$id]['duration_ms'] ?: ($it['duration_ms'] ?? 0);
                    if(empty($it['isrc']) && !empty($map[$id]['isrc'])) $it['isrc'] = $map[$id]['isrc'];
                    if(empty($it['track_name']) && !empty($map[$id]['name'])) $it['track_name'] = $map[$id]['name'];
                    if(empty($it['album_id']) && !empty($map[$id]['album_id'])) $it['album_id'] = $map[$id]['album_id'];

                    if(!empty($map[$id]['artists'])){
                        foreach($map[$id]['artists'] as $a){
                            if(!empty($a['id'])) $this->upsert_artist_min($a['id'], $a['name'] ?? '');
                        }
                    }
                }
            }
            unset($it);
        }

        // 2) Genres aggregated from artist meta map
        foreach($items as &$it){
            $aids   = array_filter(array_map('trim', explode(',', $it['artist_ids'] ?? '')));
            $genres = [];
            foreach($aids as $aid){
                $g = $artist_meta_map[$aid]['genres'] ?? '';
                if($g!=='') $genres = array_merge($genres, array_map('trim', explode(',', $g)));
            }
            $genres = array_values(array_unique(array_filter($genres)));
            $it['genres'] = implode(', ', $genres);
        }
        unset($it);

        // 3) /v1/albums — fetch labels and apply
        $alb_ids = [];
        foreach($items as $it){ if(!empty($it['album_id'])) $alb_ids[] = $it['album_id']; }
        $labels_map = $this->fetch_albums_labels($alb_ids);
        if(is_wp_error($labels_map)) return $labels_map;

        foreach($items as &$it){
            $aid = $it['album_id'] ?? '';
            $it['label'] = $aid && isset($labels_map[$aid]) ? $labels_map[$aid] : ($it['label'] ?? '');
        }
        unset($it);

        return true;
    }

    private function upsert_artist_min($artist_id,$name=''){
        global $wpdb;
        $name = trim((string)$name);
        if(!$artist_id || $name==='') return; // rule: skip unnamed
        $slug = $this->generate_artist_slug($name, $artist_id);
        $wpdb->query($wpdb->prepare(
            "INSERT INTO {$this->artist_table} (artist_id, artist_name, artist_slug, origin_country, status, created_at, updated_at)
             VALUES (%s,%s,%s,NULL,'draft',%s,%s)
             ON DUPLICATE KEY UPDATE artist_name=VALUES(artist_name), artist_slug=VALUES(artist_slug), updated_at=VALUES(updated_at)",
            $artist_id, $name, $slug, current_time('mysql',1), current_time('mysql',1)
        ));
    }

    private function filter_tracks_by_origin_strict(&$enriched, &$all_by_tid, $origin_filter){
        if(!$origin_filter) return;
        global $wpdb;

        // collect all artist IDs once
        $all_aids = [];
        foreach($enriched as $row){
            foreach(array_filter(array_map('trim', explode(',', $row['artist_ids'] ?? ''))) as $aid){
                $all_aids[$aid] = 1;
            }
        }
        $all_aids = array_keys($all_aids);
        if(!$all_aids){ $enriched = []; $all_by_tid = []; return; }

        // fetch mapping in chunks
        $origin_map = [];
        foreach(array_chunk($all_aids, 1000) as $chunk){
            $placeholders = implode(',', array_fill(0, count($chunk), '%s'));
            $rows = $wpdb->get_results($wpdb->prepare(
                "SELECT artist_id, origin_country FROM {$this->artist_table} WHERE artist_id IN ($placeholders)",
                $chunk
            ), ARRAY_A);
            foreach($rows as $r){ $origin_map[$r['artist_id']] = strtoupper($r['origin_country'] ?? ''); }
        }

        // keep only tracks where every listed artist exists in map and matches the filter
        foreach($enriched as $tid=>$row){
            $aids = array_filter(array_map('trim', explode(',', $row['artist_ids'] ?? '')));
            if(!$aids){ unset($enriched[$tid], $all_by_tid[$tid]); continue; }
            $ok = true;
            foreach($aids as $aid){
                if(!isset($origin_map[$aid]) || $origin_map[$aid] !== $origin_filter){ $ok = false; break; }
            }
            if(!$ok){ unset($enriched[$tid], $all_by_tid[$tid]); }
        }
    }

    /* ===== Release Window Search (album-first, filtered) ===== */
    private function fetch_tracks_release_window($from,$to,$market,$max_total=1000){
        if(!$from || !$to) return new WP_Error('bad_range','Provide from/to dates (YYYY-MM-DD)');
        $yf = intval(substr($from,0,4)); $yt = intval(substr($to,0,4));
        $items = []; $seen_tracks = []; $seen_albums = [];
        $market = $market ?: 'KE';

        for($y=$yf; $y<=$yt; $y++){
            $offset=0; $limit=50; $fetched=0;
            do{
                // album-first search for the year; filter by exact release_date later
                $data = $this->api_request('GET', $this->api_base().'/v1/search', [
                    'q'    => 'year:'.$y,
                    'type' => 'album',
                    'market'=>$market,
                    'limit'=>$limit,
                    'offset'=>$offset
                ]);
                if(is_wp_error($data)) return $data;

                $albums = $data['albums']['items'] ?? [];
                foreach($albums as $alb){
                    $aid = $alb['id'] ?? '';
                    if(!$aid || isset($seen_albums[$aid])) continue;
                    $rdate = $alb['release_date'] ?? '';
                    if(!$rdate || $rdate < $from || $rdate > $to) continue; // restrict to range
                    $seen_albums[$aid]=1;

                    // fetch album tracks (paged)
                    $t_offset=0; $t_limit=50;
                    do{
                        $atracks = $this->api_request('GET', $this->api_base().'/v1/albums/'.rawurlencode($aid).'/tracks', [
                            'market'=>$market,'limit'=>$t_limit,'offset'=>$t_offset
                        ]);
                        if(is_wp_error($atracks)) return $atracks;
                        foreach(($atracks['items'] ?? []) as $t){
                            $tid = $t['id'] ?? ''; if(!$tid || isset($seen_tracks[$tid])) continue;
                            $seen_tracks[$tid]=1;

                            $artists=[]; $artist_ids=[];
                            foreach(($t['artists'] ?? []) as $a){
                                if(!empty($a['name'])) $artists[]=$a['name'];
                                if(!empty($a['id'])) $artist_ids[]=$a['id'];
                            }
                            // album image not present here; reuse from album object
                            $img=''; if(!empty($alb['images']) && is_array($alb['images'])){
                                usort($alb['images'], fn($a,$b)=>intval($b['width']??0)<=>intval($a['width']??0));
                                $img=$alb['images'][0]['url'] ?? '';
                            }
                            $items[]=[
                                'playlist_id'=>null,
                                'rank_in_src'=>0,
                                'track_id'=>$tid,
                                'album_id'=>$aid,
                                'track_name'=>$t['name'] ?? '',
                                'artists'=>implode(', ',$artists),
                                'artist_ids'=>implode(',', array_values(array_unique($artist_ids))),
                                'duration_ms'=>intval($t['duration_ms'] ?? 0),
                                'album_image_url'=>$img,
                                'album_release_date'=>$rdate,
                                'label'=>'',
                                'added_at'=>null,
                                'popularity'=>0,    // filled during enrich
                                'isrc'=>'',         // filled during enrich
                            ];
                        }
                        $t_offset += $t_limit;
                        if (count($atracks['items'] ?? []) < $t_limit) break;
                    } while (true);
                }
                $fetched += count($albums);
                $offset  += $limit;
                if ($fetched >= intval($data['albums']['total'] ?? 0)) break;
                if (count($items) >= $max_total) break; // global cap
            } while(true);
        }
        return $items;
    }

    /* ===== Scoring + Store (per chart) ===== */
    private function nairobi_today_date(){ $tz=new DateTimeZone(self::TZ); return (new DateTime('now',$tz))->format('Y-m-d'); }
    private function parse_weights_map($csv){
        $map=[]; foreach(explode(',', (string)$csv) as $pair){
            $pair=trim($pair); if(!$pair) continue;
            if(strpos($pair,'=')!==false){ [$k,$v]=array_map('trim', explode('=',$pair,2)); $kid=$this->normalize_playlist_id($k); if($kid){ $map[$kid]=floatval($v); } }
        } return $map;
    }

    private function apply_album_date_filter($items_map, $from, $to){
        if (!$from && !$to) return $items_map;
        foreach($items_map as $tid=>$occurs){
            $keep = false;
            foreach($occurs as $r){
                $ad = $r['album_release_date'] ?? '';
                if(!$ad) continue;
                if (($from && $ad < $from) || ($to && $ad > $to)) {
                    // no-op
                } else {
                    $keep = true; break;
                }
            }
            if(!$keep){ unset($items_map[$tid]); }
        }
        return $items_map;
    }

    public function ingest_and_compute_chart($chart_key, $chart_conf, $manual=false, $as_draft=false){
        $opts=$this->get_options();
        if(!$opts['client_id']||!$opts['client_secret']){
            if($manual){ add_action('admin_notices',function(){ echo '<div class="error"><p>'.esc_html__('Configure Client ID/Secret first in Settings.', 'wakilisha-charts').'</p></div>'; }); }
            return;
        }

        $market = strtoupper($chart_conf['market'] ?? ($opts['market'] ?: 'KE'));
        $limitWanted = max(10, min(200, intval($chart_conf['chart_limit'] ?? 100)));
        $source_type = $chart_conf['source_type'] ?? 'playlists';
        $filter_from = $this->safe_date($chart_conf['filter_release_from'] ?? '');
        $filter_to   = $this->safe_date($chart_conf['filter_release_to'] ?? '');

        $all_by_tid=[];
        $weights = $this->parse_weights_map($chart_conf['playlist_weights'] ?? '');
        $weights = array_replace($weights, $this->parse_weights_map($chart_conf['fallback_weights'] ?? ''));
        $origin_filter = $this->valid_iso($chart_conf['origin_filter'] ?? '');
        $fallback_ids = ($source_type === 'playlists') ? $this->normalize_many($chart_conf['fallback_playlists'] ?? '') : [];
        $used_fallback = false;

        if ($source_type === 'playlists'){
            $sources = $this->normalize_many($chart_conf['playlist_multi'] ?? '');
            if (empty($sources)) { $this->mark_last_run(false,'No valid playlists configured for chart '.$chart_key); return; }
            foreach($sources as $pid){
                $res=$this->fetch_playlist_tracks($pid,$market);
                if(is_wp_error($res)){ $this->mark_last_run(false,$res->get_error_message()); return; }
                $res = apply_filters('waki_playlist_items', $res, $pid, $market, $chart_key, $chart_conf);
                $rank=0;
                foreach($res as $row){ $rank++; $row['rank_in_src']=$rank; $all_by_tid[$row['track_id']][]=$row; }
            }
        } else { // release_window
            $from = $this->safe_date($chart_conf['release_from'] ?? '');
            $to   = $this->safe_date($chart_conf['release_to'] ?? '');
            if(!$from || !$to){ $this->mark_last_run(false,'Missing release window dates'); return; }
            $res = $this->fetch_tracks_release_window($from,$to,$market, 3000);
            if(is_wp_error($res)){ $this->mark_last_run(false,$res->get_error_message()); return; }
            $res = apply_filters('waki_release_window_items', $res, $from, $to, $market, $chart_key, $chart_conf);
            foreach($res as $row){ $all_by_tid[$row['track_id']][]=$row; }
        }

        $compute = function($all) use ($filter_from,$filter_to,$origin_filter,$opts,$weights,$limitWanted,$chart_key,$chart_conf){
            $all = $this->apply_album_date_filter($all, $filter_from, $filter_to);
            $flat=[]; foreach($all as $tid=>$arr){ $flat[]=$arr[0]; }
            $enr=$this->enrich_tracks($flat); if(is_wp_error($enr)) return $enr;
            $enriched=[]; foreach($flat as $f){ $enriched[$f['track_id']]=apply_filters('waki_row_after_enrich', $f, $chart_key, $chart_conf); }
            if ($origin_filter){ $this->filter_tracks_by_origin_strict($enriched, $all, $origin_filter); }
            $alpha=floatval($opts['alpha']); $beta=floatval($opts['beta']); $gamma=floatval($opts['gamma']); $presence_bonus=floatval($opts['presence_bonus']);
            $scored=[];
            foreach($all as $tid=>$occurs){
                if(!isset($enriched[$tid])) continue;
                $pop = intval($enriched[$tid]['popularity'] ?? 0);
                $best_rank = 0;
                if(!empty($occurs[0]['playlist_id'])){ $best_rank = min(array_map(fn($r)=>intval($r['rank_in_src'] ?: 9999), $occurs)); }
                $playlists_count = !empty($occurs[0]['playlist_id']) ? count($occurs) : 1;
                $min_days=9999;
                foreach($occurs as $r){
                    $basis = !empty($r['added_at']) ? $r['added_at'] : (!empty($r['album_release_date']) ? $r['album_release_date'].' 00:00:00' : null);
                    if($basis){ $days = max(0, floor((time() - strtotime($basis.' UTC'))/86400)); if($days < $min_days) $min_days = $days; }
                }
                $recency = max(0, 28 - min($min_days, 28));
                $norm_pop  = $pop / 100;
                $norm_rank = $best_rank>0 ? (1 / max(1,$best_rank)) : 0;
                $presence_mult = 1 + ($presence_bonus * max(0,$playlists_count-1));
                $playlist_weight_sum = 1.0;
                if(!empty($occurs[0]['playlist_id'])){
                    $playlist_weight_sum = 0.0;
                    foreach($occurs as $r){ $playlist_weight_sum += ($weights[$r['playlist_id']] ?? 1.0); }
                    if ($playlist_weight_sum <= 0) $playlist_weight_sum = 1.0;
                }
                $components = [
                    'norm_pop'=>$norm_pop,'norm_rank'=>$norm_rank,'recency'=>$recency/28,
                    'alpha'=>$alpha,'beta'=>$beta,'gamma'=>$gamma,
                    'presence_mult'=>$presence_mult,'playlist_weight_sum'=>$playlist_weight_sum
                ];
                $components = apply_filters('waki_score_components',$components,$enriched[$tid],$occurs,$chart_key,$chart_conf);
                $score_component = ($components['alpha']*$components['norm_pop'])
                                 + ($components['beta'] *$components['norm_rank'])
                                 + ($components['gamma']*$components['recency']);
                $score = $score_component * $components['playlist_weight_sum'] * $components['presence_mult'];
                if ($score <= 0) $score = 0.05;
                $base = $enriched[$tid] + [
                    'track_id'=>$tid,
                    'track_name'=>$enriched[$tid]['track_name'] ?? ($occurs[0]['track_name'] ?? ''),
                    'artists'=>$enriched[$tid]['artists'] ?? ($occurs[0]['artists'] ?? ''),
                    'artist_ids'=>$enriched[$tid]['artist_ids'] ?? ($occurs[0]['artist_ids'] ?? ''),
                    'genres'=>$enriched[$tid]['genres'] ?? '',
                    'album_image_url'=>$enriched[$tid]['album_image_url'] ?? ($occurs[0]['album_image_url'] ?? ''),
                    'album_release_date'=>$enriched[$tid]['album_release_date'] ?? ($occurs[0]['album_release_date'] ?? ''),
                    'label'=>$enriched[$tid]['label'] ?? ($occurs[0]['label'] ?? ''),
                    'added_at'=> $occurs[0]['added_at'] ?? null,
                    'in_playlists'=>$playlists_count,
                    'isrc'=>$enriched[$tid]['isrc'] ?? '',
                ];
                $base['score']=$score;
                $scored[]=$base;
            }
            usort($scored,function($a,$b){
                if($a['score']==$b['score']){
                    if(($a['popularity'] ?? 0) == ($b['popularity'] ?? 0)){
                        return strcmp($b['album_release_date'] ?? '1970-01-01', $a['album_release_date'] ?? '1970-01-01');
                    }
                    return (($a['popularity'] ?? 0) < ($b['popularity'] ?? 0))?1:-1;
                }
                return ($a['score']<$b['score'])?1:-1;
            });
            $scored = array_slice($scored,0,$limitWanted);
            $chart_date = $this->safe_date($chart_conf['chart_date'] ?? '') ?: $this->nairobi_today_date();
            $prev_date=$this->get_previous_chart_date($chart_key,$chart_date);
            $prev_positions=$prev_date ? $this->map_positions_for_date($chart_key,$prev_date) : [];
            $prev_we=$prev_date ? $this->map_weeks_for_date($chart_key,$prev_date) : [];
            $prev_scores=$prev_date ? $this->map_scores_for_date($chart_key,$prev_date) : [];
            $prev_pops=$prev_date ? $this->map_pop_for_date($chart_key,$prev_date) : [];
            $pos=0;
            foreach($scored as &$it){
                $pos++;
                $tid=$it['track_id'];
                $prev_pos = $prev_positions[$tid] ?? null;
                $it['position']=$pos;
                $it['position_change']=($prev_pos!==null)?($prev_pos-$pos):0;
                $it['weeks_on_chart']=($prev_pos!==null)?intval($prev_we[$tid] ?? 0)+1:1;
                $deb = $this->db_get_debut_info($chart_key,$tid);
                if ($deb){ $it['debut_position']=intval($deb['position']); $it['debut_date']=$deb['chart_date']; }
                else { $it['debut_position']=$pos; $it['debut_date']=$chart_date; }
                $pk = $this->db_get_peak_info($chart_key,$tid,$chart_date);
                if ($pk){
                    $bestPrev = intval($pk['position']);
                    if ($pos < $bestPrev){ $it['peak_position']=$pos; $it['peak_date']=$chart_date; }
                    else { $it['peak_position']=$bestPrev; $it['peak_date']=$pk['chart_date']; }
                } else { $it['peak_position']=$pos; $it['peak_date']=$chart_date; }
                $prev_sc  = $prev_scores[$tid] ?? 0;
                $prev_pop = $prev_pops[$tid] ?? 0;
                $it['score_change']=round(($it['score'] - $prev_sc),4);
                $it['popularity_change']=intval(($it['popularity'] ?? 0) - $prev_pop);
            } unset($it);
            return [$scored,$chart_date];
        };

        $add_fallback = function() use (&$all_by_tid,$fallback_ids,$market,$chart_key,$chart_conf){
            foreach($fallback_ids as $pid){
                $res=$this->fetch_playlist_tracks($pid,$market);
                if(is_wp_error($res)) continue;
                $res = apply_filters('waki_playlist_items', $res, $pid, $market, $chart_key, $chart_conf);
                $rank=0;
                foreach($res as $row){ $rank++; $row['rank_in_src']=$rank; $all_by_tid[$row['track_id']][]=$row; }
            }
        };

        $result = $compute($all_by_tid);
        if(is_wp_error($result)){ $this->mark_last_run(false,$result->get_error_message()); return; }
        [$scored,$chart_date] = $result;

        if(count($scored) < $limitWanted && !empty($fallback_ids)){
            $add_fallback();
            $used_fallback = true;
            $result = $compute($all_by_tid);
            if(is_wp_error($result)){ $this->mark_last_run(false,$result->get_error_message()); return; }
            [$scored,$chart_date] = $result;
        }

        $positions = wp_list_pluck($scored,'position');
        $expected_positions = range(1,count($scored));
        if ($positions !== $expected_positions) {
            $pos = 0;
            foreach ($scored as &$row){ $row['position'] = ++$pos; }
            unset($row);
            $positions = wp_list_pluck($scored,'position');
            if ($positions !== $expected_positions) {
                $err = new WP_Error('chart_position_conflict','Chart positions conflict detected. Please review playlist configuration.');
                $this->mark_last_run(false,$err->get_error_message(), '', $chart_date, $chart_key);
                return $err;
            }
        }

        $snapshot_id = $chart_key.'-'.(function_exists('wp_generate_uuid4') ? wp_generate_uuid4() : uniqid('', true));
        $ok = $this->store_chart_rows($chart_key,$chart_date,$snapshot_id,$scored);
        if($ok === false && !$used_fallback && !empty($fallback_ids)){
            $add_fallback();
            $result = $compute($all_by_tid);
            if(is_wp_error($result)){ $this->mark_last_run(false,$result->get_error_message()); return; }
            [$scored,$chart_date] = $result;
            $positions = wp_list_pluck($scored,'position');
            $expected_positions = range(1,count($scored));
            if ($positions !== $expected_positions) {
                $pos = 0;
                foreach ($scored as &$row){ $row['position'] = ++$pos; }
                unset($row);
                $positions = wp_list_pluck($scored,'position');
                if ($positions !== $expected_positions) {
                    $err = new WP_Error('chart_position_conflict','Chart positions conflict detected. Please review playlist configuration.');
                    $this->mark_last_run(false,$err->get_error_message(), '', $chart_date, $chart_key);
                    return $err;
                }
            }
            $snapshot_id = $chart_key.'-'.(function_exists('wp_generate_uuid4') ? wp_generate_uuid4() : uniqid('', true));
            $ok = $this->store_chart_rows($chart_key,$chart_date,$snapshot_id,$scored);
        }
        if($ok === false){
            global $wpdb;
            $err = $wpdb->last_error ?: 'No chart rows to insert';
            $this->mark_last_run(false,$err,$snapshot_id,$chart_date,$chart_key);
            return;
        }
        $this->mark_last_run(true,'',$snapshot_id,$chart_date,$chart_key);

        $auto_publish = ($chart_conf['auto_make_post'] ?? '1')==='1';
        $do_publish   = $manual ? !$as_draft : $auto_publish;
        $this->ensure_chart_post($chart_key,$chart_conf,$chart_date,$scored,$do_publish,$snapshot_id);
    }

    private function mark_last_run($ok,$err='',$snapshot_id='',$chart_date='',$chart_key=''){
        $opts=$this->get_options();
        $opts['last_run']=current_time('mysql');
        if($snapshot_id) $opts['last_snapshot']=$snapshot_id;
        if($chart_date)  $opts['last_chart_date']=$chart_date;
        update_option(self::OPTS,$opts);

        if ($chart_key){
            $charts = $this->get_charts();
            if(isset($charts[$chart_key])){
                $charts[$chart_key]['last_run']=current_time('mysql');
                $charts[$chart_key]['last_chart_date']=$chart_date;
                $charts[$chart_key]['last_snapshot']=$snapshot_id;
                $this->put_charts($charts);
            }
        }
        if(!$ok && $err){ $clean=stripslashes($err); update_option(self::SLUG.'_last_error',$clean); error_log('[WAKI Charts] Ingest failed: '.$clean); }
        else delete_option(self::SLUG.'_last_error');
    }

    private function store_chart_rows($chart_key,$chart_date,$snapshot_id,$items){
        global $wpdb;
        if (empty($items)) {
            $wpdb->last_error = 'No chart rows to insert';
            return false;
        }
        $limitWanted = count($items);

        $wpdb->query('START TRANSACTION');

        $wpdb->query($wpdb->prepare(
            "DELETE FROM {$this->table} WHERE chart_key=%s AND chart_date=%s AND snapshot_id=%s",
            $chart_key,$chart_date,$snapshot_id
        ));

        foreach($items as $it){
            $data = [
                'chart_key'=>$chart_key,'chart_date'=>$chart_date,'snapshot_id'=>$snapshot_id,
                'track_id'=>$it['track_id'],'isrc'=>$it['isrc'] ?? '',
                'track_name'=>$it['track_name'],'artists'=>$it['artists'],
                'artist_ids'=>$it['artist_ids'] ?? '',
                'genres'=>$it['genres'] ?? '',
                'popularity'=>intval($it['popularity'] ?? 0),'duration_ms'=>intval($it['duration_ms'] ?? 0),
                'album_image_url'=>$it['album_image_url'],'album_release_date'=>$it['album_release_date'],
                'label'=>$it['label'] ?? '',
                'added_at'=>$it['added_at'],
                'score'=>floatval($it['score']),'position'=>intval($it['position']),
                'position_change'=>intval($it['position_change']),'weeks_on_chart'=>intval($it['weeks_on_chart']),
                'peak_position'=>intval($it['peak_position']),'peak_date'=>!empty($it['peak_date'])?$it['peak_date']:null,
                'debut_position'=>intval($it['debut_position'] ?? 0),'debut_date'=>!empty($it['debut_date'])?$it['debut_date']:null,
                'score_change'=>floatval($it['score_change']),'popularity_change'=>intval($it['popularity_change']),
                'in_playlists'=>intval($it['in_playlists'] ?? 1),
                'created_at'=>current_time('mysql',1),'updated_at'=>current_time('mysql',1),
            ];
            $res = $wpdb->insert($this->table,$data);
            if($res === false){
                $wpdb->query('ROLLBACK');
                error_log($wpdb->last_error);
                return false;
            }
        }

        $positions = array_map('intval', $wpdb->get_col($wpdb->prepare(
            "SELECT position FROM {$this->table}
             WHERE chart_key=%s AND chart_date=%s AND snapshot_id=%s
             ORDER BY position ASC",
            $chart_key,$chart_date,$snapshot_id
        )));
        $expected = range(1, $limitWanted);
        $missing = array_values(array_diff($expected, $positions));
        $counts = array_count_values($positions);
        $duplicates = array_values(array_keys(array_filter($counts, fn($c)=>$c>1)));
        if($positions !== $expected){
            $msg = "Unexpected positions for {$chart_key} {$chart_date} [{$snapshot_id}] Missing: {" . implode(',', $missing) . "} Duplicates: {" . implode(',', $duplicates) . "}";
            error_log($msg);
            add_action('admin_notices', function() use ($msg, $chart_key, $chart_date) {
                $url = admin_url('admin.php?page=' . self::SLUG . '&edit_chart=' . $chart_key);
                echo '<div class="notice notice-error"><p>' . esc_html($msg) . ' <a href="' . esc_url($url) . '">Edit chart source playlists</a></p></div>';
            });
            $wpdb->query('ROLLBACK');
            $wpdb->last_error = $msg;
            return false;
        }

        $wpdb->query('COMMIT');
        return true;
    }

    /* ===== Data accessors ===== */
    private function get_latest_chart_date($chart_key='default'){
        $q = new \WP_Query([
            'post_type'      => self::CPT,
            'post_status'    => 'publish',
            'posts_per_page' => 1,
            'meta_key'       => '_waki_chart_date',
            'orderby'        => 'meta_value',
            'order'          => 'DESC',
            'fields'         => 'ids',
            'meta_query'     => [[ 'key'=>'_waki_chart_key','value'=>$chart_key ]],
        ]);
        return $q->have_posts() ? get_post_meta($q->posts[0],'_waki_chart_date',true) : null;
    }
    private function get_previous_chart_date($chart_key,$current_date){
        $q = new \WP_Query([
            'post_type'      => self::CPT,
            'post_status'    => 'publish',
            'posts_per_page' => 1,
            'meta_key'       => '_waki_chart_date',
            'orderby'        => 'meta_value',
            'order'          => 'DESC',
            'fields'         => 'ids',
            'meta_query'     => [
                ['key'=>'_waki_chart_key','value'=>$chart_key],
                ['key'=>'_waki_chart_date','value'=>$current_date,'compare'=>'<','type'=>'DATE'],
            ],
        ]);
        return $q->have_posts() ? get_post_meta($q->posts[0],'_waki_chart_date',true) : null;
    }
    private function get_chart_rows($chart_key,$date,$limit=0,$snapshot_id=''){
        global $wpdb;
        $sql = "SELECT * FROM {$this->table} WHERE chart_key=%s AND chart_date=%s";
        $args = [$chart_key,$date];
        if ($snapshot_id!==''){
            $sql .= " AND snapshot_id=%s";
            $args[] = $snapshot_id;
        } else {
            $snapshot_id = $this->get_latest_snapshot_id($chart_key,$date);
            if ($snapshot_id){
                $sql .= " AND snapshot_id=%s";
                $args[] = $snapshot_id;
            }
        }
        $sql .= " ORDER BY position ASC";
        if ($limit>0) $sql .= $wpdb->prepare(" LIMIT %d", $limit);
        return $wpdb->get_results($wpdb->prepare($sql,$args), ARRAY_A) ?: [];
    }

    private function get_latest_snapshot_id($chart_key,$date){
        global $wpdb;
        return $wpdb->get_var($wpdb->prepare(
            "SELECT snapshot_id FROM {$this->table} WHERE chart_key=%s AND chart_date=%s GROUP BY snapshot_id ORDER BY MAX(created_at) DESC LIMIT 1",
            $chart_key,$date
        ));
    }
    private function map_positions_for_date($chart_key,$date){ global $wpdb; $rows=$wpdb->get_results($wpdb->prepare("SELECT track_id,position FROM {$this->table} WHERE chart_key=%s AND chart_date=%s",$chart_key,$date),ARRAY_A); $m=[]; foreach($rows as $r){ $m[$r['track_id']]=intval($r['position']); } return $m; }
    private function map_weeks_for_date($chart_key,$date){ global $wpdb; $rows=$wpdb->get_results($wpdb->prepare("SELECT track_id,weeks_on_chart FROM {$this->table} WHERE chart_key=%s AND chart_date=%s",$chart_key,$date),ARRAY_A); $m=[]; foreach($rows as $r){ $m[$r['track_id']]=intval($r['weeks_on_chart']); } return $m; }
    private function map_scores_for_date($chart_key,$date){ global $wpdb; $rows=$wpdb->get_results($wpdb->prepare("SELECT track_id,score FROM {$this->table} WHERE chart_key=%s AND chart_date=%s",$chart_key,$date),ARRAY_A); $m=[]; foreach($rows as $r){ $m[$r['track_id']]=floatval($r['score']); } return $m; }
    private function map_pop_for_date($chart_key,$date){ global $wpdb; $rows=$wpdb->get_results($wpdb->prepare("SELECT track_id,popularity FROM {$this->table} WHERE chart_key=%s AND chart_date=%s",$chart_key,$date),ARRAY_A); $m=[]; foreach($rows as $r){ $m[$r['track_id']]=intval($r['popularity']); } return $m; }
    private function db_get_debut_info($chart_key,$track_id){
        global $wpdb;
        return $wpdb->get_row($wpdb->prepare(
            "SELECT chart_date, position FROM {$this->table} WHERE chart_key=%s AND track_id=%s ORDER BY chart_date ASC, position ASC LIMIT 1",
            $chart_key,$track_id
        ), ARRAY_A);
    }
    private function db_get_peak_info($chart_key,$track_id,$as_of_date){
        global $wpdb;
        $best_pos = $wpdb->get_var($wpdb->prepare(
            "SELECT MIN(position) FROM {$this->table} WHERE chart_key=%s AND track_id=%s AND chart_date <= %s",
            $chart_key,$track_id,$as_of_date
        ));
        if ($best_pos === null) return null;
        return $wpdb->get_row($wpdb->prepare(
            "SELECT position, chart_date FROM {$this->table}
             WHERE chart_key=%s AND track_id=%s AND chart_date <= %s AND position=%d
             ORDER BY chart_date DESC LIMIT 1",
            $chart_key,$track_id,$as_of_date,intval($best_pos)
        ), ARRAY_A);
    }
    private function get_recent_dates($chart_key,$as_of_date,$limit){
        $q = new \WP_Query([
            'post_type'      => self::CPT,
            'post_status'    => 'publish',
            'posts_per_page' => intval($limit),
            'meta_key'       => '_waki_chart_date',
            'orderby'        => 'meta_value',
            'order'          => 'DESC',
            'fields'         => 'ids',
            'meta_query'     => [
                ['key'=>'_waki_chart_key','value'=>$chart_key],
                ['key'=>'_waki_chart_date','value'=>$as_of_date,'compare'=>'<=','type'=>'DATE'],
            ],
        ]);
        $dates=[]; foreach($q->posts as $pid){ $dates[] = get_post_meta($pid,'_waki_chart_date',true); } return $dates;
    }

    /* ===== Build CPT post for each chart issue ===== */
    private function ensure_chart_post($chart_key,$chart_conf,$chart_date,$rows,$publish,$snapshot_id){
        $title = ($chart_conf['title'] ?: 'WAKILISHA Chart');
        $post_name = sanitize_title($chart_key.'-'.$chart_date);

        $existing = get_posts([
            'name'=>$post_name,
            'post_type'=>self::CPT,
            'post_status'=>'any',
            'numberposts'=>1
        ]);
        $status = $publish ? 'publish' : 'draft';

        $content = '[waki_chart chart_key="'.esc_attr($chart_key).'" chart_date="'.$chart_date.'" snapshot_id="'.esc_attr($snapshot_id).'" limit="'.intval($chart_conf['chart_limit'] ?? 100).'" fullwidth="1" history="3" show_title="0" title="'.esc_attr(($chart_conf['title'] ?: 'WAKILISHA Chart')).'"]';

        if ($existing){
            $post_id = $existing[0]->ID;
            wp_update_post(['ID'=>$post_id,'post_title'=>$title,'post_status'=>$status,'post_content'=>$content,'post_type'=>self::CPT]);
        } else {
            $post_id = wp_insert_post([
                'post_title'=>$title,
                'post_name'=>$post_name,
                'post_type'=>self::CPT,
                'post_status'=>$status,
                'post_content'=>$content,
                'post_excerpt'=> 'Weekly chart for '.$chart_conf['market'],
            ]);
        }

        if (!is_wp_error($post_id)){
            update_post_meta($post_id,'_waki_chart_key',$chart_key);
            update_post_meta($post_id,'_waki_chart_date',$chart_date);
            update_post_meta($post_id,'_waki_snapshot_id',$snapshot_id);
            update_post_meta($post_id,'_waki_chart_title',($chart_conf['title'] ?: 'WAKILISHA Chart'));
            $meta = $this->parse_chart_meta($chart_conf);
            $payload = [
                'chart_key' => $meta['chart_key'] ?? $chart_key,
                'countries' => array_values($meta['countries'] ?? []),
                'genres' => array_values($meta['genres'] ?? []),
                'languages' => array_values($meta['languages'] ?? []),
                'format' => $meta['format'] ? [$meta['format']] : [],
            ];
            update_post_meta($post_id,'waki_chart_payload', wp_json_encode($payload));
            if(!empty($rows) && !empty($rows[0]['album_image_url'])){
                $cover_url = $rows[0]['album_image_url'];
                update_post_meta($post_id,'_waki_cover_url',$cover_url);
                $this->sideload_featured_image($cover_url,$post_id);
            } else {
                delete_post_meta($post_id,'_waki_cover_url');
            }
        }
    }
    private function sideload_featured_image($url,$post_id){
        if(!$url) return;
        require_once ABSPATH.'wp-admin/includes/file.php';
        require_once ABSPATH.'wp-admin/includes/media.php';
        require_once ABSPATH.'wp-admin/includes/image.php';
        $tmp = download_url($url);
        if (is_wp_error($tmp)) return;
        $file = ['name'=>basename(parse_url($url,PHP_URL_PATH)),'type'=>'image/jpeg','tmp_name'=>$tmp,'size'=>@filesize($tmp)?:0,'error'=>0];
        $id = media_handle_sideload($file, $post_id);
        if (!is_wp_error($id)) set_post_thumbnail($post_id, $id);
    }

    public function remove_chart_rows($post_id){
        if(get_post_type($post_id)!==self::CPT) return;
        $key  = get_post_meta($post_id,'_waki_chart_key',true);
        $date = get_post_meta($post_id,'_waki_chart_date',true);
        $sid  = get_post_meta($post_id,'_waki_snapshot_id',true);
        if(!$key || !$date || !$sid) return;
        global $wpdb;
        $wpdb->delete($this->table,['chart_key'=>$key,'chart_date'=>$date,'snapshot_id'=>$sid],['%s','%s','%s']);
    }

    public function output_social_meta(){
        if(!is_singular(self::CPT)) return;
        global $post; if(!$post) return;
        $img = get_the_post_thumbnail_url($post,'full');
        $key  = get_post_meta($post->ID,'_waki_chart_key',true);
        $date = get_post_meta($post->ID,'_waki_chart_date',true);
        $sid  = get_post_meta($post->ID,'_waki_snapshot_id',true);
        $rows = $this->get_chart_rows($key,$date,3,$sid);
        $top = [];
        foreach($rows as $r){
            $names = array_map('trim', explode(',', $r['artists']));
            foreach($names as $n){ if($n && !in_array($n,$top,true)) $top[]=$n; }
        }
        $intro = $top?('Featuring '.implode(', ',$top).' and more'):'';
        $title = get_the_title($post);
        $url   = get_permalink($post);
        echo '\n<meta property="og:title" content="'.esc_attr($title).'" />';
        if($intro) echo '\n<meta property="og:description" content="'.esc_attr($intro).'" />';
        if($url)   echo '\n<meta property="og:url" content="'.esc_url($url).'" />';
        if($img)   echo '\n<meta property="og:image" content="'.esc_url($img).'" />';
        echo '\n<meta name="twitter:card" content="summary_large_image" />';
        echo '\n<meta name="twitter:title" content="'.esc_attr($title).'" />';
        if($intro) echo '\n<meta name="twitter:description" content="'.esc_attr($intro).'" />';
        if($img)   echo '\n<meta name="twitter:image" content="'.esc_url($img).'" />';
        echo "\n";
    }

    public function pretty_shortlink($shortlink,$id){
        $p = get_post($id);
        if($p && $p->post_type===self::CPT) return get_permalink($id);
        return $shortlink;
    }

    /* ===== Shortcodes / Frontend ===== */
    public function register_shortcodes(){
        add_shortcode('waki_artist', [$this, 'render_artist_profile']);
    }

    public function render_artist_profile($atts){
        $atts = shortcode_atts([
            'id' => '',
        ], $atts, 'waki_artist');

        $artist_id = sanitize_text_field($atts['id']);
        if (!$artist_id) return '';

        global $wpdb;
        $artist = $wpdb->get_row(
            $wpdb->prepare(
                "SELECT artist_id, artist_name, image_url, genres, followers, biography, latest_release, top_tracks, discography, chart_stats, video_urls, related_artist_ids FROM {$this->artist_table} WHERE artist_id=%s AND status='publish'",
                $artist_id
            ),
            ARRAY_A
        );
        if (!$artist) return '<p>Artist not found.</p>';

        ob_start();
        include WAKI_CHARTS_DIR . 'templates/artist-profile.php';
        return ltrim(ob_get_clean());
    }

    public function force_single_content($content){
        if (is_singular(self::CPT)){
            $post_id = get_the_ID();
            $key  = get_post_meta($post_id,'_waki_chart_key',true);
            $date = get_post_meta($post_id,'_waki_chart_date',true) ?: $this->get_latest_chart_date($key ?: 'default');
            $sid  = get_post_meta($post_id,'_waki_snapshot_id',true);
            if($key && $date){
                $sc = '[waki_chart chart_key="'.esc_attr($key).'" chart_date="'.esc_attr($date).'" snapshot_id="'.esc_attr($sid).'" fullwidth="1" show_title="1" history="3" title="'.esc_attr(get_the_title($post_id)).'"]';
                return $sc;
            }
        }
        return $content;
    }

    private function is_new_by_release($album_release_date, $chart_date){
        if(!$album_release_date || !$chart_date) return false;
        // treat as NEW if album release is within the last 7 days up to chart_date
        $cd = strtotime($chart_date.' 00:00:00 UTC');
        $rd = strtotime($album_release_date.' 00:00:00 UTC');
        if($rd === false || $cd === false) return false;
        if($rd > $cd) return false; // future dates not allowed
        $days = floor(($cd - $rd) / 86400);
        return ($days >= 0 && $days <= 7);
    }

    /* ===== Shortcode: Single Chart with per-track history + HERO ===== */
    public function shortcode_latest_chart($atts){
        $atts = shortcode_atts([
            'chart_key'=>'default',
            'limit'=>'100',
            'title'=>'WAKILISHA Chart (Latest)',
            'fullwidth'=>'1',
            'chart_date'=>'',
            'show_title'=>'0',
            'history'=>'3',
            'snapshot_id'=>''
        ], $atts, 'waki_chart');

        $chart_key = $this->normalize_slug($atts['chart_key']);
        $limit = max(10, min(200, intval($atts['limit'])));
        $date  = $atts['chart_date'] ?: $this->get_latest_chart_date($chart_key);
        if (!$date) return '<p>No chart data yet.</p>';

        // Build dates list: selected + up to N prev
        $history = max(0, min(6, intval($atts['history'])));
        $dates = $this->get_recent_dates($chart_key,$date, $history+1); // includes self
        if (!$dates) $dates = [$date];

        // Primary list (first date)
        $first_date = $dates[0];
        $sid = $atts['snapshot_id'];
        $first_rows = $this->get_chart_rows($chart_key,$first_date, $limit,$sid);
        $hero = $first_rows[0] ?? null;
        $top_artists = [];
        foreach(array_slice($first_rows,0,3) as $r){
            $names = array_map('trim', explode(',', $r['artists']));
            foreach($names as $n){
                if($n !== '' && !in_array($n, $top_artists)){
                    $top_artists[] = $n;
                }
            }
        }
        $intro_artists = implode(', ', $top_artists);

        $opts = $this->get_options();
        $size = $opts['hero_img_size'] ?? 'full';
        $bg = '';
        $post = get_post();
        if($post && has_post_thumbnail($post)){
            $bg = get_the_post_thumbnail_url($post, $size);
        } elseif($hero){
            $bg = $hero['album_image_url'];
        }

        $charts  = $this->get_charts();
        $country = '';
        if($chart_key && !empty($charts[$chart_key]['origin_filter'])){
            $iso = strtoupper($charts[$chart_key]['origin_filter']);
            $map = $this->iso_list();
            $country = $map[$iso] ?? $iso;
        }
        $updated = $post ? get_post_modified_time(get_option('date_format'), false, $post) : '';

        ob_start();
        include WAKI_CHARTS_DIR . 'templates/latest-chart.php';
        return ltrim(ob_get_clean());
    }

    /* ===== Shortcode: Archive ===== */
    public function shortcode_charts_archive($atts){
        $atts = shortcode_atts(['per_page'=>12], $atts, 'waki_charts_archive');

        $route_key  = sanitize_text_field(get_query_var('waki_chart_key'));
        $route_date = sanitize_text_field(get_query_var('waki_chart_date'));
        if ($route_key || $route_date){
            $key  = $this->normalize_slug($route_key ?: 'default');
            $date = $route_date ?: $this->get_latest_chart_date($key);
            return do_shortcode('[waki_chart chart_key="'.esc_attr($key).'" chart_date="'.esc_attr($date).'" fullwidth="1" show_title="1" history="3" title="WAKILISHA — '.esc_attr($key).'"]');
        }

        $paged = max(1, get_query_var('paged') ?: 1);
        $ppp = max(9, min(15, intval($atts['per_page'])));

        $opts = $this->get_options();
        $hero_img = '';
        if(!empty($opts['archive_hero_img'])){
            $id = attachment_url_to_postid($opts['archive_hero_img']);
            $size = $opts['hero_img_size'] ?? 'full';
            $hero_img = $id ? wp_get_attachment_image_url($id, $size) : $opts['archive_hero_img'];
            $hero_img = esc_url($hero_img);
        }

        $q = new WP_Query([
            'post_type'     => self::CPT,
            'posts_per_page'=> $ppp,
            'paged'         => $paged,
            'post_status'   => 'publish',
        ]);

        ob_start();
        include WAKI_CHARTS_DIR . 'templates/charts-archive.php';
        return ltrim(ob_get_clean());
    }

    /* ===== Entry rendering with NEW tag rule + interactive history ===== */
    private function format_duration($ms){ $ms=intval($ms); $s=floor($ms/1000); $m=floor($s/60); $s=$s%60; return sprintf('%d:%02d',$m,$s); }

    private function get_track_history_points($chart_key,$track_id,$as_of_date,$limit=52){
        // dates returned newest-first; convert to chronological for plotting
        $dates = $this->get_recent_dates($chart_key,$as_of_date,$limit);
        $dates = array_reverse($dates ?: []);
        $out = [];
        foreach($dates as $d){
            $pos = $this->map_positions_for_date($chart_key,$d)[$track_id] ?? null;
            $out[] = ['date'=>$d,'position'=>$pos ?: null];
        }
        return $out; // chronological left→right
    }

    private function render_history_mini($chart_key,$track_id,$as_of_date,$weeks=52,$pts=null){
        $pts = $pts ?: $this->get_track_history_points($chart_key,$track_id,$as_of_date,$weeks);
        $maxRank = 100;
        $pad=8;
        $step = 36; // px per point (enables horizontal scroll)
        $w = max( (count($pts)-1)*$step + 2*$pad, 320 );
        $h = 110;

        // Build polyline path and nodes
        $path = '';
        $nodes = [];
        foreach($pts as $i=>$p){
            $x = $pad + $i*$step;
            $pos = $p['position'] ?: $maxRank;
            $y = $pad + ($h-2*$pad) * (($pos-1)/($maxRank-1));
            $path .= ($i?' L ':'M ').round($x,1).' '.round($y,1);
            $nodes[] = ['x'=>round($x,1),'y'=>round($y,1),'pos'=>($p['position'] ?: '—'),'date'=>$p['date']];
        }

        ob_start();
        include WAKI_CHARTS_DIR . 'templates/history-mini.php';
        return ltrim(ob_get_clean());
    }

    private function render_entry_row($r, $chart_key, $chart_date){
        $pos = intval($r['position']);
        $pc  = intval($r['position_change']);

        // NEW-tag rule: album release within 7 days up to the chart date
        $is_new = $this->is_new_by_release($r['album_release_date'] ?? '', $chart_date);

        // movement label
        $mv  = '—';
        if ($is_new) $mv = '<span class="mv mv-new">NEW</span>';
        elseif ($pc > 0)  $mv = '<span class="mv mv-up">▲ '.$pc.'</span>';
        elseif ($pc < 0)  $mv = '<span class="mv mv-down">▼ '.abs($pc).'</span>';

        $thumb = esc_url($r['album_image_url']); $has_thumb = !empty($thumb);
        $title = $r['track_name']; $artist = $r['artists'];
        $duration = $this->format_duration($r['duration_ms']);
        $weeks = intval($r['weeks_on_chart']);
        $peak = intval($r['peak_position']);
        $peak_date = !empty($r['peak_date']) ? esc_html($r['peak_date']) : esc_html($r['chart_date'] ?? '—');
        $debut_pos = intval($r['debut_position'] ?? 0);
        $debut_date = !empty($r['debut_date']) ? esc_html($r['debut_date']) : esc_html($r['chart_date'] ?? '—');
        $album_date = $r['album_release_date'] ? esc_html($r['album_release_date']) : '—';
        $label = isset($r['label']) && $r['label'] !== '' ? esc_html($r['label']) : '—';

        $hist_pts = $this->get_track_history_points($chart_key,$r['track_id'],$chart_date,52);
        $has_history = count(array_filter($hist_pts, fn($p)=>$p['position']!==null)) > 1;

        ob_start(); ?>
        <details class="waki-entry entry-redesign <?php echo $has_thumb ? 'has-thumb' : ''; ?>" data-entry>
          <summary class="waki-entry-head" role="button" aria-label="Toggle entry">
            <div class="waki-entry-pos">
              <div class="num"><?php echo $pos; ?></div>
              <div class="move"><?php echo $mv; ?></div>
            </div>
            <div class="waki-vbar" aria-hidden="true"></div>
            <?php if($has_thumb): ?>
              <div class="waki-entry-thumb"><img src="<?php echo $thumb; ?>" alt="" loading="lazy"></div>
            <?php endif; ?>
            <div class="waki-entry-main" title="<?php echo esc_attr($title.' — '.$artist); ?>">
              <div class="ttl"><?php echo esc_html($title); ?></div>
              <div class="art"><?php echo esc_html($artist); ?></div>
            </div>
            <div class="waki-entry-toggle" aria-hidden="true"></div>
          </summary>

          <div class="waki-entry-body">
            <div class="waki-metrics-grid">
              <div class="tm">
                <div class="tm-label">Chart Debut</div>
                <div class="tm-val"><?php echo $debut_pos ?: '—'; ?></div>
                <div class="tm-sub">on <?php echo $debut_date ?: '—'; ?></div>
              </div>
              <div class="tm">
                <div class="tm-label">Peak Position</div>
                <div class="tm-val"><?php echo $peak ?: '—'; ?></div>
                <div class="tm-sub">on <?php echo $peak_date ?: '—'; ?></div>
              </div>
              <div class="tm">
                <div class="tm-label">Weeks on Chart</div>
                <div class="tm-val"><?php echo $weeks ?: '—'; ?></div>
                <div class="tm-sub">&nbsp;</div>
              </div>
              <div class="tm">
                <div class="tm-label">Length</div>
                <div class="tm-val"><?php echo esc_html($duration); ?></div>
                <div class="tm-sub">&nbsp;</div>
              </div>
              <div class="tm">
                <div class="tm-label">Release Date</div>
                <div class="tm-val small"><?php echo $album_date; ?></div>
                <div class="tm-sub">&nbsp;</div>
              </div>
              <div class="tm">
                <div class="tm-label">Label</div>
                <div class="tm-val small"><?php echo $label; ?></div>
                <div class="tm-sub">&nbsp;</div>
              </div>
            </div>

            <?php if($has_history): ?>
              <button type="button" class="waki-mini-btn" data-show-history>Position history <span class="chev" aria-hidden="true">▾</span></button>
              <div class="waki-mini-wrap" style="display:none">
                <?php echo $this->render_history_mini($chart_key, $r['track_id'], $chart_date, 52, $hist_pts); // up to one year ?>
              </div>
            <?php endif; ?>
          </div>
        </details>
        <?php
        return ltrim(ob_get_clean());
    }

    /* ===== Dry Run core ===== */
    private function dry_run_chart($chart_key,$chart_conf){
        $state_log = [];
        $state_log[] = 'Idle';
        $state_log[] = 'Validating config';
        $meta = $this->parse_chart_meta($chart_conf);

        $pre = $this->validate_chart_config([
            'slug'=>$chart_conf['slug'],'title'=>$chart_conf['title'],'market'=>$chart_conf['market'],
            'src'=>$chart_conf['source_type'],
            'pl'=>$chart_conf['playlist_multi'] ?? '',
            'from'=>$chart_conf['release_from'] ?? '',
            'to'=>$chart_conf['release_to'] ?? '',
            'f_rel_from'=>$chart_conf['filter_release_from'] ?? '',
            'f_rel_to'=>$chart_conf['filter_release_to'] ?? '',
            'chart_date'=>$chart_conf['chart_date'] ?? ''
        ]);
        if(!$pre['ok']){
            $state_log[] = 'Validation failed';
            return [
                'state_log'=>$state_log,
                'playlist_checks'=>[],
                'rules'=>[],
                'counts'=>['error'=>1],
                'top_preview'=>[],
                'dedupe_sample'=>[],
                'parsed_meta'=>$meta
            ];
        }

        $state_log[] = 'Inspecting sources';
        $market = strtoupper($chart_conf['market']);
        $weights = $this->parse_weights_map($chart_conf['playlist_weights'] ?? '');
        $weights = array_replace($weights, $this->parse_weights_map($chart_conf['fallback_weights'] ?? ''));
        $weights_err = $this->validate_weights_syntax($chart_conf['playlist_weights'] ?? '', $this->normalize_many($chart_conf['playlist_multi'] ?? ''));
        $fb_weights_err = $this->validate_weights_syntax($chart_conf['fallback_weights'] ?? '', $this->normalize_many($chart_conf['fallback_playlists'] ?? ''));

        $playlist_checks = [];
        $all_items = [];
        $all_by_tid = [];

        if(($chart_conf['source_type'] ?? 'playlists')==='playlists'){
            $pls = $this->normalize_many($chart_conf['playlist_multi'] ?? '');
            foreach($pls as $pid){
                $playlist_checks[] = $this->validate_playlist_meta($pid);
                $res = $this->fetch_playlist_tracks($pid,$market);
                if(!is_wp_error($res)){
                    $res = apply_filters('waki_playlist_items', $res, $pid, $market, $chart_key, $chart_conf);
                    $rank=0;
                    foreach($res as $row){ $rank++; $row['rank_in_src']=$rank; $all_items[]=$row; $all_by_tid[$row['track_id']][]=$row; }
                }
            }
        } else {
            $from = $this->safe_date($chart_conf['release_from'] ?? '');
            $to   = $this->safe_date($chart_conf['release_to'] ?? '');
            $res = $this->fetch_tracks_release_window($from,$to,$market, 1500);
            if(!is_wp_error($res)){
                $res = apply_filters('waki_release_window_items', $res, $from, $to, $market, $chart_key, $chart_conf);
                foreach($res as $row){ $all_items[]=$row; $all_by_tid[$row['track_id']][]=$row; }
            }
        }

        $total_gathered = count($all_items);
        $after_dedupe   = count($all_by_tid);

        // sample duplicates visualization
        $dups = [];
        foreach($all_by_tid as $tid=>$occ){ if(count($occ)>1){ $dups[]=['track_id'=>$tid,'playlists'=>array_values(array_unique(array_filter(array_map(fn($o)=>$o['playlist_id'] ?? '',$occ))))]; } }
        $dedupe_sample = $dups;

        // album-date filter
        $filter_from = $this->safe_date($chart_conf['filter_release_from'] ?? '');
        $filter_to   = $this->safe_date($chart_conf['filter_release_to'] ?? '');
        $all_by_tid = $this->apply_album_date_filter($all_by_tid, $filter_from, $filter_to);

        // Enrich minimal (popularity, links, label) to be able to score preview
        $flat=[]; foreach($all_by_tid as $tid=>$arr){ $flat[]=$arr[0]; }
        $this->enrich_tracks($flat);
        $enriched=[]; foreach($flat as $f){ $enriched[$f['track_id']]=$f; }

        // Optional origin filter — STRICT
        $origin_filter = $this->valid_iso($chart_conf['origin_filter'] ?? '');
        if ($origin_filter){
            $this->filter_tracks_by_origin_strict($enriched, $all_by_tid, $origin_filter);
        }

        // Score to preview top 5 (with floor)
        $opts=$this->get_options();
        $alpha=floatval($opts['alpha']); $beta=floatval($opts['beta']); $gamma=floatval($opts['gamma']); $presence_bonus=floatval($opts['presence_bonus']);
        $scored=[];
        foreach($all_by_tid as $tid=>$occurs){
            if(!isset($enriched[$tid])) continue;
            $pop = intval($enriched[$tid]['popularity'] ?? 0);
            $best_rank = 0;
            if(!empty($occurs[0]['playlist_id'])){
                $best_rank = min(array_map(fn($r)=>intval($r['rank_in_src'] ?: 9999), $occurs));
            }
            $playlists_count = !empty($occurs[0]['playlist_id']) ? count($occurs) : 1;

            $min_days=9999;
            foreach($occurs as $r){
                $basis = !empty($r['added_at']) ? $r['added_at'] : (!empty($r['album_release_date'])?$r['album_release_date'].' 00:00:00': null);
                if($basis){
                    $days=max(0, floor((time()-strtotime($basis.' UTC'))/86400));
                    if($days<$min_days) $min_days=$days;
                }
            }
            $recency = max(0, 28 - min($min_days, 28));

            $norm_pop  = $pop / 100;
            $norm_rank = $best_rank>0 ? (1 / max(1,$best_rank)) : 0;
            $presence_mult = 1 + ($presence_bonus * max(0,$playlists_count-1));

            $playlist_weight_sum = 1.0;
            if(!empty($occurs[0]['playlist_id'])){
                $playlist_weight_sum = 0.0;
                foreach($occurs as $r){ $playlist_weight_sum += ($weights[$r['playlist_id']] ?? 1.0); }
                if ($playlist_weight_sum <= 0) $playlist_weight_sum = 1.0;
            }

            $score_component = ($alpha*($norm_pop)) + ($beta*$norm_rank) + ($gamma*($recency/28));
            $score = $score_component * $playlist_weight_sum * $presence_mult;
            if ($score <= 0) $score = 0.05;

            $scored[] = $enriched[$tid] + ['track_id'=>$tid,'score'=>$score];
        }
        usort($scored, fn($a,$b)=>($a['score']<$b['score'])?1:-1);
        $top_preview = array_slice($scored,0,5);
        $final_eligible = count($scored);
        $limitWanted = max(10, min(200, intval($chart_conf['chart_limit'] ?? 100)));

        $rules = [
            'source_type' => $chart_conf['source_type'] ?? 'playlists',
            'market'      => $market,
            'origin_filter' => $origin_filter ?: '—',
            'album_release_from' => $filter_from ?: '—',
            'album_release_to'   => $filter_to ?: '—',
            'weights' => $weights,
            'limit'   => $limitWanted,
        ];
        if($weights_err){ $rules['weights_error'] = $weights_err; }
        if($fb_weights_err){ $rules['fallback_weights_error'] = $fb_weights_err; }

        $counts = [
            'Total items gathered' => $total_gathered,
            'After dedupe (unique tracks)' => $after_dedupe,
            'After album-date filter' => count($all_by_tid),
            'After origin filter' => count($all_by_tid),
            'Final eligible (before clamp)' => $final_eligible,
            'To be published (limit)' => min($limitWanted, $final_eligible),
        ];

        $state_log[] = 'Ready';

        return [
            'state_log'=>$state_log,
            'playlist_checks'=>$playlist_checks,
            'rules'=>$rules,
            'counts'=>$counts,
            'top_preview'=>$top_preview,
            'dedupe_sample'=>$dedupe_sample,
            'parsed_meta'=>$meta
        ];
    }

    private function default_archive_intro(){
        return __('From club heaters to quiet stunners, this is a living record of Kenyan music, tracked weekly, filtered by region and genre, and more.', 'wakilisha-charts');
    }

    public function register_rest_routes(){
        register_rest_route('wakicharts/v1', '/archive-intro', [
            [
                'methods'  => WP_REST_Server::READABLE,
                'callback' => [$this, 'rest_get_archive_intro'],
                'permission_callback' => '__return_true',
            ],
            [
                'methods'  => WP_REST_Server::EDITABLE,
                'callback' => [$this, 'rest_update_archive_intro'],
                'permission_callback' => function(){ return current_user_can('manage_options'); },
                'args'     => [
                    'intro' => [
                        'type' => 'string',
                        'required' => true,
                        'sanitize_callback' => 'sanitize_text_field',
                    ],
                ],
            ],
        ]);
    }

    public function rest_get_archive_intro($request){
        return rest_ensure_response([
            'intro' => get_option(self::ARCHIVE_INTRO, $this->default_archive_intro())
        ]);
    }

    public function rest_update_archive_intro($request){
        $intro = $request->get_param('intro');
        update_option(self::ARCHIVE_INTRO, $intro);
        return rest_ensure_response(['intro' => $intro]);
    }

}
