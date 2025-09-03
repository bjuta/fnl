        <template id="waki-archive-hero">
          <div class="waki-archive-hero"<?php if($hero_img) echo ' style="--hero:url(\''.$hero_img.'\')"'; ?>>
            <div class="waki-hero-inner">
              <h1 class="waki-hero-title"><?php esc_html_e('WAKILISHA Charts', 'wakilisha-charts'); ?></h1>
              <?php $intro = get_option(Waki_Charts::ARCHIVE_INTRO, $this->default_archive_intro()); ?>
              <p class="waki-hero-sub"><?php echo esc_html($intro); ?></p>
            </div>
          </div>
        </template>
        <section class="waki-wrap waki-fw" id="waki-archive" data-loading>
          <div class="waki-skel" style="height:500px"></div>

          <?php if(!$q->have_posts()): ?>
            <p><?php esc_html_e('No charts yet.', 'wakilisha-charts'); ?></p>
          <?php else: ?>
            <div class="waki-archive-grid"><!-- card container -->
            <?php while($q->have_posts()): $q->the_post(); ?>
              <article class="waki-arch-card">
                <?php
                  $cid   = get_the_ID();
                  $key   = get_post_meta($cid,'_waki_chart_key',true);
                  $date  = get_post_meta($cid,'_waki_chart_date',true);
                  $sid   = get_post_meta($cid,'_waki_snapshot_id',true);
                  $rows  = $this->get_chart_rows($key,$date,10,$sid);
                  $imgs  = array_filter(array_column($rows,'album_image_url'));
                  $cover = $imgs ? $imgs[array_rand($imgs)] : '';
                  if(!$cover){
                    $cover = get_post_meta($cid,'_waki_cover_url',true);
                    if(!$cover){
                      $thumb = get_the_post_thumbnail_url(null,'large');
                      if($thumb) $cover = $thumb;
                    }
                  }

                  $countries = get_the_terms($cid,'waki_country');
                  if(is_wp_error($countries) || !$countries){ $countries = []; }
                  $genres = get_the_terms($cid,'waki_genre');
                  if(is_wp_error($genres) || !$genres){ $genres = []; }
                  $languages = get_the_terms($cid,'waki_language');
                  if(is_wp_error($languages) || !$languages){ $languages = []; }
                  $updated = get_post_modified_time(get_option('date_format'), false, $cid);
                ?>
                <a class="cover" href="<?php the_permalink(); ?>"<?php if($cover) echo ' style="background-image:url(\''.esc_url($cover).'\')"'; ?>></a>
                <div class="inner">
                  <h3><a href="<?php the_permalink(); ?>"><?php the_title(); ?></a></h3>
                  <p class="meta"><?php echo sprintf(esc_html__('Updated %s', 'wakilisha-charts'), esc_html($updated)); ?></p>
                  <div class="waki-hero-meta">
                    <?php foreach($countries as $c): $code = strtoupper($c->slug); ?>
                      <a class="waki-chip" href="?country=<?php echo esc_attr($code); ?>" data-filter="country:<?php echo esc_attr($code); ?>"><?php echo esc_html($code); ?></a>
                    <?php endforeach; ?>
                    <?php foreach($genres as $g): ?>
                      <a class="waki-chip" href="?genre=<?php echo esc_attr($g->slug); ?>" data-filter="genre:<?php echo esc_attr($g->slug); ?>"><?php echo esc_html($g->name); ?></a>
                    <?php endforeach; ?>
                    <?php foreach($languages as $l): ?>
                      <a class="waki-chip" href="?language=<?php echo esc_attr($l->slug); ?>" data-filter="language:<?php echo esc_attr($l->slug); ?>"><?php echo esc_html($l->name); ?></a>
                    <?php endforeach; ?>
                  </div>
                  <a class="view-link" href="<?php the_permalink(); ?>"><?php esc_html_e('View Chart', 'wakilisha-charts'); ?></a>
                </div>
              </article>
            <?php endwhile; wp_reset_postdata(); ?>
            </div>

            <div class="waki-pager">
              <?php
                echo paginate_links([
                    'total'  => $q->max_num_pages,
                    'current'=> $paged,
                    'prev_text'=>'«',
                    'next_text'=>'»'
                ]);
              ?>
            </div>
          <?php endif; ?>
        </section>
