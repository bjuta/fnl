        <template id="waki-archive-hero">
          <div class="waki-archive-hero"<?php if($hero_img) echo ' style="--hero:url(\''.$hero_img.'\')"'; ?>>
            <div class="waki-hero-inner">
              <h1 class="waki-hero-title"><?php esc_html_e('WAKILISHA Charts', 'wakilisha-charts'); ?></h1>
              <p class="waki-hero-sub"><?php esc_html_e('From club heaters to quiet stunners, this is a living record of Kenyan music, tracked weekly, filtered by region and genre, and more.', 'wakilisha-charts'); ?></p>
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
                  $cover = get_post_meta($cid,'_waki_cover_url',true);
                  if(!$cover){
                    $thumb = get_the_post_thumbnail_url(null,'large');
                    if($thumb) $cover = $thumb;
                  }

                  $country = '';
                  $charts  = $this->get_charts();
                  if($key && !empty($charts[$key]['origin_filter'])){
                    $iso = strtoupper($charts[$key]['origin_filter']);
                    $map = $this->iso_list();
                    $country = $map[$iso] ?? $iso;
                  }
                  $updated = get_post_modified_time(get_option('date_format'), false, $cid);
                  $genres = [];
                  $terms = get_the_terms($cid,'category');
                  if($terms && !is_wp_error($terms)){
                    $genres = wp_list_pluck($terms,'name');
                  }
                ?>
                <a class="cover" href="<?php the_permalink(); ?>"<?php if($cover) echo ' style="background-image:url(\''.esc_url($cover).'\')"'; ?>></a>
                <div class="inner">
                  <h3><a href="<?php the_permalink(); ?>"><?php the_title(); ?></a></h3>
                  <p class="meta"><?php echo sprintf(esc_html__('Updated %s', 'wakilisha-charts'), esc_html($updated)); ?></p>
                  <?php if($country){ ?><p class="meta"><?php echo sprintf(esc_html__('Region: %s', 'wakilisha-charts'), esc_html($country)); ?></p><?php } ?>
                  <?php if($genres){ ?><p class="meta"><?php echo sprintf(esc_html__('Genres: %s', 'wakilisha-charts'), esc_html(implode(', ',$genres))); ?></p><?php } ?>
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
