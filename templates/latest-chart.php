<?php if($hero && $bg): ?>
        <template id="waki-chart-hero">
          <div class="waki-chart-hero" style="--hero:url('<?php echo esc_url($bg); ?>')">
            <div class="waki-hero-inner">
              <?php if($atts['show_title']==='1'): ?>
                <h1 class="waki-hero-title"><?php echo esc_html($atts['title']);?></h1>
              <?php endif; ?>
              <p class="waki-hero-sub"><?php echo sprintf(esc_html__('Featuring %s and more', 'wakilisha-charts'), esc_html($intro_artists)); ?></p>
              <div class="waki-hero-meta">
                <?php if($country){ ?><span class="waki-chip"><?php echo esc_html($country); ?></span><?php } ?>
                <?php if($updated){ ?><span class="waki-chip"><?php echo sprintf(esc_html__('Updated %s', 'wakilisha-charts'), esc_html($updated)); ?></span><?php } ?>
              </div>
            </div>
          </div>
        </template>
        <?php endif; ?>
        <section class="waki-wrap <?php echo ($atts['fullwidth']==='1'?'waki-fw':''); ?>" data-waki-chart="<?php echo esc_attr($chart_key); ?>" data-loading>
          <div class="waki-skel" style="height:400px"></div>
          <div class="waki-main">
            <?php if($atts['show_title']==='1' && !$hero): ?>
               <h2 class="waki-arch-title" style="text-align:left"><?php echo esc_html($atts['title']);?></h2>
            <?php endif; ?>

            <div class="waki-history" data-chart="<?php echo esc_attr($chart_key); ?>">
              <div class="waki-history-nav" role="tablist">
                <?php $first=true; foreach($dates as $d): ?>
                  <button class="waki-hbtn <?php echo $first?'active':''; ?>" role="tab" data-date="<?php echo esc_attr($d); ?>"><?php echo esc_html($d); ?></button>
                <?php $first=false; endforeach; ?>
              </div>
            </div>

            <?php
            $firstList = true;
            foreach($dates as $d):
              $rows = $this->get_chart_rows($chart_key,$d, $limit, ($d===$first_date)?$sid:'');
              if (!$rows) continue;
              $chunks = array_chunk($rows, 10);
              ?>
              <div class="waki-list" data-for-date="<?php echo esc_attr($d); ?>" style="<?php echo $firstList?'':'display:none'; ?>">
                <?php foreach($chunks as $ci => $chunk){
                    $hidden = $ci === 0 ? '' : 'style="display:none"';
                    echo '<div class="waki-chunk" '.$hidden.'>';
                    foreach($chunk as $r){
                        echo $this->render_entry_row($r, $chart_key, $d);
                    }
                    echo '</div>';
                    if($ci < count($chunks)-1){
                        $btn_style = $ci === 0 ? '' : 'style="display:none"';
                        echo '<div class="waki-load-wrap" '.$btn_style.'><button class="waki-load-more">'.esc_html__('Load more', 'wakilisha-charts').'</button></div>';
                    }
                } ?>
              </div>
            <?php $firstList=false; endforeach; ?>
          </div>
        </section>
