<?php ?>
        <div class="waki-hist-mini" data-mini>
          <div class="waki-spark-wrap" data-spark-wrap>
            <svg viewBox="0 0 <?php echo $w; ?> <?php echo $h; ?>" width="<?php echo $w; ?>" height="<?php echo $h; ?>" class="waki-spark" data-spark data-lastcount="<?php echo max(10,count($pts)); ?>">
              <rect x="0" y="0" width="<?php echo $w; ?>" height="<?php echo $h; ?>" fill="#fff"/>
              <!-- Top/Bottom rails -->
              <line x1="0" y1="<?php echo $pad; ?>" x2="<?php echo $w; ?>" y2="<?php echo $pad; ?>" stroke="#e5e7eb" stroke-width="1"/>
              <line x1="0" y1="<?php echo $h-$pad; ?>" x2="<?php echo $w; ?>" y2="<?php echo $h-$pad; ?>" stroke="#e5e7eb" stroke-width="1"/>
              <text x="4" y="<?php echo $pad+10; ?>" fill="#6b7280" font-size="10">#1</text>
              <text x="4" y="<?php echo $h-$pad-2; ?>" fill="#6b7280" font-size="10">#<?php echo $maxRank; ?></text>

              <?php if($path): ?>
                <path d="<?php echo esc_attr($path); ?>" fill="none" stroke="#111827" stroke-width="2"/>
                <?php foreach($nodes as $n): ?>
                  <circle cx="<?php echo $n['x']; ?>" cy="<?php echo $n['y']; ?>" r="5" fill="#111827" stroke="transparent" stroke-width="10" style="cursor:pointer"
                          data-pt data-date="<?php echo esc_attr($n['date']);?>" data-pos="<?php echo esc_attr($n['pos']);?>"></circle>
                <?php endforeach; ?>
              <?php endif; ?>
            </svg>
            <div class="waki-tooltip" role="status" aria-live="polite" hidden></div>
          </div>

          <table class="waki-mini-table">
            <thead><tr><th><?php esc_html_e('Date', 'wakilisha-charts'); ?></th><th><?php esc_html_e('Position', 'wakilisha-charts'); ?></th></tr></thead>
            <tbody>
              <?php foreach(array_slice(array_reverse($pts),0,10) as $p): // newest 10 in table ?>
                <tr><td><?php echo esc_html($p['date']); ?></td><td><?php echo $p['position'] ? '#'.intval($p['position']) : '—'; ?></td></tr>
              <?php endforeach; ?>
            </tbody>
          </table>
          <p class="waki-help"><?php esc_html_e('Tip: scroll the graph horizontally to see older weeks. Hover or tap points for exact position and date.', 'wakilisha-charts'); ?></p>
        </div>
