<?php
/* Artist profile template */
$artist = $artist ?? get_query_var('waki_artist');
if (!$artist) {
  echo '<p>'.esc_html__('Artist not found.', 'wakilisha-charts').'</p>';
  return;
}
if (is_array($artist)) {
  $artist = (object) $artist;
}
?>
<section class="waki-wrap waki-artist-page">
  <div class="waki-artist-hero">
    <?php if (!empty($artist->image_url)) : ?>
      <div class="artist-image">
        <img src="<?php echo esc_url($artist->image_url); ?>" alt="<?php echo esc_attr($artist->artist_name); ?>" />
      </div>
    <?php endif; ?>
    <div class="artist-info">
      <h2 class="artist-name"><?php echo esc_html($artist->artist_name); ?></h2>
      <?php if (!empty($artist->biography)) : ?>
        <p class="artist-bio-snippet"><?php echo esc_html(wp_trim_words($artist->biography, 30, '...')); ?></p>
      <?php endif; ?>
        <div class="artist-meta">
          <?php if (!empty($artist->followers)) : ?>
            <span class="meta-item"><?php echo sprintf(esc_html__('%s followers', 'wakilisha-charts'), number_format_i18n(intval($artist->followers))); ?></span>
          <?php endif; ?>
        </div>
    </div>
  </div>

  <nav class="waki-artist-tabs">
    <button class="waki-tab-btn active" data-tab="overview"><?php esc_html_e('Overview', 'wakilisha-charts'); ?></button>
    <button class="waki-tab-btn" data-tab="discography"><?php esc_html_e('Discography', 'wakilisha-charts'); ?></button>
    <button class="waki-tab-btn" data-tab="charts"><?php esc_html_e('Chart History', 'wakilisha-charts'); ?></button>
    <button class="waki-tab-btn" data-tab="videos"><?php esc_html_e('Videos', 'wakilisha-charts'); ?></button>
    <button class="waki-tab-btn" data-tab="related"><?php esc_html_e('Related Artists', 'wakilisha-charts'); ?></button>
  </nav>

  <div class="waki-artist-tab-content active" data-tab-content="overview">
    <?php
    $has_overview = false;
    if (!empty($artist->biography)) {
      echo '<div class="artist-biography">' . wpautop(esc_html($artist->biography)) . '</div>';
      $has_overview = true;
    }
    if (!empty($artist->top_tracks) && is_array($artist->top_tracks)) {
      echo '<ul class="artist-tracks">';
      foreach ($artist->top_tracks as $track) {
        echo '<li><div class="track">' . esc_html($track) . '</div></li>';
      }
      echo '</ul>';
      $has_overview = true;
    }
    if (!$has_overview) {
      echo '<p>'.esc_html__('No overview available.', 'wakilisha-charts').'</p>';
    }
    ?>
  </div>

  <div class="waki-artist-tab-content" data-tab-content="discography">
    <?php if (!empty($artist->discography) && is_array($artist->discography)) : ?>
      <ul class="artist-discography">
        <?php foreach ($artist->discography as $item) : ?>
          <li><div class="release"><?php echo esc_html($item); ?></div></li>
        <?php endforeach; ?>
      </ul>
    <?php else : ?>
      <p><?php esc_html_e('No discography available.', 'wakilisha-charts'); ?></p>
    <?php endif; ?>
  </div>

  <div class="waki-artist-tab-content" data-tab-content="charts">
    <?php if (!empty($artist->chart_history) && is_array($artist->chart_history)) : ?>
      <ul class="artist-chart-history">
        <?php foreach ($artist->chart_history as $entry) : ?>
          <li><div class="entry"><?php echo esc_html($entry); ?></div></li>
        <?php endforeach; ?>
      </ul>
    <?php else : ?>
      <p><?php esc_html_e('No chart history available.', 'wakilisha-charts'); ?></p>
    <?php endif; ?>
  </div>

  <div class="waki-artist-tab-content" data-tab-content="videos">
    <?php if (!empty($artist->videos) && is_array($artist->videos)) : ?>
      <ul class="artist-videos">
        <?php foreach ($artist->videos as $video) : ?>
          <li><div class="video-item"><a href="<?php echo esc_url($video); ?>" target="_blank" rel="noopener noreferrer"><?php echo esc_html($video); ?></a></div></li>
        <?php endforeach; ?>
      </ul>
    <?php else : ?>
      <p><?php esc_html_e('No videos available.', 'wakilisha-charts'); ?></p>
    <?php endif; ?>
  </div>

  <div class="waki-artist-tab-content" data-tab-content="related">
    <?php if (!empty($artist->related_artists) && is_array($artist->related_artists)) : ?>
      <ul class="artist-related">
        <?php foreach ($artist->related_artists as $rel) : ?>
          <li><div class="related-artist"><?php echo esc_html($rel); ?></div></li>
        <?php endforeach; ?>
      </ul>
    <?php else : ?>
      <p><?php esc_html_e('No related artists available.', 'wakilisha-charts'); ?></p>
    <?php endif; ?>
  </div>
</section>

