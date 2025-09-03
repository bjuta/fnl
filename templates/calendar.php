<?php
if (!defined('ABSPATH')) exit;
?>
<div class="waki-calendar" data-format="<?php echo esc_attr($format_slug); ?>" data-year="<?php echo esc_attr($year_num); ?>">
<?php
for ($m = 1; $m <= 12; $m++):
    $first = new DateTime(sprintf('%04d-%02d-01', $year_num, $m));
    $days_in_month = (int) $first->format('t');
    $wday = (int) $first->format('N'); // 1 (Mon) - 7 (Sun)
?>
  <div class="waki-cal-month">
    <h4><?php echo esc_html($first->format('F')); ?></h4>
    <table class="waki-cal-table">
      <thead>
        <tr>
          <th>Mon</th><th>Tue</th><th>Wed</th><th>Thu</th><th>Fri</th><th>Sat</th><th>Sun</th>
        </tr>
      </thead>
      <tbody>
        <tr>
<?php
    for ($i = 1; $i < $wday; $i++) echo '<td class="pad"></td>';
    for ($d = 1; $d <= $days_in_month; $d++):
        $date = sprintf('%04d-%02d-%02d', $year_num, $m, $d);
        $monday = date('Y-m-d', strtotime($date . ' monday this week'));
        $cls = [];
        if (isset($week_lookup[$monday])) $cls[] = 'has-chart';
        $cls_attr = $cls ? ' class="' . esc_attr(implode(' ', $cls)) . '"' : '';
        echo '<td data-date="' . esc_attr($date) . '"' . $cls_attr . '><a href="' . esc_url(home_url('/' . Waki_Charts::CPT_SLUG . '/' . $format_slug . '/' . $monday . '/')) . '">' . $d . '</a></td>';
        if (($wday + $d - 1) % 7 == 0 && $d != $days_in_month) echo '</tr><tr>';
    endfor;
    $last = ($wday + $days_in_month - 1) % 7;
    if ($last !== 0) {
        for ($i = $last + 1; $i <= 7; $i++) echo '<td class="pad"></td>';
    }
?>
        </tr>
      </tbody>
    </table>
  </div>
<?php endfor; ?>
</div>
