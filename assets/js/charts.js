document.addEventListener('click', function(e){
  const chip = e.target.closest('.waki-chip[data-country], .waki-chip[data-genre], .waki-chip[data-language]');
  if(!chip) return;
  e.preventDefault();
  const active = window.wakiActiveFilters || {country:null,genre:null,language:null};
  window.wakiActiveFilters = active;
  let type, value;
  if(chip.dataset.country){type='country'; value=chip.dataset.country;}
  else if(chip.dataset.genre){type='genre'; value=chip.dataset.genre;}
  else {type='language'; value=chip.dataset.language;}
  if(active[type] === value){
    active[type] = null;
    chip.classList.remove('active');
  } else {
    active[type] = value;
    document.querySelectorAll('.waki-chip[data-' + type + ']').forEach(c=>c.classList.toggle('active', c===chip));
  }
  const entries = document.querySelectorAll('[data-chart-item]');
  entries.forEach(entry => {
    let show = true;
    for(const key in active){
      const val = active[key];
      if(!val) continue;
      const attrs = (entry.getAttribute('data-' + key) || '').split(/\s+/);
      if(!attrs.includes(val)) { show = false; break; }
    }
    entry.style.display = show ? '' : 'none';
  });
});
