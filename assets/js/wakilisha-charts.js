// Remove skeletons on load
document.addEventListener('DOMContentLoaded',function(){
  document.querySelectorAll('[data-loading]').forEach(el=>el.removeAttribute('data-loading'));
  const liftHero = id=>{
    const tpl = document.getElementById(id);
    if(!tpl) return;
    const header = document.querySelector('header');
    const hero = tpl.content.firstElementChild.cloneNode(true);
    if(header){
      const setH = ()=>hero.style.setProperty('--header-h', header.getBoundingClientRect().height + 'px');
      setH();
      window.addEventListener('resize', setH);
      header.insertAdjacentElement('afterend', hero);
    }
    tpl.remove();
  };
  liftHero('waki-chart-hero');
  liftHero('waki-archive-hero');

  // drag-to-scroll for archive cards
  const grid = document.querySelector('#waki-archive .waki-archive-grid');
  if(grid){
    let isDown = false;
    let startX = 0;
    let scrollStart = 0;
    grid.addEventListener('pointerdown', e=>{
      isDown = true;
      startX = e.clientX;
      scrollStart = grid.scrollLeft;
    });
    grid.addEventListener('pointermove', e=>{
      if(!isDown) return;
      e.preventDefault();
      const x = e.clientX - startX;
      grid.scrollLeft = scrollStart - x;
    }, {passive:false});
    ['pointerup','pointerleave'].forEach(ev=>{
      grid.addEventListener(ev, ()=>{ isDown = false; });
    });
  }
});

// Recent weeks tab switcher
document.addEventListener('click',function(e){
  const btn = e.target.closest('.waki-hbtn'); if(!btn) return;
  const wrap = btn.closest('.waki-main'); if(!wrap) return;
  const date = btn.getAttribute('data-date');
  wrap.querySelectorAll('.waki-hbtn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  wrap.querySelectorAll('.waki-list[data-for-date]').forEach(list=>{
    list.style.display = (list.getAttribute('data-for-date')===date)?'block':'none';
  });
});

// Toggle mini history
document.addEventListener('click',function(e){
  const b = e.target.closest('[data-show-history]'); if(!b) return;
  const wrap = b.parentElement.querySelector('.waki-mini-wrap'); if(!wrap) return;
  const wasHidden = (wrap.style.display==='none' || !wrap.style.display);
  wrap.style.display = wasHidden ? 'block' : 'none';
  b.classList.toggle('open', wasHidden);
  b.setAttribute('aria-expanded', wasHidden ? 'true' : 'false');
  if(wasHidden){
    const scroller = wrap.querySelector('[data-spark-wrap]');
    if(scroller){
      setTimeout(()=>{ scroller.scrollLeft = scroller.scrollWidth - scroller.clientWidth; },50);
    }
  }
});

// Hover / click tooltip for spark nodes
(function(){
  function showTip(container, msg){
    const tip = container.querySelector('.waki-tooltip');
    if(!tip) return;
    tip.textContent = msg;
    tip.hidden = false;
  }
  function hideTip(container){
    const tip = container.querySelector('.waki-tooltip');
    if(tip) tip.hidden = true;
  }
  document.addEventListener('pointerover', function(e){
    const pt = e.target.closest('[data-pt]'); if(!pt) return;
    const cont = pt.closest('.waki-spark-wrap'); if(!cont) return;
    const date = pt.getAttribute('data-date');
    const pos = pt.getAttribute('data-pos');
    showTip(cont, (pos && pos !== '—') ? ('Position #' + pos + ' on ' + date) : ('No chart entry on ' + date));
  });
  document.addEventListener('pointerout', function(e){
    const pt = e.target.closest('[data-pt]'); if(!pt) return;
    const cont = pt.closest('.waki-spark-wrap'); if(!cont) return;
    hideTip(cont);
  });
  document.addEventListener('click', function(e){
    const pt = e.target.closest('[data-pt]'); if(!pt) return;
    const cont = pt.closest('.waki-spark-wrap'); if(!cont) return;
    const date = pt.getAttribute('data-date');
    const pos = pt.getAttribute('data-pos');
    showTip(cont, (pos && pos !== '—') ? ('Position #' + pos + ' on ' + date) : ('No chart entry on ' + date));
  });
})();

// Artist profile tabs
document.addEventListener('click', function(e){
  const btn = e.target.closest('.waki-tab-btn'); if(!btn) return;
  const container = btn.closest('.waki-artist-page'); if(!container) return;
  const tab = btn.getAttribute('data-tab');
  container.querySelectorAll('.waki-tab-btn').forEach(b=>b.classList.toggle('active', b===btn));
  container.querySelectorAll('.waki-artist-tab-content').forEach(sec=>{
    sec.classList.toggle('active', sec.getAttribute('data-tab-content')===tab);
  });
});
