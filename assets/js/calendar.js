document.addEventListener('DOMContentLoaded',function(){
  var today=new Date().toISOString().split('T')[0];
  document.querySelectorAll('.waki-calendar td[data-date="'+today+'"]')
    .forEach(function(td){td.classList.add('today');});
});
