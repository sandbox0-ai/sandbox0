import RFB from './novnc/core/rfb.js';

const desktop = document.getElementById('desktop');
const status = document.getElementById('status');
let client;

function connect() {
  if (client) client.disconnect();
  desktop.replaceChildren();
  status.textContent = 'Connecting…';
  const endpoint = new URL('/vnc', location.href);
  endpoint.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const current = new RFB(desktop, endpoint.href, { shared: true });
  client = current;
  current.scaleViewport = true;
  current.resizeSession = true;
  current.focusOnClick = true;
  current.showDotCursor = true;
  current.qualityLevel = 6;
  current.compressionLevel = 2;
  current.addEventListener('connect', () => {
    if (client === current) status.textContent = 'You and your agent share this browser';
  });
  current.addEventListener('disconnect', () => {
    if (client === current) status.textContent = 'Disconnected — reconnect, or renew your preview link';
  });
  current.addEventListener('securityfailure', () => {
    if (client === current) status.textContent = 'Viewer connection rejected';
  });
}

document.getElementById('reconnect').addEventListener('click', connect);
connect();
