'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execute = promisify(execFile);

const CONTROL_PORT = 9224;
const PANEL_HEIGHT = 52;

// All assets are runtime files. Persistent user data belongs only in the profile.
function prepareDesktop(directory) {
  const save = (name, contents, mode = 0o644) => {
    const file = path.join(directory, name);
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o755 });
    fs.writeFileSync(file, contents, { mode });
    fs.chmodSync(file, mode);
  };
  save('launch-browser', `#!/bin/sh\nexec curl -fsS --max-time 40 -X POST http://127.0.0.1:${CONTROL_PORT}/open\n`, 0o755);
  save('browser.desktop', `[Desktop Entry]\nType=Application\nName=Browser\nExec=${directory}/launch-browser\nIcon=web-browser\nTerminal=false\n`);
  save('menu.xml', `<?xml version="1.0"?>
<openbox_menu xmlns="http://openbox.org/3.4/menu">
  <menu id="root-menu" label="Browser desktop">
    <item label="Open browser"><action name="Execute"><command>${directory}/launch-browser</command></action></item>
    <separator/>
    <menu id="client-list-combined-menu" label="Windows"/>
  </menu>
</openbox_menu>\n`);
  // Retain the distribution's native titlebar and keyboard bindings, but expose
  // only actions supported by this browser desktop in the background menu.
  const defaults = fs.readFileSync('/etc/xdg/openbox/rc.xml', 'utf8');
  const config = defaults.replace(/(<desktops>[\s\S]*?<number>)\d+/, (_match, prefix) => prefix + '1')
    .replace(/<menu>[\s\S]*?<\/menu>/, `<menu><file>${directory}/menu.xml</file></menu>`);
  save('rc.xml', config);
  save('panel.conf', `
rounded = 0
border_width = 0
background_color = #172b3a 100
rounded = 6
border_width = 0
background_color = #294556 100
background_color_hover = #3a5c70 100
panel_items = LT
panel_size = 100% ${PANEL_HEIGHT}
panel_position = bottom center horizontal
panel_padding = 8 6 8
panel_background_id = 1
panel_layer = top
strut_policy = follow_size
autohide = 0
launcher_icon_size = 32
launcher_padding = 4 0 8
launcher_item_app = ${directory}/browser.desktop
taskbar_mode = single_desktop
taskbar_hide_if_empty = 0
taskbar_hide_inactive_tasks = 0
task_maximum_size = 280 38
task_padding = 8 3 6
task_font = sans 10
task_font_color = #dbe8ef 100
task_active_font_color = #ffffff 100
task_background_id = 2
task_active_background_id = 2
task_iconified_background_id = 2
task_icon = 1
task_text = 1
task_tooltip = 1
mouse_left = toggle_iconify
mouse_middle = none
mouse_right = none
mouse_scroll_up = none
mouse_scroll_down = none
`);
  const width = 960, height = 600;
  const pixels = Buffer.alloc(width * height * 3);
  for (let row = 0; row < height; row++) {
    for (let col = 0; col < width; col++) {
      const t = row / height;
      const light = Math.max(0, 1 - Math.hypot(col / width - 0.75, t - 0.3));
      const offset = (row * width + col) * 3;
      pixels[offset] = Math.round(12 + light * 15);
      pixels[offset + 1] = Math.round(26 + light * 24 - t * 8);
      pixels[offset + 2] = Math.round(40 + light * 32 - t * 10);
    }
  }
  save('background.ppm', Buffer.concat([Buffer.from(`P6\n${width} ${height}\n255\n`), pixels]));
  for (const name of ['.cache', '.config']) fs.mkdirSync(path.join(directory, name), { recursive: true, mode: 0o700 });
}

function workArea(text) {
  const active = text.split('\n').find(line => /^\d+\s+\*/.test(line));
  const match = active?.match(/WA:\s+(-?\d+),(-?\d+)\s+(\d+)x(\d+)/);
  if (!match || Number(match[3]) <= 0 || Number(match[4]) <= 0) return null;
  return match.slice(1).map(Number);
}

function windowRows(text, pid) {
  return text.trim().split('\n').flatMap(line => {
    const [id, desktop, owner, x, y, width, height] = line.trim().split(/\s+/);
    if (Number(owner) !== pid || Number(desktop) < 0 || !/^0x[\da-f]+$/i.test(id)) return [];
    return [{ id, x: Number(x), y: Number(y), width: Number(width), height: Number(height) }];
  });
}

function fittedGeometry(window, area, properties) {
  if (/\b_NET_WM_STATE_(?:FULLSCREEN|HIDDEN|MAXIMIZED_HORZ|MAXIMIZED_VERT)\b/.test(properties)) return null;
  const extents = properties.match(/_NET_FRAME_EXTENTS[^=]*=\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+)/);
  const [left, right, top, bottom] = extents ? extents.slice(1).map(Number) : [0, 0, 0, 0];
  const [originX, originY, areaWidth, areaHeight] = area;
  const limitWidth = areaWidth - left - right, limitHeight = areaHeight - top - bottom;
  if (limitWidth <= 0 || limitHeight <= 0) return null;
  const width = window.width > limitWidth ? Math.floor(limitWidth * 0.9) : window.width;
  const height = window.height > limitHeight ? Math.floor(limitHeight * 0.9) : window.height;
  const x = Math.max(originX, Math.min(window.x - left, originX + limitWidth - width));
  const y = Math.max(originY, Math.min(window.y - top, originY + limitHeight - height));
  if ([x, y, width, height].every((value, index) => value === [window.x - left, window.y - top, window.width, window.height][index])) return null;
  return `0,${x},${y},${width},${height}`;
}

class DesktopMaintenance {
  constructor(directory, currentPid, command = execute) {
    this.directory = directory;
    this.currentPid = currentPid;
    this.command = command;
    this.dimensions = '';
  }
  async refresh(signal) {
    const run = (command, args) => this.command(command, args, { timeout: 2000, signal });
    const desktops = (await run('wmctrl', ['-d'])).stdout;
    const dimensions = desktops.match(/^\d+\s+\*.*?DG:\s+(\d+x\d+)/m)?.[1];
    if (dimensions && dimensions !== this.dimensions) {
      await run('feh', ['--no-fehbg', '--bg-fill', path.join(this.directory, 'background.ppm')]);
      this.dimensions = dimensions;
    }
    const area = workArea(desktops), pid = this.currentPid();
    if (!area || !pid) return;
    const windows = windowRows((await run('wmctrl', ['-lpG'])).stdout, pid);
    for (const window of windows) {
      const properties = (await run('xprop', ['-id', window.id, '_NET_WM_STATE', '_NET_FRAME_EXTENTS'])).stdout;
      const geometry = fittedGeometry(window, area, properties);
      if (geometry && this.currentPid() === pid && !signal.aborted) await run('wmctrl', ['-ir', window.id, '-e', geometry]);
    }
  }
}

module.exports = { CONTROL_PORT, PANEL_HEIGHT, prepareDesktop, workArea, windowRows, fittedGeometry, DesktopMaintenance };
