import RFB from './novnc/core/rfb.js';

// Small viewers scale the remote desktop; they must not resize native browser
// windows below their usable minimum. Keep the viewer's aspect ratio.
export function desktopSize(width, height) {
  const factor = Math.max(1, 640 / width, 480 / height);
  return { w: Math.ceil(width * factor), h: Math.ceil(height * factor) };
}

export default class DesktopRFB extends RFB {
  _screenSize() {
    const size = super._screenSize();
    return this.resizingDesktop ? desktopSize(size.w, size.h) : size;
  }
  _requestRemoteResize() {
    const size = super._screenSize();
    if (size.w <= 0 || size.h <= 0) return;
    this.resizingDesktop = true;
    try { super._requestRemoteResize(); }
    finally { this.resizingDesktop = false; }
  }
}
