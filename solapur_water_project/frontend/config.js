// frontend/config.js
// Auto-detects environment.
// IMPORTANT: Replace 'dhara-api.onrender.com' with your actual Render backend URL.

const API_BASE = (
    window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1' ||
    window.location.hostname === ''
)
    ? 'http://localhost:8000'
    : 'https://hydro-equity-engine-smc.onrender.com';  // ← update this
