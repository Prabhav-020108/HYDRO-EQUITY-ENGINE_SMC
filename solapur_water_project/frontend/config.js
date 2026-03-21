// frontend/config.js
// Auto-detects environment: localhost → dev server, anything else → Render production.
// Must be the FIRST <script> tag in every dashboard HTML file.
// No imports, no exports — plain browser JavaScript.

const API_BASE = (
    window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1' ||
    window.location.hostname === ''
)
    ? 'http://localhost:8000'
    : 'https://dhara-api.onrender.com';
