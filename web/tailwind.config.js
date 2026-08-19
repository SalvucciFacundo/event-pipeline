/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['"JetBrains Mono"', '"IBM Plex Mono"', 'ui-monospace', 'monospace'],
      },
      colors: {
        surface: '#0B0C0A',
        panel: '#11130F',
        line: '#2A2D26',
        signal: '#00E676',
        alert: '#FF3B30',
        amber: '#FFB800',
        text: '#E6E6E0',
        dim: '#8A8F85',
      },
    },
  },
  plugins: [],
}
