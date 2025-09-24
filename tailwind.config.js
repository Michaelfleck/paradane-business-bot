/** Tailwind for PDF HTML templates (Playwright).
 * Scans HTML templates and Python format strings used to inject classes.
 */
module.exports = {
  content: [
    "./project/template/*.html",
    "./project/template/**/*.html",
    "./project/**/*.py"
  ],
  theme: {
    extend: {}
  },
  safelist: [
    // Explicit frequently-used utilities
    "font-semibold",
    "font-bold",
    "italic",
    "underline",
    "line-through",
    "text-left",
    "text-center",
    "text-right",
    "truncate",
    // Color and spacing patterns commonly used across reports
    { pattern: /^(text|bg|border)-(slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-(50|100|200|300|400|500|600|700|800|900)$/ },
    { pattern: /^(p|px|py|pt|pr|pb|pl|m|mx|my|mt|mr|mb|ml)-(0|1|2|3|4|5|6|8|10|12|16|20|24|32)$/ },
    { pattern: /^grid-cols-(1|2|3|4|5|6|12)$/ },
    { pattern: /^(justify|items)-(start|center|end|between|around)$/ }
  ]
};