/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: "#8B50D4",
        "primary-soft": "#B084F7",
        background: "#FFFFFF",
        surface: "#F8F8FB",
        "text-primary": "#0A0A0A",
        "text-secondary": "#6B6B6B",
        border: "#E6E6E6",
      },
      spacing: {
        0.5: "4px",
        1: "8px",
        2: "16px",
        3: "24px",
        4: "32px",
        6: "48px",
        8: "64px",
      },
      borderRadius: {
        sm: "8px",
        md: "12px",
        lg: "16px",
        xl: "20px",
      },
      boxShadow: {
        soft: "0 10px 30px rgba(0,0,0,0.05)",
        card: "0 1px 3px rgba(0,0,0,0.05)",
      },
      fontFamily: {
        sans: ["var(--font-geist-sans)", "Geist", "ui-sans-serif", "system-ui", "sans-serif"],
        display: ["var(--font-geist-sans)", "Geist", "ui-sans-serif", "system-ui", "sans-serif"],
        mono: ["var(--font-geist-mono)", "Geist Mono", "ui-monospace", "SFMono-Regular", "monospace"],
      },
    },
  },
};
