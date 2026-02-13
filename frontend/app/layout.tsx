import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: {
    default: "Grablin | AI-First Acquisition Workspace",
    template: "%s | Grablin",
  },
  description:
    "Grablin combines WKO, Project Facts, Open Firmenbuch, and EVI data to help teams qualify companies faster and run better acquisition workflows.",
  icons: {
    icon: [{ url: "/icon.png", type: "image/png" }],
    shortcut: ["/icon.png"],
    apple: [{ url: "/icon.png" }],
  },
  openGraph: {
    title: "Grablin | AI-First Acquisition Workspace",
    description:
      "Research companies, validate contact history, and review financial data in one acquisition workflow.",
    images: ["/icon.png"],
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: "Grablin | AI-First Acquisition Workspace",
    description:
      "Research companies, validate contact history, and review financial data in one acquisition workflow.",
    images: ["/icon.png"],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
        suppressHydrationWarning
      >
        {children}
      </body>
    </html>
  );
}
