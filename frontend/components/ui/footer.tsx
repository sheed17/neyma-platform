import * as React from "react";
import Link from "next/link";

import { Button } from "@/components/ui/button";

interface FooterProps {
  logo: React.ReactNode;
  brandName: string;
  socialLinks: Array<{
    icon: React.ReactNode;
    href: string;
    label: string;
  }>;
  mainLinks: Array<{
    href: string;
    label: string;
  }>;
  legalLinks: Array<{
    href: string;
    label: string;
  }>;
  copyright: {
    text: string;
    license?: string;
  };
  theme?: "light" | "dark";
  className?: string;
}

export function Footer({
  logo,
  brandName,
  socialLinks,
  mainLinks,
  legalLinks,
  copyright,
  theme = "light",
  className,
}: FooterProps) {
  const isDark = theme === "dark";

  return (
    <footer className={className ?? "pb-6 pt-16 lg:pb-8 lg:pt-24"}>
      <div className="px-4 lg:px-8">
        <div className="md:flex md:items-start md:justify-between">
          <Link href="/" className="flex items-center gap-x-2" aria-label={brandName}>
            {logo ? <>{logo}</> : null}
            <span className={`text-xl font-bold ${isDark ? "text-white" : "text-black"}`}>{brandName}</span>
          </Link>
          <ul className="mt-6 flex list-none space-x-3 md:mt-0">
            {socialLinks.map((link, i) => (
              <li key={i}>
                <Button
                  variant="secondary"
                  size="icon"
                  className={
                    isDark
                      ? "h-10 w-10 rounded-full border border-white/10 bg-white/5 text-white hover:bg-white/10"
                      : "h-10 w-10 rounded-full"
                  }
                  asChild
                >
                  <a href={link.href} target="_blank" rel="noreferrer" aria-label={link.label}>
                    {link.icon}
                  </a>
                </Button>
              </li>
            ))}
          </ul>
        </div>
        <div className={`mt-6 pt-6 md:mt-4 md:pt-8 lg:grid lg:grid-cols-10 ${isDark ? "border-t border-white/10" : "border-t"}`}>
          <nav className="lg:col-[4/11] lg:mt-0">
            <ul className="-mx-2 -my-1 flex list-none flex-wrap lg:justify-end">
              {mainLinks.map((link, i) => (
                <li key={i} className="mx-2 my-1 shrink-0">
                  <Link
                    href={link.href}
                    className={isDark ? "text-sm text-white underline-offset-4 hover:underline" : "text-sm text-primary underline-offset-4 hover:underline"}
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
          <div className="mt-6 lg:col-[4/11] lg:mt-0">
            <ul className="-mx-3 -my-1 flex list-none flex-wrap lg:justify-end">
              {legalLinks.map((link, i) => (
                <li key={i} className="mx-3 my-1 shrink-0">
                  <Link
                    href={link.href}
                    className={isDark ? "text-sm text-white/65 underline-offset-4 hover:text-white hover:underline" : "text-sm text-muted-foreground underline-offset-4 hover:underline"}
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
          <div className={`mt-6 whitespace-nowrap text-sm leading-6 lg:col-[1/4] lg:row-[1/3] lg:mt-0 ${isDark ? "text-white/65" : "text-muted-foreground"}`}>
            <div>{copyright.text}</div>
            {copyright.license ? <div>{copyright.license}</div> : null}
          </div>
        </div>
      </div>
    </footer>
  );
}
