"use client";

import * as React from "react";

import { Button, type ButtonProps } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export type ShineHoverButtonProps = Omit<ButtonProps, "variant">;

const shineHoverClasses =
  "relative overflow-hidden rounded-lg border border-transparent bg-gradient-to-r from-[#8B50D4] to-[#A873F3] px-6 py-3 text-white shadow-sm transition-all duration-200 hover:-translate-y-[1px] hover:shadow-md hover:brightness-[0.98] before:absolute before:inset-0 before:rounded-[inherit] before:bg-[linear-gradient(45deg,transparent_25%,rgba(255,255,255,0.72)_50%,transparent_75%,transparent_100%)] before:bg-[length:250%_250%,100%_100%] before:bg-[position:200%_0,0_0] before:bg-no-repeat before:transition-[background-position_0s_ease] before:duration-1000 hover:before:bg-[position:-100%_0,0_0]";

const ShineHoverButton = React.forwardRef<HTMLButtonElement, ShineHoverButtonProps>(
  ({ className, asChild = false, children, ...props }, ref) => {
    return (
      <Button
        ref={ref}
        asChild={asChild}
        {...props}
        className={cn(shineHoverClasses, className)}
      >
        {children}
      </Button>
    );
  }
);

ShineHoverButton.displayName = "ShineHoverButton";

export default ShineHoverButton;
