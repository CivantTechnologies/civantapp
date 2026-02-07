import * as React from "react"
import { cva } from "class-variance-authority";

import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-ring/60",
  {
    variants: {
      variant: {
        primary:
          "border-transparent bg-primary/20 text-primary",
        secondary:
          "border-border bg-secondary text-secondary-foreground",
        ghost:
          "border-border/70 bg-transparent text-muted-foreground",
        default:
          "border-transparent bg-primary/20 text-primary",
        destructive:
          "border-transparent bg-destructive/20 text-destructive",
        outline: "border-border text-foreground",
      },
    },
    defaultVariants: {
      variant: "primary",
    },
  }
)

function Badge({
  className = "",
  variant = undefined,
  ...props
}) {
  return (<div className={cn(badgeVariants({ variant: /** @type {any} */ (variant) }), className)} {...props} />);
}

export { Badge, badgeVariants }
