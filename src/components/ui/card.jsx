import * as React from "react"

import { cn } from "@/lib/utils"

/** @typedef {import("react").HTMLAttributes<HTMLDivElement>} DivProps */

/** @type {import("react").ForwardRefExoticComponent<DivProps & import("react").RefAttributes<HTMLDivElement>>} */
const Card = React.forwardRef(function Card({ className, ...props }, ref) {
  return (
    <div
      ref={ref}
      className={cn("rounded-xl border bg-card text-card-foreground shadow", className)}
      {...props} />
  );
});
Card.displayName = "Card"

/** @type {import("react").ForwardRefExoticComponent<DivProps & import("react").RefAttributes<HTMLDivElement>>} */
const CardHeader = React.forwardRef(function CardHeader({ className, ...props }, ref) {
  return (
    <div
      ref={ref}
      className={cn("flex flex-col space-y-1.5 p-6", className)}
      {...props} />
  );
});
CardHeader.displayName = "CardHeader"

/** @type {import("react").ForwardRefExoticComponent<DivProps & import("react").RefAttributes<HTMLDivElement>>} */
const CardTitle = React.forwardRef(function CardTitle({ className, ...props }, ref) {
  return (
    <div
      ref={ref}
      className={cn("font-semibold leading-none tracking-tight", className)}
      {...props} />
  );
});
CardTitle.displayName = "CardTitle"

/** @type {import("react").ForwardRefExoticComponent<DivProps & import("react").RefAttributes<HTMLDivElement>>} */
const CardDescription = React.forwardRef(function CardDescription({ className, ...props }, ref) {
  return (
    <div
      ref={ref}
      className={cn("text-sm text-muted-foreground", className)}
      {...props} />
  );
});
CardDescription.displayName = "CardDescription"

/** @type {import("react").ForwardRefExoticComponent<DivProps & import("react").RefAttributes<HTMLDivElement>>} */
const CardContent = React.forwardRef(function CardContent({ className, ...props }, ref) {
  return <div ref={ref} className={cn("p-6 pt-0", className)} {...props} />;
});
CardContent.displayName = "CardContent"

/** @type {import("react").ForwardRefExoticComponent<DivProps & import("react").RefAttributes<HTMLDivElement>>} */
const CardFooter = React.forwardRef(function CardFooter({ className, ...props }, ref) {
  return (
    <div
      ref={ref}
      className={cn("flex items-center p-6 pt-0", className)}
      {...props} />
  );
});
CardFooter.displayName = "CardFooter"

export { Card, CardHeader, CardFooter, CardTitle, CardDescription, CardContent }
