import * as React from "react"
import * as TabsPrimitive from "@radix-ui/react-tabs"

import { cn } from "@/lib/utils"
/** @typedef {import("react").ComponentPropsWithoutRef<typeof TabsPrimitive.List>} TabsListProps */
/** @typedef {import("react").ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger>} TabsTriggerProps */
/** @typedef {import("react").ComponentPropsWithoutRef<typeof TabsPrimitive.Content>} TabsContentProps */

const Tabs = TabsPrimitive.Root

/** @type {import("react").ForwardRefExoticComponent<TabsListProps & import("react").RefAttributes<any>>} */
const TabsList = React.forwardRef(function TabsList({ className = "", ...props }, ref) {
  return (
    <TabsPrimitive.List
      ref={ref}
      className={cn(
        "inline-flex h-9 items-center justify-center rounded-lg bg-muted p-1 text-muted-foreground",
        className
      )}
      {...props} />
  );
});
TabsList.displayName = TabsPrimitive.List.displayName

/** @type {import("react").ForwardRefExoticComponent<TabsTriggerProps & import("react").RefAttributes<any>>} */
const TabsTrigger = React.forwardRef(function TabsTrigger({ className = "", ...props }, ref) {
  return (
    <TabsPrimitive.Trigger
      ref={ref}
      className={cn(
        "inline-flex items-center justify-center whitespace-nowrap rounded-md px-3 py-1 text-sm font-medium ring-offset-background transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 data-[state=active]:bg-background data-[state=active]:text-foreground data-[state=active]:shadow",
        className
      )}
      {...props} />
  );
});
TabsTrigger.displayName = TabsPrimitive.Trigger.displayName

/** @type {import("react").ForwardRefExoticComponent<TabsContentProps & import("react").RefAttributes<any>>} */
const TabsContent = React.forwardRef(function TabsContent({ className = "", ...props }, ref) {
  return (
    <TabsPrimitive.Content
      ref={ref}
      className={cn(
        "mt-2 ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
        className
      )}
      {...props} />
  );
});
TabsContent.displayName = TabsPrimitive.Content.displayName

export { Tabs, TabsList, TabsTrigger, TabsContent }
