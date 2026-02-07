import * as React from "react"
import * as RadioGroupPrimitive from "@radix-ui/react-radio-group"
import { Circle } from "lucide-react"

import { cn } from "@/lib/utils"

/** @typedef {import("react").ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Root>} RadioGroupProps */
/** @typedef {import("react").ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Item>} RadioGroupItemProps */

/** @type {import("react").ForwardRefExoticComponent<RadioGroupProps & import("react").RefAttributes<any>>} */
const RadioGroup = React.forwardRef(function RadioGroup({ className = "", ...props }, ref) {
  return <RadioGroupPrimitive.Root className={cn("grid gap-2", className)} {...props} ref={ref} />;
});
RadioGroup.displayName = RadioGroupPrimitive.Root.displayName

/** @type {import("react").ForwardRefExoticComponent<RadioGroupItemProps & import("react").RefAttributes<any>>} */
const RadioGroupItem = React.forwardRef(function RadioGroupItem({ className = "", ...props }, ref) {
  return (
    <RadioGroupPrimitive.Item
      ref={ref}
      className={cn(
        "aspect-square h-4 w-4 rounded-full border border-primary text-primary shadow focus:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}>
      <RadioGroupPrimitive.Indicator className="flex items-center justify-center">
        <Circle className="h-3.5 w-3.5 fill-primary" />
      </RadioGroupPrimitive.Indicator>
    </RadioGroupPrimitive.Item>
  );
});
RadioGroupItem.displayName = RadioGroupPrimitive.Item.displayName

export { RadioGroup, RadioGroupItem }
