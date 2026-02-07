import * as React from "react"

import { cn } from "@/lib/utils"

/** @typedef {import("react").HTMLAttributes<HTMLTableSectionElement>} SectionProps */
/** @typedef {import("react").TableHTMLAttributes<HTMLTableElement>} TableProps */
/** @typedef {import("react").HTMLAttributes<HTMLTableRowElement>} TableRowProps */
/** @typedef {import("react").ThHTMLAttributes<HTMLTableCellElement>} TableHeadProps */
/** @typedef {import("react").TdHTMLAttributes<HTMLTableCellElement>} TableCellProps */
/** @typedef {import("react").HTMLAttributes<HTMLTableCaptionElement>} TableCaptionProps */

/** @type {import("react").ForwardRefExoticComponent<TableProps & import("react").RefAttributes<HTMLTableElement>>} */
const Table = React.forwardRef(function Table({ className = "", ...props }, ref) {
  return (
    <div className="relative w-full overflow-auto">
      <table
        ref={ref}
        className={cn("w-full caption-bottom text-sm", className)}
        {...props} />
    </div>
  );
});
Table.displayName = "Table"

/** @type {import("react").ForwardRefExoticComponent<SectionProps & import("react").RefAttributes<HTMLTableSectionElement>>} */
const TableHeader = React.forwardRef(function TableHeader({ className = "", ...props }, ref) {
  return <thead ref={ref} className={cn("[&_tr]:border-b", className)} {...props} />;
});
TableHeader.displayName = "TableHeader"

/** @type {import("react").ForwardRefExoticComponent<SectionProps & import("react").RefAttributes<HTMLTableSectionElement>>} */
const TableBody = React.forwardRef(function TableBody({ className = "", ...props }, ref) {
  return (
    <tbody
      ref={ref}
      className={cn("[&_tr:last-child]:border-0", className)}
      {...props} />
  );
});
TableBody.displayName = "TableBody"

/** @type {import("react").ForwardRefExoticComponent<SectionProps & import("react").RefAttributes<HTMLTableSectionElement>>} */
const TableFooter = React.forwardRef(function TableFooter({ className = "", ...props }, ref) {
  return (
    <tfoot
      ref={ref}
      className={cn("border-t bg-muted/50 font-medium [&>tr]:last:border-b-0", className)}
      {...props} />
  );
});
TableFooter.displayName = "TableFooter"

/** @type {import("react").ForwardRefExoticComponent<TableRowProps & import("react").RefAttributes<HTMLTableRowElement>>} */
const TableRow = React.forwardRef(function TableRow({ className = "", ...props }, ref) {
  return (
    <tr
      ref={ref}
      className={cn(
        "border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted",
        className
      )}
      {...props} />
  );
});
TableRow.displayName = "TableRow"

/** @type {import("react").ForwardRefExoticComponent<TableHeadProps & import("react").RefAttributes<HTMLTableCellElement>>} */
const TableHead = React.forwardRef(function TableHead({ className = "", ...props }, ref) {
  return (
    <th
      ref={ref}
      className={cn(
        "h-10 px-2 text-left align-middle font-medium text-muted-foreground [&:has([role=checkbox])]:pr-0 [&>[role=checkbox]]:translate-y-[2px]",
        className
      )}
      {...props} />
  );
});
TableHead.displayName = "TableHead"

/** @type {import("react").ForwardRefExoticComponent<TableCellProps & import("react").RefAttributes<HTMLTableCellElement>>} */
const TableCell = React.forwardRef(function TableCell({ className = "", ...props }, ref) {
  return (
    <td
      ref={ref}
      className={cn(
        "p-2 align-middle [&:has([role=checkbox])]:pr-0 [&>[role=checkbox]]:translate-y-[2px]",
        className
      )}
      {...props} />
  );
});
TableCell.displayName = "TableCell"

/** @type {import("react").ForwardRefExoticComponent<TableCaptionProps & import("react").RefAttributes<HTMLTableCaptionElement>>} */
const TableCaption = React.forwardRef(function TableCaption({ className = "", ...props }, ref) {
  return (
    <caption
      ref={ref}
      className={cn("mt-4 text-sm text-muted-foreground", className)}
      {...props} />
  );
});
TableCaption.displayName = "TableCaption"

export {
  Table,
  TableHeader,
  TableBody,
  TableFooter,
  TableHead,
  TableRow,
  TableCell,
  TableCaption,
}
