import React from 'react';
import { cn } from '@/lib/utils';

/** @typedef {import('react').HTMLAttributes<HTMLDivElement>} DivProps */

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const Page = React.forwardRef(function Page({ className, ...props }, ref) {
  return (
    <section
      ref={ref}
      className={cn('mx-auto w-full max-w-7xl space-y-6 px-4 py-6 md:px-6 md:py-8', className)}
      {...props}
    />
  );
});
Page.displayName = 'Page';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageHeader = React.forwardRef(function PageHeader({ className, ...props }, ref) {
  return (
    <header
      ref={ref}
      className={cn('flex flex-col gap-3 rounded-2xl border border-border/80 bg-card/60 p-5 md:p-6', className)}
      {...props}
    />
  );
});
PageHeader.displayName = 'PageHeader';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageTitle = React.forwardRef(function PageTitle({ className, ...props }, ref) {
  return <h1 ref={ref} className={cn('text-2xl font-bold tracking-tight text-card-foreground md:text-3xl', className)} {...props} />;
});
PageTitle.displayName = 'PageTitle';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageDescription = React.forwardRef(function PageDescription({ className, ...props }, ref) {
  return <p ref={ref} className={cn('text-sm text-muted-foreground md:text-base', className)} {...props} />;
});
PageDescription.displayName = 'PageDescription';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageBody = React.forwardRef(function PageBody({ className, ...props }, ref) {
  return <div ref={ref} className={cn('space-y-6', className)} {...props} />;
});
PageBody.displayName = 'PageBody';

export { Page, PageHeader, PageTitle, PageDescription, PageBody };
