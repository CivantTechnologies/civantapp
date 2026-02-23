import React from 'react';
import { cn } from '@/lib/utils';

/** @typedef {import('react').HTMLAttributes<HTMLDivElement>} DivProps */

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const Page = React.forwardRef(function Page({ className, ...props }, ref) {
  return (
    <section
      ref={ref}
      className={cn('mx-auto w-full max-w-7xl space-y-8 px-6 py-8 md:px-8 md:py-10', className)}
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
      className={cn('flex flex-col gap-4', className)}
      {...props}
    />
  );
});
PageHeader.displayName = 'PageHeader';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageHero = React.forwardRef(function PageHero({ className, ...props }, ref) {
  return (
    <header
      ref={ref}
      className={cn(
        'civant-hero flex min-h-[60vh] flex-col justify-center gap-5 py-16 md:min-h-[65vh] md:py-20',
        className
      )}
      {...props}
    />
  );
});
PageHero.displayName = 'PageHero';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageHeroActions = React.forwardRef(function PageHeroActions({ className, ...props }, ref) {
  return (
    <div
      ref={ref}
      className={cn('flex flex-wrap items-center gap-3', className)}
      {...props}
    />
  );
});
PageHeroActions.displayName = 'PageHeroActions';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageTitle = React.forwardRef(function PageTitle({ className, ...props }, ref) {
  return <h1 ref={ref} className={cn('text-4xl font-semibold tracking-tight text-card-foreground md:text-5xl', className)} {...props} />;
});
PageTitle.displayName = 'PageTitle';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageDescription = React.forwardRef(function PageDescription({ className, ...props }, ref) {
  return <p ref={ref} className={cn('max-w-3xl text-base text-muted-foreground md:text-lg', className)} {...props} />;
});
PageDescription.displayName = 'PageDescription';

/** @type {import('react').ForwardRefExoticComponent<DivProps & import('react').RefAttributes<HTMLDivElement>>} */
const PageBody = React.forwardRef(function PageBody({ className, ...props }, ref) {
  return <div ref={ref} className={cn('space-y-6', className)} {...props} />;
});
PageBody.displayName = 'PageBody';

export { Page, PageHeader, PageHero, PageHeroActions, PageTitle, PageDescription, PageBody };
