import { lazy } from 'react';
import Home from './pages/Home';
import System from './pages/System';
import __Layout from './Layout.jsx';

const Alerts = lazy(() => import('./pages/Alerts'));
const Architecture = lazy(() => import('./pages/Architecture'));
const Competitors = lazy(() => import('./pages/Competitors'));
const Connectors = lazy(() => import('./pages/Connectors'));
const Insights = lazy(() => import('./pages/Insights'));
const Integrations = lazy(() => import('./pages/Integrations'));
const PipelineAdmin = lazy(() => import('./pages/PipelineAdmin'));
const Forecast = lazy(() => import('./pages/Predictions'));
const Profile = lazy(() => import('./pages/Profile'));
const Search = lazy(() => import('./pages/Search'));
const TenderDetail = lazy(() => import('./pages/TenderDetail'));

export const PAGES = {
    "Alerts": Alerts,
    "Architecture": Architecture,
    "Competitors": Competitors,
    "Connectors": Connectors,
    "Home": Home,
    "Insights": Insights,
    "Integrations": Integrations,
    "PipelineAdmin": PipelineAdmin,
    "Forecast": Forecast,
    "Profile": Profile,
    "Search": Search,
    "System": System,
    "TenderDetail": TenderDetail,
}

export const pagesConfig = {
    mainPage: "Home",
    Pages: PAGES,
    Layout: __Layout,
};
