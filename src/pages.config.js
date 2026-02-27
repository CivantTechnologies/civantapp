import { lazy } from 'react';
import Home from './pages/Home';
import System from './pages/System';
import __Layout from './Layout.jsx';

const loadAlerts = () => import('./pages/Alerts');
const loadArchitecture = () => import('./pages/Architecture');
const loadCompetitors = () => import('./pages/Competitors');
const loadCompanyProfile = () => import('./pages/CompanyProfile');
const loadConnectors = () => import('./pages/Connectors');
const loadInsights = () => import('./pages/Insights');
const loadIntegrations = () => import('./pages/Integrations');
const loadPipelineAdmin = () => import('./pages/PipelineAdmin');
const loadForecast = () => import('./pages/Predictions');
const loadProfile = () => import('./pages/Profile');
const loadSearch = () => import('./pages/Search');
const loadTenderDetail = () => import('./pages/TenderDetail');
const loadReports = () => import('./pages/Reports');

const Alerts = lazy(loadAlerts);
const Architecture = lazy(loadArchitecture);
const Competitors = lazy(loadCompetitors);
const CompanyProfile = lazy(loadCompanyProfile);
const Connectors = lazy(loadConnectors);
const Insights = lazy(loadInsights);
const Integrations = lazy(loadIntegrations);
const PipelineAdmin = lazy(loadPipelineAdmin);
const Forecast = lazy(loadForecast);
const Profile = lazy(loadProfile);
const Search = lazy(loadSearch);
const TenderDetail = lazy(loadTenderDetail);
const Reports = lazy(loadReports);

const CORE_PREFETCH_LOADERS = [
  loadCompanyProfile,
  loadForecast,
  loadSearch,
  loadCompetitors,
  loadAlerts,
  loadInsights
];

export const PAGES = {
    "Alerts": Alerts,
    "Architecture": Architecture,
    "Competitors": Competitors,
    "CompanyProfile": CompanyProfile,
    "Connectors": Connectors,
    "Home": Home,
    "Insights": Insights,
    "Integrations": Integrations,
    "PipelineAdmin": PipelineAdmin,
    "Forecast": Forecast,
    "Profile": Profile,
    "Reports": Reports,
    "Search": Search,
    "System": System,
    "TenderDetail": TenderDetail,
}

export const pagesConfig = {
    mainPage: "Home",
    Pages: PAGES,
    Layout: __Layout,
};

export async function prefetchCorePages() {
    await Promise.allSettled(CORE_PREFETCH_LOADERS.map((loader) => loader()));
}
