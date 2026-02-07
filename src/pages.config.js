import Alerts from './pages/Alerts';
import Architecture from './pages/Architecture';
import Competitors from './pages/Competitors';
import Connectors from './pages/Connectors';
import Home from './pages/Home';
import Insights from './pages/Insights';
import Integrations from './pages/Integrations';
import PipelineAdmin from './pages/PipelineAdmin';
import Predictions from './pages/Predictions';
import Search from './pages/Search';
import System from './pages/System';
import TenderDetail from './pages/TenderDetail';
import __Layout from './Layout.jsx';


export const PAGES = {
    "Alerts": Alerts,
    "Architecture": Architecture,
    "Competitors": Competitors,
    "Connectors": Connectors,
    "Home": Home,
    "Insights": Insights,
    "Integrations": Integrations,
    "PipelineAdmin": PipelineAdmin,
    "Predictions": Predictions,
    "Search": Search,
    "System": System,
    "TenderDetail": TenderDetail,
}

export const pagesConfig = {
    mainPage: "Home",
    Pages: PAGES,
    Layout: __Layout,
};
