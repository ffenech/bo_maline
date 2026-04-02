import { BrowserRouter, Routes, Route, Link, useLocation, Navigate, Outlet } from 'react-router-dom'
import { Database, Menu, X, Building2, FileText, LogOut, Receipt, DollarSign, Filter, ClipboardCheck } from 'lucide-react'
import { useState, lazy, Suspense } from 'react'
import { useAuth } from './contexts/AuthContext'
import logoMaline from './assets/logo.avif'

// Lazy load des pages
const MCDPage = lazy(() => import('./pages/MCDPage'))
const AgenciesPage = lazy(() => import('./pages/AgenciesPage'))
const LeadsPage = lazy(() => import('./pages/LeadsPage'))
const LeadsV1Page = lazy(() => import('./pages/LeadsV1Page'))

const StatsAgencesPage = lazy(() => import('./pages/StatsAgencesPage'))
const FacturesPage = lazy(() => import('./pages/FacturesPage'))
const GestionPubPage = lazy(() => import('./pages/GestionPubPage'))
const LoginPage = lazy(() => import('./pages/LoginPage'))
const ForgotPasswordPage = lazy(() => import('./pages/ForgotPasswordPage'))
const ResetPasswordPage = lazy(() => import('./pages/ResetPasswordPage'))
const FunnelPage = lazy(() => import('./pages/FunnelPage'))
const ControleFacturationPage = lazy(() => import('./pages/ControleFacturationPage'))

function Sidebar({ onCollapseChange }: { onCollapseChange: (collapsed: boolean) => void }) {
  const location = useLocation()
  const [isCollapsed, setIsCollapsed] = useState(true)
  const [showUserMenu, setShowUserMenu] = useState(false)
  const { signOut } = useAuth()

  const handleToggleCollapse = () => {
    const newState = !isCollapsed
    setIsCollapsed(newState)
    onCollapseChange(newState)
  }

  const menuItems = [
    { path: '/gestion-pub', label: 'Gestion Pub', icon: DollarSign, color: 'text-yellow-600' },
    { path: '/leads', label: 'Stat esti V1 et V2', icon: FileText, color: 'text-blue-600' },
    { path: '/agencies', label: 'Agences', icon: Building2, color: 'text-orange-600' },

    { path: '/agency-stats', label: 'Stats agences', icon: FileText, color: 'text-rose-600' },
    { path: '/funnel', label: 'Tunnel conversion', icon: Filter, color: 'text-teal-600' },
    { path: '/factures', label: 'Factures', icon: Receipt, color: 'text-emerald-600' },
    { path: '/controle-facturation', label: 'Controle Facturation', icon: ClipboardCheck, color: 'text-indigo-600' },
    { path: '/mcd', label: 'Schemas BDD', icon: Database, color: 'text-purple-600' },
  ]

  return (
    <aside className={`fixed left-0 top-0 h-screen bg-white border-r border-gray-200 transition-all duration-300 ${isCollapsed ? 'w-20' : 'w-72'} flex flex-col shadow-lg z-20`}>
      {/* Logo / Header */}
      <div className={`p-4 flex items-center border-b border-gray-200 ${isCollapsed ? 'justify-center' : 'justify-between'}`}>
        {!isCollapsed ? (
          <>
            <div className="flex items-center">
              <img src={logoMaline} alt="Maline" className="h-10 w-auto" />
            </div>
            <button
              onClick={handleToggleCollapse}
              className="p-2 hover:bg-gray-100 rounded-lg transition-all text-gray-500 hover:text-gray-700"
            >
              <X className="w-5 h-5" />
            </button>
          </>
        ) : (
          <button
            onClick={handleToggleCollapse}
            className="p-2 hover:bg-gray-100 rounded-lg transition-all text-gray-500 hover:text-gray-700"
          >
            <Menu className="w-5 h-5" />
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-4 py-2 space-y-2">
        {menuItems.map((item) => {
          const Icon = item.icon
          const isActive = location.pathname === item.path

          return (
            <div key={item.path} className="relative group/tooltip">
              <Link
                to={item.path}
                className={`flex items-center px-4 py-3 rounded-xl transition-all duration-200 group ${isCollapsed ? 'justify-center' : ''} ${
                  isActive
                    ? 'bg-gradient-to-r from-blue-600 to-cyan-600 text-white shadow-md'
                    : 'text-gray-700 hover:bg-gray-100'
                }`}
              >
                <Icon className={`w-5 h-5 flex-shrink-0 ${isActive ? 'text-white' : item.color} ${isActive ? '' : 'group-hover:scale-110'} transition-all`} />
                {!isCollapsed && <span className="ml-3 text-sm font-medium">{item.label}</span>}
                {isActive && !isCollapsed && (
                  <div className="ml-auto w-2 h-2 bg-white rounded-full animate-pulse"></div>
                )}
              </Link>
              {isCollapsed && (
                <div className="absolute left-full top-1/2 -translate-y-1/2 ml-2 px-2.5 py-1.5 bg-gray-900 text-white text-xs font-medium rounded-md whitespace-nowrap opacity-0 group-hover/tooltip:opacity-100 pointer-events-none transition-opacity duration-150 z-50">
                  {item.label}
                </div>
              )}
            </div>
          )
        })}
      </nav>

      {/* User Section */}
      <div className="p-4 border-t border-gray-200 relative">
        <div
          className={`flex items-center ${isCollapsed ? 'justify-center' : ''} px-3 py-3 rounded-xl hover:bg-gray-100 transition-colors cursor-pointer group`}
          onClick={(e) => {
            e.stopPropagation()
            setShowUserMenu(!showUserMenu)
          }}
        >
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-600 to-cyan-600 flex items-center justify-center text-sm font-bold text-white flex-shrink-0 shadow-md group-hover:scale-105 transition-transform">
            M
          </div>
          {!isCollapsed && (
            <div className="ml-3">
              <p className="text-sm font-semibold text-gray-900">Maline</p>
              <p className="text-xs text-gray-500">Administrateur</p>
            </div>
          )}
        </div>

        {/* Menu utilisateur */}
        {showUserMenu && !isCollapsed && (
          <>
            {/* Backdrop pour fermer le menu */}
            <div
              className="fixed inset-0 z-40"
              onClick={() => setShowUserMenu(false)}
            />
            <div className="absolute bottom-full left-4 right-4 mb-2 bg-white rounded-lg shadow-xl border border-gray-200 py-2 z-50">
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  setShowUserMenu(false)
                  void signOut()
                }}
                className="w-full px-4 py-2 text-left text-sm text-gray-700 hover:bg-gray-100 flex items-center gap-2 transition-colors"
              >
                <LogOut className="w-4 h-4" />
                Se déconnecter
              </button>
            </div>
          </>
        )}
      </div>
    </aside>
  )
}

// Composant de chargement
function PageLoader() {
  return (
    <div className="min-h-screen grid place-items-center bg-gray-50">
      <div className="text-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
        <p className="text-gray-600 mt-4">Chargement...</p>
      </div>
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Suspense fallback={<PageLoader />}>
        <Routes>
          {/* Public */}
          <Route path="/login" element={<LoginPage />} />
          <Route path="/forgot-password" element={<ForgotPasswordPage />} />
          <Route path="/reset-password" element={<ResetPasswordPage />} />

          {/* Private */}
          <Route element={<RequireAuth><PrivateLayout /></RequireAuth>}>
            <Route index element={<GestionPubPage />} />
            <Route path="/mcd" element={<MCDPage />} />
            <Route path="/agencies" element={<AgenciesPage />} />
            <Route path="/leads" element={<LeadsPage />} />
            <Route path="/leads-v1" element={<LeadsV1Page />} />

            <Route path="/agency-stats" element={<StatsAgencesPage />} />
            <Route path="/gestion-pub" element={<GestionPubPage />} />
            <Route path="/funnel" element={<FunnelPage />} />
            <Route path="/factures" element={<FacturesPage />} />
            <Route path="/controle-facturation" element={<ControleFacturationPage />} />
          </Route>
        </Routes>
      </Suspense>
    </BrowserRouter>
  )
}

export default App

function RequireAuth({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth()
  const location = useLocation()

  if (loading) {
    return (
      <div className="min-h-screen grid place-items-center bg-gray-50">
        <div className="text-gray-600">Chargement…</div>
      </div>
    )
  }

  if (!user) {
    return <Navigate to="/login" state={{ from: location }} replace />
  }

  return <>{children}</>
}

function PrivateLayout() {
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(true)
  const location = useLocation()

  // Définir le titre et sous-titre en fonction de la page
  const getPageTitle = () => {
    switch (location.pathname) {
      case '/mcd':
        return { title: 'Schémas BDD', subtitle: 'Visualisation et exploration de la base de données' }
      case '/agencies':
        return { title: 'Liste des clients', subtitle: 'Gestion et suivi des clients' }
      case '/leads':
        return { title: 'Statistiques Estimateur V1 et V2', subtitle: 'Suivi des leads et taux de conversion' }

      case '/agency-stats':
        return { title: 'Statistiques agences', subtitle: 'Actifs, résiliations et churn par mois (BDD V2)' }
      case '/gestion-pub':
        return { title: 'Gestion Pub', subtitle: 'Suivi des budgets publicitaires et CPL par client' }
      case '/funnel':
        return { title: 'Tunnel de conversion', subtitle: 'Analyse du parcours utilisateur sur les estimateurs FR et ES' }
      case '/factures':
        return { title: 'Factures', subtitle: 'Gestion et téléchargement des factures prestataires' }
      case '/controle-facturation':
        return { title: 'Controle Facturation', subtitle: 'Verification croisee factures Zoho / campagnes Meta & Google Ads' }
      default:
        return { title: 'Tableau de bord', subtitle: 'Analyse et suivi des KPI' }
    }
  }

  const { title, subtitle } = getPageTitle()

  return (
    <div className="min-h-screen bg-gray-100 flex overflow-x-hidden">
      {/* Sidebar */}
      <Sidebar onCollapseChange={setIsSidebarCollapsed} />

      {/* Main Content */}
      <main className={`flex-1 transition-all duration-300 ${isSidebarCollapsed ? 'ml-20' : 'ml-72'} overflow-x-hidden`}>
        {/* Top Bar */}
        <header className="bg-gradient-to-r from-blue-600 to-cyan-600 shadow-lg sticky top-0 z-10">
          <div className="px-8 py-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-2xl font-bold text-white mb-1">{title}</h2>
                <p className="text-sm text-blue-100">{subtitle}</p>
              </div>
            </div>
          </div>
        </header>

        {/* Page Content */}
        <div className="p-6 max-w-full overflow-x-hidden">
          <Outlet />
        </div>
      </main>
    </div>
  )
}
