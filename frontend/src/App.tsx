import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { useStore } from '@/store';
import NavBar from '@/components/NavBar';
import Landing from '@/pages/Landing';
import ShoppingPage from '@/pages/Shopping';
import Cart from './pages/Cart';
import Orders from './pages/Orders';
import AdminDashboard from '@/pages/AdminDashboard';
import './index.css';

const ProtectedRoute: React.FC<{ children: React.ReactNode; requireAdmin?: boolean }> = ({
  children,
  requireAdmin = false,
}) => {
  const { isAuthenticated, user } = useStore();

  if (!isAuthenticated) {
    return <Navigate to="/" />;
  }

  if (requireAdmin && !user?.is_admin) {
    return <Navigate to="/shopping" />;
  }

  return <>{children}</>;
};

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Landing />} />
        <Route
          path="/shopping"
          element={
            <ProtectedRoute>
              <div className="flex flex-col min-h-screen">
                <NavBar />
                <div className="flex-1">
                  <ShoppingPage />
                </div>
              </div>
            </ProtectedRoute>
          }
        />
        <Route
          path="/cart"
          element={
            <ProtectedRoute>
              <div className="flex flex-col min-h-screen">
                <NavBar />
                <div className="flex-1">
                  <Cart />
                </div>
              </div>
            </ProtectedRoute>
          }
        />
        <Route
          path="/orders"
          element={
            <ProtectedRoute>
              <div className="flex flex-col min-h-screen">
                <NavBar />
                <div className="flex-1">
                  <Orders />
                </div>
              </div>
            </ProtectedRoute>
          }
        />
        <Route path="/dashboard" element={<Navigate to="/shopping" />} />
        <Route
          path="/admin"
          element={
            <ProtectedRoute requireAdmin>
              <div className="flex flex-col min-h-screen">
                <NavBar />
                <div className="flex-1">
                  <AdminDashboard />
                </div>
              </div>
            </ProtectedRoute>
          }
        />
        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </Router>
  );
}

export default App;