import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuthStore, useCartStore } from '@/store';
import { Button } from '@/lib/ui/button';
import { LogOut, ShoppingCart, ArrowLeft, Home } from 'lucide-react';
import { motion } from 'framer-motion';

export const NavBar: React.FC = () => {
  const { user, isAuthenticated, logout } = useAuthStore();
  const { items } = useCartStore();
  const navigate = useNavigate();
  const location = useLocation();

  const cartTotal = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
  const isCartPage = location.pathname === '/cart';
  const isAdminPage = location.pathname === '/admin';

  const handleSignOut = () => {
    logout();
    navigate('/');
  };

  const handleCartClick = () => {
    navigate('/cart');
  };

  const handleBackToShopping = () => {
    navigate('/shopping');
  };

  const getPageTitle = () => {
    if (isCartPage) return 'Shopping Cart';
    if (isAdminPage) return 'Admin Dashboard';
    return 'Browse Products';
  };

  return (
    <nav className="sticky top-0 z-40 w-full border-b bg-white/80 backdrop-blur-md shadow-sm">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16 gap-4">
          {/* Left Section */}
          <div className="flex items-center gap-4">
            {/* Back Button (Cart Page) */}
            {isCartPage && (
              <Button
                variant="ghost"
                size="icon"
                onClick={handleBackToShopping}
                className="hover:bg-gray-100"
                title="Back to Shopping"
              >
                <ArrowLeft className="w-5 h-5" />
              </Button>
            )}

            {/* Logo/Brand */}
            <div 
              className="flex items-center cursor-pointer hover:opacity-80 transition-opacity" 
              onClick={() => isAdminPage ? navigate('/admin') : navigate('/shopping')}
            >
              <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
                ShopHub
              </h1>
            </div>

            {/* Page Title */}
            {isAuthenticated && (
              <span className="text-gray-600 text-sm hidden sm:inline-block">
                {getPageTitle()}
              </span>
            )}
          </div>

          {/* Right Section */}
          <div className="flex items-center gap-4">
            {isAuthenticated && user ? (
              <>
                {/* Cart Icon with Badge and Total (Shopping/Admin Pages) */}
                {!isCartPage && (
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={handleCartClick}
                    className="relative group"
                  >
                    <ShoppingCart className="w-5 h-5 text-slate-600 group-hover:text-slate-900 transition-colors" />
                    {items.length > 0 && (
                      <motion.span
                        initial={{ scale: 0 }}
                        animate={{ scale: 1 }}
                        className="absolute -top-2 -right-2 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center font-bold"
                      >
                        {items.length}
                      </motion.span>
                    )}
                  </motion.button>
                )}

                {/* Cart Total Display (Shopping/Admin Pages) */}
                {!isCartPage && items.length > 0 && (
                  <div className="hidden sm:flex items-center gap-2 px-3 py-1 bg-red-50 rounded-lg">
                    <span className="text-xs text-gray-600">Total:</span>
                    <span className="font-bold text-red-600">${cartTotal.toFixed(2)}</span>
                  </div>
                )}

                {/* User Info & Actions */}
                <div className="flex items-center gap-3 pl-3 border-l border-gray-200">
                  <div className="text-right hidden sm:block">
                    <p className="text-sm font-medium text-gray-900">{user.email}</p>
                    <p className="text-xs text-gray-500 capitalize">{user.role}</p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleSignOut}
                    className="flex items-center gap-2 hover:bg-red-50 hover:text-red-600 hover:border-red-300"
                  >
                    <LogOut className="w-4 h-4" />
                    <span className="hidden sm:inline">Sign Out</span>
                  </Button>
                </div>
              </>
            ) : (
              <Button
                onClick={() => navigate('/')}
                variant="outline"
                className="hover:bg-blue-50"
              >
                Sign In
              </Button>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
};

export default NavBar;
