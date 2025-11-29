import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { useCartStore } from '@/store';
import { Button } from '@/lib/ui/button';
import { Input } from '@/lib/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui/card';
import { ShoppingCart, Trash2, Loader2, X } from 'lucide-react';

export const CartPage: React.FC = () => {
  const navigate = useNavigate();
  const { items, removeFromCart, updateQuantity, fetchCart } = useCartStore();
  const [loading, setLoading] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);

  useEffect(() => {
    fetchCart();
  }, []);

  const cartTotal = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
  const taxRate = 0.08;
  const tax = cartTotal * taxRate;
  const finalTotal = cartTotal + tax;

  const handleRemove = async (productId: number) => {
    setRemovingId(productId);
    try {
      await removeFromCart(productId);
    } catch (error) {
      console.error('Failed to remove item:', error);
    } finally {
      setRemovingId(null);
    }
  };

  const handleQuantityChange = async (productId: number, quantity: number) => {
    if (quantity < 1) {
      await handleRemove(productId);
      return;
    }
    try {
      await updateQuantity(productId, quantity);
    } catch (error) {
      console.error('Failed to update quantity:', error);
    }
  };

  const handleCheckout = async () => {
    setLoading(true);
    try {
      // Simulate checkout
      await new Promise(resolve => setTimeout(resolve, 2000));
      alert('Order placed successfully!');
      navigate('/shopping');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-12">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Cart Items */}
          <div className="lg:col-span-2">
            {items.length === 0 ? (
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="text-center py-20"
              >
                <ShoppingCart className="mx-auto mb-4 text-slate-400" size={48} />
                <p className="text-slate-600 text-lg mb-6">Your cart is empty</p>
                <Button
                  onClick={() => navigate('/shopping')}
                  className="bg-red-500 hover:bg-red-600"
                >
                  Continue Shopping
                </Button>
              </motion.div>
            ) : (
              <div className="space-y-4">
                <AnimatePresence>
                  {items.map((item) => (
                    <motion.div
                      key={item.id}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      exit={{ opacity: 0, x: 20 }}
                      layout
                    >
                      <Card className="overflow-hidden">
                        <CardContent className="p-6 flex gap-6 items-start">
                          {/* Product Image */}
                          <div className="w-24 h-24 bg-gradient-to-br from-red-100 to-red-50 rounded-lg flex items-center justify-center flex-shrink-0">
                            <div className="text-red-400 text-3xl font-bold">
                              {item.name.charAt(0)}
                            </div>
                          </div>

                          {/* Product Details */}
                          <div className="flex-1">
                            <h3 className="font-semibold text-lg text-slate-900">
                              {item.name}
                            </h3>
                            <p className="text-slate-600 text-sm mb-2">
                              {item.description}
                            </p>
                            <p className="font-bold text-red-500">
                              ${item.price.toFixed(2)}
                            </p>
                          </div>

                          {/* Quantity Control */}
                          <div className="flex items-center gap-4">
                            <div className="flex items-center gap-2 bg-slate-100 rounded-lg p-1">
                              <button
                                onClick={() => handleQuantityChange(item.id, item.quantity - 1)}
                                className="px-2 py-1 hover:bg-slate-200 rounded transition-colors"
                              >
                                −
                              </button>
                              <span className="w-6 text-center font-semibold">
                                {item.quantity}
                              </span>
                              <button
                                onClick={() => handleQuantityChange(item.id, item.quantity + 1)}
                                className="px-2 py-1 hover:bg-slate-200 rounded transition-colors"
                              >
                                +
                              </button>
                            </div>

                            {/* Remove Button */}
                            <motion.button
                              whileHover={{ scale: 1.1 }}
                              whileTap={{ scale: 0.9 }}
                              onClick={() => handleRemove(item.id)}
                              disabled={removingId === item.id}
                              className="text-red-500 hover:text-red-700 p-2 hover:bg-red-50 rounded-lg transition-colors disabled:opacity-50"
                            >
                              {removingId === item.id ? (
                                <Loader2 size={20} className="animate-spin" />
                              ) : (
                                <Trash2 size={20} />
                              )}
                            </motion.button>
                          </div>

                          {/* Subtotal */}
                          <div className="text-right font-semibold text-slate-900">
                            ${(item.price * item.quantity).toFixed(2)}
                          </div>
                        </CardContent>
                      </Card>
                    </motion.div>
                  ))}
                </AnimatePresence>
              </div>
            )}
          </div>

          {/* Order Summary */}
          {items.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
            >
              <Card className="sticky top-24 bg-gradient-to-br from-slate-50 to-slate-100">
                <CardHeader>
                  <CardTitle>Order Summary</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-3 pb-4 border-b border-slate-200">
                    <div className="flex justify-between text-slate-600">
                      <span>Subtotal</span>
                      <span>${cartTotal.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between text-slate-600">
                      <span>Tax (8%)</span>
                      <span>${tax.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between text-slate-600">
                      <span>Shipping</span>
                      <span>Free</span>
                    </div>
                  </div>

                  <div className="flex justify-between text-lg font-bold text-slate-900">
                    <span>Total</span>
                    <span className="text-red-500">${finalTotal.toFixed(2)}</span>
                  </div>

                  <motion.button
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    onClick={handleCheckout}
                    disabled={loading}
                    className={`w-full px-4 py-3 rounded-lg font-semibold text-white transition-all ${
                      loading
                        ? 'bg-gray-400'
                        : 'bg-red-500 hover:bg-red-600'
                    }`}
                  >
                    {loading ? (
                      <>
                        <Loader2 size={16} className="animate-spin inline mr-2" />
                        Processing...
                      </>
                    ) : (
                      'Proceed to Checkout'
                    )}
                  </motion.button>

                  <Button
                    variant="outline"
                    onClick={() => navigate('/shopping')}
                    className="w-full border-slate-300 text-slate-900 hover:bg-slate-50"
                  >
                    Continue Shopping
                  </Button>
                </CardContent>
              </Card>
            </motion.div>
          )}
        </div>
      </main>
    </div>
  );
};

export default CartPage;
