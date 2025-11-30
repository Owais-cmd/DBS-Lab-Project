import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { useStore } from '../store';
import { api } from '../services/api';
import { Item } from '../types/api';
import { Button } from '../lib/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../lib/ui/card';
import { ShoppingCart, Trash2, Plus, Minus, ArrowRight, Package } from 'lucide-react';
import { formatPrice } from '../lib/priceFormatter';

export const Cart: React.FC = () => {
  const navigate = useNavigate();
  const { cart, updateCartItem, removeFromCart, placeOrder } = useStore();
  const [items, setItems] = useState<Map<number, Item>>(new Map());
  const [loading, setLoading] = useState(true);
  const [placing, setPlacing] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    loadCartItems();
  }, [cart]);

  const loadCartItems = async () => {
    try {
      setLoading(true);
      const itemMap = new Map<number, Item>();
      
      // Fetch item details for each cart item
      for (const cartItem of cart) {
        if (!itemMap.has(cartItem.item_id)) {
          const item = await api.items.getById(cartItem.item_id);
          itemMap.set(cartItem.item_id, item);
        }
      }
      
      setItems(itemMap);
      setError('');
    } catch (err: any) {
      setError('Failed to load cart items');
      console.error('Error loading cart items:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateQuantity = async (itemId: number, newQuantity: number) => {
    if (newQuantity < 1) return;
    
    try {
      await updateCartItem(itemId, newQuantity);
    } catch (err: any) {
      console.error('Error updating quantity:', err);
    }
  };

  const handleRemoveItem = async (itemId: number) => {
    try {
      await removeFromCart(itemId);
    } catch (err: any) {
      console.error('Error removing item:', err);
    }
  };

  const handlePlaceOrder = async () => {
    setPlacing(true);
    try {
      await placeOrder();
      navigate('/shopping');
    } catch (err: any) {
      setError('Failed to place order. Please try again.');
      console.error('Error placing order:', err);
    } finally {
      setPlacing(false);
    }
  };

  const calculateTotal = (): number => {
    return cart.reduce((total, cartItem) => {
      const item = items.get(cartItem.item_id);
      return total + (item ? item.price * cartItem.quantity : 0);
    }, 0);
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <ShoppingCart className="w-16 h-16 mx-auto text-gray-400 animate-pulse" />
          <p className="mt-4 text-gray-600">Loading cart...</p>
        </div>
      </div>
    );
  }

  if (cart.length === 0) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <motion.div
            initial={{ scale: 0.8, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            transition={{ duration: 0.5 }}
          >
            <ShoppingCart className="w-24 h-24 mx-auto text-gray-300" />
            <h2 className="mt-6 text-2xl font-bold text-gray-900">Your cart is empty</h2>
            <p className="mt-2 text-gray-600">Add some products to get started!</p>
            <Button onClick={() => navigate('/shopping')} className="mt-6">
              Start Shopping
              <ArrowRight className="w-4 h-4 ml-2" />
            </Button>
          </motion.div>
        </div>
      </div>
    );
  }

  const total = calculateTotal();

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Shopping Cart</h1>
          <p className="text-gray-600">{cart.length} {cart.length === 1 ? 'item' : 'items'} in your cart</p>
        </div>

        {error && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-600"
          >
            {error}
          </motion.div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Cart Items */}
          <div className="lg:col-span-2 space-y-4">
            {cart.map((cartItem, index) => {
              const item = items.get(cartItem.item_id);
              if (!item) return null;

              return (
                <motion.div
                  key={cartItem.item_id}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ duration: 0.3, delay: index * 0.1 }}
                >
                  <Card>
                    <CardContent className="p-4">
                      <div className="flex gap-4">
                        {/* Item Image */}
                        <div className="flex-shrink-0 w-24 h-24 bg-gray-100 rounded-lg overflow-hidden">
                          {item.image_url ? (
                            <img
                              src={item.image_url}
                              alt={item.name}
                              className="w-full h-full object-cover"
                              onError={(e) => {
                                e.currentTarget.src = `https://via.placeholder.com/400x300?text=${encodeURIComponent(item.name)}`;
                              }}
                            />
                          ) : (
                            <div className="w-full h-full flex items-center justify-center">
                              <Package className="w-8 h-8 text-gray-300" />
                            </div>
                          )}
                        </div>

                        {/* Item Details */}
                        <div className="flex-grow">
                          <h3 className="font-semibold text-lg text-gray-900">{item.name}</h3>
                          <p className="text-sm text-gray-600 mt-1 line-clamp-2">{item.description}</p>
                          <p className="text-sm text-gray-500 mt-1">Category: {item.category}</p>
                          <p className="text-lg font-bold text-blue-600 mt-2">{formatPrice(item.price)}</p>
                        </div>

                        {/* Quantity Controls */}
                        <div className="flex flex-col items-end justify-between">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleRemoveItem(cartItem.item_id)}
                            className="text-red-600 hover:text-red-700 hover:bg-red-50"
                          >
                            <Trash2 className="w-4 h-4" />
                          </Button>

                          <div className="flex items-center gap-2 bg-gray-100 rounded-lg p-1">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleUpdateQuantity(cartItem.item_id, cartItem.quantity - 1)}
                              disabled={cartItem.quantity <= 1}
                              className="h-8 w-8 p-0"
                            >
                              <Minus className="w-4 h-4" />
                            </Button>
                            <span className="w-8 text-center font-semibold">{cartItem.quantity}</span>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleUpdateQuantity(cartItem.item_id, cartItem.quantity + 1)}
                              className="h-8 w-8 p-0"
                            >
                              <Plus className="w-4 h-4" />
                            </Button>
                          </div>

                          <div className="text-right">
                            <p className="text-sm text-gray-600">Subtotal</p>
                            <p className="text-lg font-bold text-gray-900">
                              {formatPrice(item.price * cartItem.quantity)}
                            </p>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </motion.div>
              );
            })}
          </div>

          {/* Order Summary */}
          <div className="lg:col-span-1">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5 }}
              className="sticky top-8"
            >
              <Card>
                <CardHeader>
                  <CardTitle>Order Summary</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">Subtotal</span>
                      <span className="font-semibold">{formatPrice(total)}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">Shipping</span>
                      <span className="font-semibold text-green-600">Free</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">Tax</span>
                      <span className="font-semibold">Included</span>
                    </div>
                  </div>

                  <div className="border-t pt-4">
                    <div className="flex justify-between items-center mb-6">
                      <span className="text-lg font-bold text-gray-900">Total</span>
                      <span className="text-2xl font-bold text-blue-600">{formatPrice(total)}</span>
                    </div>

                    <Button
                      onClick={handlePlaceOrder}
                      className="w-full"
                      size="lg"
                      disabled={placing}
                    >
                      {placing ? 'Placing Order...' : 'Place Order'}
                      {!placing && <ArrowRight className="w-4 h-4 ml-2" />}
                    </Button>

                    <Button
                      onClick={() => navigate('/shopping')}
                      variant="outline"
                      className="w-full mt-3"
                    >
                      Continue Shopping
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </motion.div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Cart;
