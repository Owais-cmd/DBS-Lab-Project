import React, { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useCartStore, useProductStore } from '@/store';
import { Button } from '@/lib/ui/button';
import { Badge } from '@/lib/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui/card';
import { ShoppingCart, Heart, Star, Loader2, AlertCircle } from 'lucide-react';
import { formatPriceSimple } from '@/lib/priceFormatter';

export const ShoppingPage: React.FC = () => {
  const { products, loading, fetchProducts } = useProductStore();
  const { items, addToCart } = useCartStore();
  const [addingId, setAddingId] = useState<number | null>(null);
  const [quantity, setQuantity] = useState<{ [key: number]: number }>({});
  const [wishlist, setWishlist] = useState<Set<number>>(new Set());
  const cartTotal = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

  useEffect(() => {
    fetchProducts();
  }, []);

  const handleAddToCart = async (productId: number) => {
    setAddingId(productId);
    try {
      const product = products.find(p => p.id === productId);
      if (product) {
        await addToCart(product, quantity[productId] || 1);
        setQuantity({ ...quantity, [productId]: 1 });
      }
    } catch (error) {
      console.error('Failed to add to cart:', error);
    } finally {
      setAddingId(null);
    }
  };

  const toggleWishlist = (productId: number) => {
    const newWishlist = new Set(wishlist);
    if (newWishlist.has(productId)) {
      newWishlist.delete(productId);
    } else {
      newWishlist.add(productId);
    }
    setWishlist(newWishlist);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-12">
        {/* Hero Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="mb-12 text-center"
        >
          <h2 className="text-4xl font-bold text-slate-900 mb-4">
            Discover Amazing Products
          </h2>
          <p className="text-xl text-slate-600">
            Curated selection with optimized database performance
          </p>
        </motion.div>

        {/* Products Grid */}
        {loading ? (
          <div className="flex items-center justify-center py-20">
            <Loader2 className="animate-spin text-red-500" size={40} />
          </div>
        ) : products.length === 0 ? (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="text-center py-20"
          >
            <AlertCircle className="mx-auto mb-4 text-slate-400" size={40} />
            <p className="text-slate-600">No products available. Admin, add some!</p>
          </motion.div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <AnimatePresence>
              {products.map((product, index) => (
                <motion.div
                  key={product.id}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -20 }}
                  transition={{ delay: index * 0.1 }}
                  whileHover={{ y: -8 }}
                  className="h-full"
                >
                  <Card className="overflow-hidden hover:shadow-lg transition-shadow h-full flex flex-col">
                    {/* Product Image */}
                    <div className="w-full h-48 bg-gradient-to-br from-red-100 to-red-50 flex items-center justify-center relative overflow-hidden">
                      <img
                        src={product.image_url || ''}
                        alt={product.name}
                        className="w-full h-full object-cover hover:scale-105 transition-transform duration-300"
                        crossOrigin="anonymous"
                        referrerPolicy="no-referrer"
                        onError={(e) => {
                          (e.target as HTMLImageElement).style.display = 'none';
                        }}
                      />
                      <span className="absolute text-4xl text-red-400 font-bold" style={{ display: product.image_url ? 'none' : 'block' }}>
                        {product.name.charAt(0)}
                      </span>
                      <motion.button
                        whileHover={{ scale: 1.2 }}
                        whileTap={{ scale: 0.9 }}
                        onClick={() => toggleWishlist(product.id)}
                        className="absolute top-3 right-3 bg-white rounded-full p-2 shadow-md hover:bg-red-50"
                      >
                        <Heart
                          size={20}
                          className={wishlist.has(product.id) ? 'fill-red-500 text-red-500' : 'text-slate-400'}
                        />
                      </motion.button>
                    </div>

                    <CardHeader className="flex-1">
                      <div className="flex items-start justify-between mb-2">
                        <CardTitle className="text-lg">{product.name}</CardTitle>
                        <Badge variant="secondary" className="ml-2">
                          <Star size={12} className="mr-1" />
                          4.5
                        </Badge>
                      </div>
                      <CardDescription className="text-base">
                        {product.description}
                      </CardDescription>
                    </CardHeader>

                    <CardContent className="space-y-4">
                      <div className="flex items-center justify-between">
                        <span className="text-2xl font-bold text-red-500">
                          {formatPriceSimple(product.price)}
                        </span>
                        <div className="flex items-center gap-2 bg-slate-100 rounded-lg p-1">
                          <button
                            onClick={() => setQuantity({
                              ...quantity,
                              [product.id]: Math.max(1, (quantity[product.id] || 1) - 1)
                            })}
                            className="px-2 py-1 hover:bg-slate-200 rounded"
                          >
                            −
                          </button>
                          <span className="w-6 text-center font-semibold">
                            {quantity[product.id] || 1}
                          </span>
                          <button
                            onClick={() => setQuantity({
                              ...quantity,
                              [product.id]: (quantity[product.id] || 1) + 1
                            })}
                            className="px-2 py-1 hover:bg-slate-200 rounded"
                          >
                            +
                          </button>
                        </div>
                      </div>

                      <motion.button
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                        onClick={() => handleAddToCart(product.id)}
                        disabled={addingId === product.id}
                        className={`w-full px-4 py-2 rounded-lg font-semibold transition-all flex items-center justify-center gap-2 ${
                          addingId === product.id
                            ? 'bg-gray-300 text-gray-600'
                            : 'bg-red-500 hover:bg-red-600 text-white'
                        }`}
                      >
                        {addingId === product.id ? (
                          <>
                            <Loader2 size={16} className="animate-spin" />
                            Adding...
                          </>
                        ) : (
                          <>
                            <ShoppingCart size={16} />
                            Add to Cart
                          </>
                        )}
                      </motion.button>
                    </CardContent>
                  </Card>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        )}
      </main>
    </div>
  );
};

export default ShoppingPage;
