import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { useStore } from '../store';
import { api } from '../services/api';
import { Item } from '../types/api';
import { Button } from '../lib/ui/button';
import { Input } from '../lib/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../lib/ui/card';
import { Badge } from '../lib/ui/badge';
import { Search, ShoppingCart, Package, Filter } from 'lucide-react';
import { formatPrice } from '../lib/priceFormatter';

export const Shopping: React.FC = () => {
  const [items, setItems] = useState<Item[]>([]);
  const [filteredItems, setFilteredItems] = useState<Item[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [categories, setCategories] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [addingToCart, setAddingToCart] = useState<number | null>(null);
  
  const { addToCart, cart } = useStore();

  useEffect(() => {
    loadItems();
  }, []);

  useEffect(() => {
    filterItems();
  }, [searchQuery, selectedCategory, items]);

  const loadItems = async () => {
    try {
      setLoading(true);
      const data = await api.items.getAll();
      setItems(data);
      
      // Extract unique categories (filter out undefined)
      const uniqueCategories = Array.from(
        new Set(data.map(item => item.category).filter((cat): cat is string => cat !== undefined))
      );
      setCategories(uniqueCategories);
      
      setError('');
    } catch (err: any) {
      setError('Failed to load items. Please try again.');
      console.error('Error loading items:', err);
    } finally {
      setLoading(false);
    }
  };

  const filterItems = () => {
    let filtered = items;

    // Filter by category
    if (selectedCategory !== 'all') {
      filtered = filtered.filter(item => item.category === selectedCategory);
    }

    // Filter by search query
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(
        item =>
          item.name.toLowerCase().includes(query) ||
          (item.description && item.description.toLowerCase().includes(query)) ||
          (item.category && item.category.toLowerCase().includes(query))
      );
    }

    setFilteredItems(filtered);
  };

  const handleAddToCart = async (item: Item) => {
    setAddingToCart(item.id);
    try {
      await addToCart(item.id, 1);
    } catch (err: any) {
      console.error('Error adding to cart:', err);
    } finally {
      setAddingToCart(null);
    }
  };

  const getItemQuantityInCart = (itemId: number): number => {
    return cart.find(ci => ci.item_id === itemId)?.quantity || 0;
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <Package className="w-16 h-16 mx-auto text-gray-400 animate-pulse" />
          <p className="mt-4 text-gray-600">Loading products...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Shop Products</h1>
          <p className="text-gray-600">Discover amazing products at great prices</p>
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

        {/* Search and Filter */}
        <div className="mb-8 space-y-4">
          <div className="relative">
            <Search className="absolute left-3 top-3 h-5 w-5 text-gray-400" />
            <Input
              type="text"
              placeholder="Search products..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10 bg-white"
            />
          </div>

          {/* Category Filter */}
          <div className="flex items-center gap-2 flex-wrap">
            <Filter className="h-4 w-4 text-gray-500" />
            <Badge
              variant={selectedCategory === 'all' ? 'default' : 'secondary'}
              className="cursor-pointer"
              onClick={() => setSelectedCategory('all')}
            >
              All
            </Badge>
            {categories.map(category => (
              <Badge
                key={category}
                variant={selectedCategory === category ? 'default' : 'secondary'}
                className="cursor-pointer"
                onClick={() => setSelectedCategory(category)}
              >
                {category}
              </Badge>
            ))}
          </div>
        </div>

        {/* Results Count */}
        <div className="mb-4 text-sm text-gray-600">
          Showing {filteredItems.length} of {items.length} products
        </div>

        {/* Products Grid */}
        {filteredItems.length === 0 ? (
          <div className="text-center py-16">
            <Package className="w-16 h-16 mx-auto text-gray-400" />
            <p className="mt-4 text-gray-600">No products found</p>
            <Button
              onClick={() => {
                setSearchQuery('');
                setSelectedCategory('all');
              }}
              className="mt-4"
            >
              Clear Filters
            </Button>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            {filteredItems.map((item, index) => {
              const quantityInCart = getItemQuantityInCart(item.id);
              const isAdding = addingToCart === item.id;

              return (
                <motion.div
                  key={item.id}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.3, delay: index * 0.05 }}
                >
                  <Card className="h-full flex flex-col hover:shadow-lg transition-shadow duration-300">
                    <div className="relative h-48 overflow-hidden rounded-t-lg bg-gray-100">
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
                          <Package className="w-16 h-16 text-gray-300" />
                        </div>
                      )}
                      <Badge className="absolute top-2 right-2 bg-white text-gray-900">
                        {item.category}
                      </Badge>
                    </div>

                    <CardHeader className="flex-grow">
                      <CardTitle className="text-lg">{item.name}</CardTitle>
                      <CardDescription className="line-clamp-2">
                        {item.description}
                      </CardDescription>
                    </CardHeader>

                    <CardContent className="pt-0">
                      <div className="flex items-center justify-between mb-4">
                        <span className="text-2xl font-bold text-blue-600">
                          {formatPrice(item.price)}
                        </span>
                      </div>

                      <Button
                        onClick={() => handleAddToCart(item)}
                        className="w-full"
                        disabled={isAdding}
                      >
                        <ShoppingCart className="w-4 h-4 mr-2" />
                        {isAdding ? 'Adding...' : quantityInCart > 0 ? `In Cart (${quantityInCart})` : 'Add to Cart'}
                      </Button>
                    </CardContent>
                  </Card>
                </motion.div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};

export default Shopping;
