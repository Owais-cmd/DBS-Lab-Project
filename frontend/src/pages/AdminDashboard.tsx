import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useStore } from '../store';
import { api } from '../services/api';
import { Item, MetricsResponse, IndexRecommendation, IndexInfo } from '../types/api';
import { Button } from '../lib/ui/button';
import { Input } from '../lib/ui/input';
import { Textarea } from '../lib/ui/textarea';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../lib/ui/card';
import { Badge } from '../lib/ui/badge';
import { 
  Package, Plus, Trash2, Edit, BarChart3, Database, 
  Users, ShoppingBag, TrendingUp, XCircle 
} from 'lucide-react';
import { formatPrice } from '../lib/priceFormatter';

export const AdminDashboard: React.FC = () => {
  const { user } = useStore();
  const [activeTab, setActiveTab] = useState<'overview' | 'products' | 'indexes'>('overview');
  
  // Products state
  const [items, setItems] = useState<Item[]>([]);
  const [showAddProduct, setShowAddProduct] = useState(false);
  const [editingItem, setEditingItem] = useState<Item | null>(null);
  const [formData, setFormData] = useState({
    name: '',
    price: '',
    description: '',
    category: '',
    image_url: ''
  });
  const [loading, setLoading] = useState(false);
  
  // Metrics state
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  
  // Indexes state
  const [recommendations, setRecommendations] = useState<IndexRecommendation[]>([]);
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [applyingIndex, setApplyingIndex] = useState<string | null>(null);
  const [comparisons, setComparisons] = useState<Record<string, any>>({});
  const [loadingComparison, setLoadingComparison] = useState<string | null>(null);

  useEffect(() => {
    loadData();
  }, [activeTab]);

  const loadData = async () => {
    try {
      if (activeTab === 'overview') {
        await loadMetrics();
      } else if (activeTab === 'products') {
        await loadItems();
      } else if (activeTab === 'indexes') {
        await loadIndexData();
      }
    } catch (err) {
      console.error('Error loading data:', err);
    }
  };

  const loadMetrics = async () => {
    try {
      const data = await api.metrics.getMetrics();
      setMetrics(data);
    } catch (err) {
      console.error('Error loading metrics:', err);
    }
  };

  const loadItems = async () => {
    try {
      const data = await api.items.getAll();
      setItems(data);
    } catch (err) {
      console.error('Error loading items:', err);
    }
  };

  const loadIndexData = async () => {
    try {
      const [recs, idxs] = await Promise.all([
        api.indexes.getRecommendations(),
        api.indexes.getIndexes()
      ]);
      setRecommendations(recs);
      setIndexes(idxs);
    } catch (err) {
      console.error('Error loading index data:', err);
    }
  };

  const handleAddProduct = async () => {
    if (!formData.name || !formData.price || !formData.category) {
      alert('Please fill all required fields');
      return;
    }

    setLoading(true);
    try {
      await api.items.create({
        name: formData.name,
        price: parseFloat(formData.price),
        description: formData.description,
        category: formData.category,
        image_url: formData.image_url || undefined
      });
      setFormData({ name: '', price: '', description: '', category: '', image_url: '' });
      setShowAddProduct(false);
      await loadItems();
    } catch (err: any) {
      alert('Failed to add product: ' + (err.message || ''));
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateProduct = async () => {
    if (!editingItem || !formData.name || !formData.price || !formData.category) {
      alert('Please fill all required fields');
      return;
    }

    setLoading(true);
    try {
      await api.items.update(editingItem.id, {
        name: formData.name,
        price: parseFloat(formData.price),
        description: formData.description,
        category: formData.category,
        image_url: formData.image_url || undefined
      });
      setFormData({ name: '', price: '', description: '', category: '', image_url: '' });
      setEditingItem(null);
      await loadItems();
    } catch (err: any) {
      alert('Failed to update product: ' + (err.message || ''));
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteProduct = async (id: number) => {
    if (!confirm('Are you sure you want to delete this product?')) return;

    try {
      await api.items.delete(id);
      await loadItems();
    } catch (err: any) {
      alert('Failed to delete product: ' + (err.message || ''));
    }
  };

  const handleEditClick = (item: Item) => {
    setEditingItem(item);
    setFormData({
      name: item.name,
      price: item.price.toString(),
      description: item.description || '',
      category: item.category || '',
      image_url: item.image_url || ''
    });
  };

  const handleApplyIndex = async (table: string, column: string) => {
    const key = `${table}.${column}`;
    setApplyingIndex(key);
    try {
      await api.indexes.applyIndex({ table, column, force: true });
      await loadIndexData();
    } catch (err: any) {
      alert('Failed to apply index: ' + (err.message || ''));
    } finally {
      setApplyingIndex(null);
    }
  };

  const handleGetComparison = async (indexName: string) => {
    setLoadingComparison(indexName);
    try {
      const result = await api.indexes.getComparison(indexName);
      setComparisons(prev => ({ ...prev, [indexName]: result }));
    } catch (err: any) {
      alert('Failed to get comparison: ' + (err.message || ''));
    } finally {
      setLoadingComparison(null);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Admin Dashboard</h1>
          <p className="text-gray-600">Welcome back, {user?.name}</p>
        </div>

        {/* Tabs */}
        <div className="flex gap-2 mb-8 border-b border-gray-200">
          <button
            onClick={() => setActiveTab('overview')}
            className={`px-6 py-3 font-semibold transition-all border-b-2 ${
              activeTab === 'overview'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-600 hover:text-gray-900'
            }`}
          >
            <BarChart3 className="inline mr-2 h-4 w-4" />
            Overview
          </button>
          <button
            onClick={() => setActiveTab('products')}
            className={`px-6 py-3 font-semibold transition-all border-b-2 ${
              activeTab === 'products'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-600 hover:text-gray-900'
            }`}
          >
            <Package className="inline mr-2 h-4 w-4" />
            Products
          </button>
          <button
            onClick={() => setActiveTab('indexes')}
            className={`px-6 py-3 font-semibold transition-all border-b-2 ${
              activeTab === 'indexes'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-600 hover:text-gray-900'
            }`}
          >
            <Database className="inline mr-2 h-4 w-4" />
            Database Indexes
          </button>
        </div>

        {/* Content */}
        <AnimatePresence mode="wait">
          {/* Overview Tab */}
          {activeTab === 'overview' && (
            <motion.div
              key="overview"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              {metrics && (
                <div className="space-y-6">
                  {/* Stats Cards */}
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium text-gray-600">Total Users</CardTitle>
                        <Users className="h-4 w-4 text-gray-400" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{metrics.total_users}</div>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium text-gray-600">Total Items</CardTitle>
                        <Package className="h-4 w-4 text-gray-400" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{metrics.total_items}</div>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium text-gray-600">Total Orders</CardTitle>
                        <ShoppingBag className="h-4 w-4 text-gray-400" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{metrics.total_orders}</div>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium text-gray-600">Total Revenue</CardTitle>
                        <TrendingUp className="h-4 w-4 text-gray-400" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{formatPrice(metrics.total_revenue)}</div>
                      </CardContent>
                    </Card>
                  </div>

                  {/* Top Customers */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Top Customers</CardTitle>
                      <CardDescription>Most valuable customers by order amount</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-4">
                        {metrics.top_customers.slice(0, 5).map((customer, index) => (
                          <div key={customer.user_id} className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                              <div className="w-8 h-8 bg-blue-100 rounded-full flex items-center justify-center font-bold text-blue-600">
                                {index + 1}
                              </div>
                              <div>
                                <p className="font-semibold">{customer.user_name}</p>
                                <p className="text-sm text-gray-500">{customer.user_email}</p>
                              </div>
                            </div>
                            <div className="text-right">
                              <p className="font-bold text-blue-600">{formatPrice(customer.total_spent)}</p>
                              <p className="text-sm text-gray-500">{customer.order_count} orders</p>
                            </div>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>

                  {/* Recent Orders */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Recent Orders</CardTitle>
                      <CardDescription>Latest orders placed - manage order statuses</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-4">
                        {metrics.recent_orders.slice(0, 10).map((order) => (
                          <div key={order.order_id} className="flex items-center justify-between border-b pb-3 last:border-0">
                            <div className="flex-grow">
                              <p className="font-semibold">Order #{order.order_id}</p>
                              <p className="text-sm text-gray-500">{order.user_name} ({order.user_email})</p>
                              <p className="text-xs text-gray-400">{new Date(order.created_at).toLocaleString()}</p>
                            </div>
                            <div className="text-right flex items-center gap-3">
                              <div>
                                <p className="font-bold">{formatPrice(order.total_amount)}</p>
                                <Badge variant={order.status === 'delivered' ? 'default' : 'secondary'}>
                                  {order.status}
                                </Badge>
                              </div>
                              {order.status === 'placed' && (
                                <Button
                                  size="sm"
                                  onClick={async () => {
                                    try {
                                      await api.orders.markDelivered(order.order_id);
                                      await loadMetrics();
                                    } catch (err: any) {
                                      alert('Failed to mark as delivered: ' + (err.message || ''));
                                    }
                                  }}
                                >
                                  Mark Delivered
                                </Button>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </motion.div>
          )}

          {/* Products Tab */}
          {activeTab === 'products' && (
            <motion.div
              key="products"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h2 className="text-2xl font-bold text-gray-900">Manage Products</h2>
                  <p className="text-gray-600">Total products: {items.length}</p>
                </div>
                <Button onClick={() => setShowAddProduct(true)}>
                  <Plus className="w-4 h-4 mr-2" />
                  Add Product
                </Button>
              </div>

              {/* Add/Edit Product Form */}
              {(showAddProduct || editingItem) && (
                <Card className="mb-6">
                  <CardHeader>
                    <CardTitle>{editingItem ? 'Edit Product' : 'Add New Product'}</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium mb-2">Name</label>
                        <Input
                          value={formData.name}
                          onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                          placeholder="Product name"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-2">Price</label>
                        <Input
                          type="number"
                          step="0.01"
                          value={formData.price}
                          onChange={(e) => setFormData({ ...formData, price: e.target.value })}
                          placeholder="0.00"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-2">Category</label>
                        <Input
                          value={formData.category}
                          onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                          placeholder="Electronics, Clothing, etc."
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium mb-2">Image URL</label>
                        <Input
                          value={formData.image_url}
                          onChange={(e) => setFormData({ ...formData, image_url: e.target.value })}
                          placeholder="https://..."
                        />
                      </div>
                      <div className="md:col-span-2">
                        <label className="block text-sm font-medium mb-2">Description</label>
                        <Textarea
                          value={formData.description}
                          onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                          placeholder="Product description"
                          rows={3}
                        />
                      </div>
                    </div>
                    <div className="flex gap-2 mt-4">
                      <Button
                        onClick={editingItem ? handleUpdateProduct : handleAddProduct}
                        disabled={loading}
                      >
                        {loading ? 'Saving...' : editingItem ? 'Update' : 'Add'}
                      </Button>
                      <Button
                        variant="outline"
                        onClick={() => {
                          setShowAddProduct(false);
                          setEditingItem(null);
                          setFormData({ name: '', price: '', description: '', category: '', image_url: '' });
                        }}
                      >
                        Cancel
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Products List */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {items.map((item) => (
                  <Card key={item.id}>
                    <CardHeader>
                      {item.image_url && (
                        <img
                          src={item.image_url}
                          alt={item.name}
                          className="w-full h-48 object-cover rounded-lg mb-3"
                        />
                      )}
                      <CardTitle className="text-lg">{item.name}</CardTitle>
                      <CardDescription className="line-clamp-2">{item.description}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="flex items-center justify-between mb-3">
                        <Badge>{item.category}</Badge>
                        <span className="text-xl font-bold text-blue-600">{formatPrice(item.price)}</span>
                      </div>
                      <div className="flex gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleEditClick(item)}
                          className="flex-1"
                        >
                          <Edit className="w-4 h-4 mr-1" />
                          Edit
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleDeleteProduct(item.id)}
                          className="text-red-600 hover:text-red-700"
                        >
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </motion.div>
          )}

          {/* Indexes Tab */}
          {activeTab === 'indexes' && (
            <motion.div
              key="indexes"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <div className="space-y-6">
                {/* Recommendations */}
                <Card>
                  <CardHeader>
                    <CardTitle>Index Recommendations</CardTitle>
                    <CardDescription>Optimize database performance by creating recommended indexes</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {recommendations.filter(rec => !rec.index_exists).map((rec, index) => {
                        const key = `${rec.table}.${rec.column}`;
                        const isApplying = applyingIndex === key;
                        
                        return (
                          <div key={index} className="border rounded-lg p-4">
                            <div className="flex items-start justify-between">
                              <div className="flex-grow">
                                <div className="flex items-center gap-2 mb-2">
                                  <code className="bg-gray-100 px-2 py-1 rounded text-sm">
                                    {rec.table}.{rec.column}
                                  </code>
                                  <Badge variant="secondary">
                                    <XCircle className="w-3 h-3 mr-1" />
                                    Not Indexed
                                  </Badge>
                                </div>
                                <div className="text-sm text-gray-600 space-y-1">
                                  <p>Calls: <span className="font-semibold">{rec.calls}</span></p>
                                  <p>Avg Time: <span className="font-semibold">{rec.avg_time_ms.toFixed(2)}ms</span></p>
                                  {rec.sample_query && (
                                    <p className="mt-2">
                                      <span className="font-semibold">Sample Query:</span>
                                      <code className="block bg-gray-100 p-2 rounded mt-1 text-xs overflow-x-auto">
                                        {rec.sample_query}
                                      </code>
                                    </p>
                                  )}
                                </div>
                              </div>
                              <div>
                                {rec.recommend && (
                                  <Button
                                    size="sm"
                                    onClick={() => handleApplyIndex(rec.table, rec.column)}
                                    disabled={isApplying}
                                  >
                                    {isApplying ? 'Applying...' : 'Apply Index'}
                                  </Button>
                                )}
                              </div>
                            </div>
                          </div>
                        );
                      })}
                      {recommendations.filter(rec => !rec.index_exists).length === 0 && (
                        <p className="text-center text-gray-500 py-8">No pending recommendations - all suggested indexes have been applied</p>
                      )}
                    </div>
                  </CardContent>
                </Card>

                {/* Existing Indexes */}
                <Card>
                  <CardHeader>
                    <CardTitle>Existing Indexes</CardTitle>
                    <CardDescription>Currently active database indexes</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {indexes.map((idx, index) => {
                        const comparison = comparisons[idx.index_name];
                        const isLoadingComp = loadingComparison === idx.index_name;
                        
                        return (
                          <div key={index} className="border rounded-lg p-4">
                            <div className="flex items-start justify-between mb-2">
                              <div className="flex-grow">
                                <p className="font-semibold">{idx.index_name}</p>
                                <p className="text-sm text-gray-600">
                                  {idx.table_name}.{idx.column_name}
                                </p>
                                <p className="text-xs text-gray-500 mt-1">
                                  Created: {new Date(idx.created_at).toLocaleString()} by {idx.user_name}
                                </p>
                              </div>
                              <div className="flex items-center gap-2">
                                <Badge>{idx.size}</Badge>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => handleGetComparison(idx.index_name)}
                                  disabled={isLoadingComp}
                                >
                                  {isLoadingComp ? 'Loading...' : 'Compare'}
                                </Button>
                              </div>
                            </div>
                            {comparison && (
                              <div className="mt-3 p-3 bg-blue-50 rounded border border-blue-200">
                                <p className="text-sm font-semibold text-blue-900 mb-2">Performance Comparison:</p>
                                <div className="grid grid-cols-2 gap-3 text-sm">
                                  <div>
                                    <p className="text-gray-600">Sequential Scan:</p>
                                    <p className="font-bold text-red-600">{(comparison.seq_scan_time * 1000).toFixed(2)}ms</p>
                                  </div>
                                  <div>
                                    <p className="text-gray-600">Index Scan:</p>
                                    <p className="font-bold text-green-600">{(comparison.index_scan_time * 1000).toFixed(2)}ms</p>
                                  </div>
                                </div>
                                <div className="mt-2">
                                  <Badge variant={comparison.faster_scan === 'index_scan' ? 'default' : 'secondary'}>
                                    {comparison.faster_scan === 'index_scan' ? '✓ Index is faster' : '⚠ Sequential scan is faster'}
                                  </Badge>
                                  {comparison.faster_scan === 'index_scan' && (
                                    <span className="ml-2 text-xs text-green-600 font-semibold">
                                      {((comparison.seq_scan_time / comparison.index_scan_time)).toFixed(1)}x faster
                                    </span>
                                  )}
                                </div>
                              </div>
                            )}
                          </div>
                        );
                      })}
                      {indexes.length === 0 && (
                        <p className="text-center text-gray-500 py-8">No indexes found</p>
                      )}
                    </div>
                  </CardContent>
                </Card>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
};

export default AdminDashboard;
