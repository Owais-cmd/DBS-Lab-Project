import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { useProductStore, useAuthStore } from '@/store';
import { Button } from '@/lib/ui/button';
import { Input } from '@/lib/ui/input';
import { Textarea } from '@/lib/ui/textarea';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/lib/ui/card';
import { Package, Plus, Trash2, BarChart3, Loader2, ChevronRight } from 'lucide-react';
import axios from 'axios';

interface Recommendation {
  table: string;
  column: string;
  calls: number;
  avg_time_ms: number;
  index_exists: boolean;
  recommend: boolean;
  sample_query: string;
}

interface Index {
  index_name: string;
  table_name: string;
  column_name: string;
  created_at: string;
  user_name: string;
  size: string;
}

export const AdminDashboard: React.FC = () => {
  const navigate = useNavigate();
  const { user, logout } = useAuthStore();
  const { products, addProduct, updateProduct, deleteProduct, fetchProducts } = useProductStore();

  const [activeTab, setActiveTab] = useState<'products' | 'analysis'>('products');
  const [showAddProduct, setShowAddProduct] = useState(false);
  const [showEditProduct, setShowEditProduct] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [formData, setFormData] = useState({ name: '', price: '', description: '', image_url: '' });
  const [loading, setLoading] = useState(false);
  const [deletingId, setDeletingId] = useState<number | null>(null);

  // Analysis state
  const [recommendations, setRecommendations] = useState<Recommendation[]>([]);
  const [indexes, setIndexes] = useState<Index[]>([]);
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [selectedRec, setSelectedRec] = useState<Recommendation | null>(null);
  const [applyingIndex, setApplyingIndex] = useState(false);

  useEffect(() => {
    if (user?.role !== 'admin') {
      navigate('/');
      return;
    }
    fetchProducts();
  }, []);

  // Fetch recommendations and indexes
  useEffect(() => {
    if (activeTab === 'analysis') {
      fetchAnalysisData();
    }
  }, [activeTab]);

  const fetchAnalysisData = async () => {
    setAnalysisLoading(true);
    try {
      const token = localStorage.getItem('token');
      const [recsRes, indexesRes] = await Promise.all([
        axios.get('http://localhost:8000/recommendations', {
          headers: { Authorization: `Bearer ${token}` }
        }),
        axios.get('http://localhost:8000/indexes', {
          headers: { Authorization: `Bearer ${token}` }
        })
      ]);
      setRecommendations(recsRes.data);
      setIndexes(indexesRes.data);
    } catch (error) {
      console.error('Failed to fetch analysis data:', error);
    } finally {
      setAnalysisLoading(false);
    }
  };

  const handleAddProduct = async () => {
    if (!formData.name || !formData.price || !formData.description) {
      alert('Please fill all required fields');
      return;
    }

    setLoading(true);
    try {
      await addProduct(formData.name, parseFloat(formData.price), formData.description, formData.image_url);
      setFormData({ name: '', price: '', description: '', image_url: '' });
      setShowAddProduct(false);
    } catch (error) {
      console.error('Failed to add product:', error);
      alert('Failed to add product');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteProduct = async (id: number) => {
    setDeletingId(id);
    try {
      await deleteProduct(id);
    } catch (error) {
      console.error('Failed to delete product:', error);
      alert('Failed to delete product');
    } finally {
      setDeletingId(null);
    }
  };

  const handleEditClick = (product: any) => {
    setEditingId(product.id);
    setFormData({
      name: product.name,
      price: product.price.toString(),
      description: product.description,
      image_url: product.image_url || ''
    });
    setShowEditProduct(true);
  };

  const handleUpdateProduct = async () => {
    if (!formData.name || !formData.price || !formData.description) {
      alert('Please fill all required fields');
      return;
    }

    setLoading(true);
    try {
      await updateProduct(
        editingId!,
        formData.name,
        parseFloat(formData.price),
        formData.description,
        formData.image_url
      );
      setFormData({ name: '', price: '', description: '', image_url: '' });
      setShowEditProduct(false);
      setEditingId(null);
    } catch (error) {
      console.error('Failed to update product:', error);
      alert('Failed to update product');
    } finally {
      setLoading(false);
    }
  };

  const handleApplyIndex = async (rec: Recommendation) => {
    setApplyingIndex(true);
    try {
      const token = localStorage.getItem('token');
      await axios.post(
        'http://localhost:8000/apply',
        {
          table: rec.table,
          column: rec.column,
          force: true,
          user: user?.email || 'admin'
        },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      await fetchAnalysisData();
      setSelectedRec(null);
    } catch (error) {
      console.error('Failed to apply index:', error);
      alert('Failed to apply index');
    } finally {
      setApplyingIndex(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      {/* Navigation Tabs */}
      <div className="bg-slate-950/30 border-b border-slate-800">
        <div className="max-w-7xl mx-auto px-4 flex gap-4">
          <button
            onClick={() => setActiveTab('products')}
            className={`px-6 py-4 font-semibold transition-all border-b-2 ${
              activeTab === 'products'
                ? 'border-red-500 text-red-500'
                : 'border-transparent text-slate-400 hover:text-slate-300'
            }`}
          >
            <Package className="inline mr-2" size={18} />
            Products
          </button>
          <button
            onClick={() => setActiveTab('analysis')}
            className={`px-6 py-4 font-semibold transition-all border-b-2 ${
              activeTab === 'analysis'
                ? 'border-red-500 text-red-500'
                : 'border-transparent text-slate-400 hover:text-slate-300'
            }`}
          >
            <BarChart3 className="inline mr-2" size={18} />
            Query Analysis
          </button>
        </div>
      </div>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-12">
        {/* Products Tab */}
        <AnimatePresence mode="wait">
          {activeTab === 'products' && (
            <motion.div
              key="products"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <div className="flex items-center justify-between mb-8">
                <div>
                  <h2 className="text-3xl font-bold text-white mb-2">Manage Products</h2>
                  <p className="text-slate-400">Total products: {products.length}</p>
                </div>
                <motion.button
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={() => setShowAddProduct(true)}
                  className="bg-red-500 hover:bg-red-600 text-white px-6 py-3 rounded-lg font-semibold flex items-center gap-2"
                >
                  <Plus size={20} />
                  Add Product
                </motion.button>
              </div>

              {/* Add Product Form */}
              <AnimatePresence>
                {showAddProduct && (
                  <motion.div
                    initial={{ opacity: 0, y: -20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -20 }}
                    className="mb-8"
                  >
                    <Card className="bg-slate-800 border-slate-700">
                      <CardHeader>
                        <CardTitle className="text-white">Add New Product</CardTitle>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Product Name
                          </label>
                          <Input
                            type="text"
                            placeholder="Enter product name"
                            value={formData.name}
                            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Price ($)
                          </label>
                          <Input
                            type="number"
                            placeholder="Enter price"
                            value={formData.price}
                            onChange={(e) => setFormData({ ...formData, price: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Description
                          </label>
                          <Textarea
                            placeholder="Enter product description"
                            value={formData.description}
                            onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Image URL (Optional)
                          </label>
                          <Input
                            type="text"
                            placeholder="Enter image URL"
                            value={formData.image_url}
                            onChange={(e) => setFormData({ ...formData, image_url: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div className="flex gap-3 pt-4">
                          <Button
                            onClick={handleAddProduct}
                            disabled={loading}
                            className="flex-1 bg-red-500 hover:bg-red-600"
                          >
                            {loading ? <Loader2 size={16} className="animate-spin inline mr-2" /> : null}
                            Add Product
                          </Button>
                          <Button
                            onClick={() => setShowAddProduct(false)}
                            variant="outline"
                            className="flex-1 border-slate-600 text-white hover:bg-slate-800 hover:text-white bg-slate-700"
                          >
                            Cancel
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  </motion.div>
                )}

                {showEditProduct && (
                  <motion.div
                    initial={{ opacity: 0, y: -20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -20 }}
                    className="mb-8"
                  >
                    <Card className="bg-slate-800 border-slate-700">
                      <CardHeader>
                        <CardTitle className="text-white">Update Product</CardTitle>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Product Name
                          </label>
                          <Input
                            type="text"
                            placeholder="Enter product name"
                            value={formData.name}
                            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Price ($)
                          </label>
                          <Input
                            type="number"
                            placeholder="Enter price"
                            value={formData.price}
                            onChange={(e) => setFormData({ ...formData, price: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Description
                          </label>
                          <Textarea
                            placeholder="Enter product description"
                            value={formData.description}
                            onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div>
                          <label className="block text-slate-300 text-sm font-semibold mb-2">
                            Image URL (Optional)
                          </label>
                          <Input
                            type="text"
                            placeholder="Enter image URL"
                            value={formData.image_url}
                            onChange={(e) => setFormData({ ...formData, image_url: e.target.value })}
                            className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-500"
                          />
                        </div>

                        <div className="flex gap-3 pt-4">
                          <Button
                            onClick={handleUpdateProduct}
                            disabled={loading}
                            className="flex-1 bg-blue-500 hover:bg-blue-600"
                          >
                            {loading ? <Loader2 size={16} className="animate-spin inline mr-2" /> : null}
                            Update Product
                          </Button>
                          <Button
                            onClick={() => {
                              setShowEditProduct(false);
                              setEditingId(null);
                              setFormData({ name: '', price: '', description: '', image_url: '' });
                            }}
                            variant="outline"
                            className="flex-1 border-slate-600 text-white hover:bg-slate-800 hover:text-white bg-slate-700"
                          >
                            Cancel
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* Products List */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                <AnimatePresence>
                  {products.map((product, index) => (
                    <motion.div
                      key={product.id}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0, scale: 0.95 }}
                      transition={{ delay: index * 0.05 }}
                      whileHover={{ y: -4 }}
                    >
                      <Card className="bg-slate-800 border-slate-700 h-full flex flex-col hover:border-red-500/50 transition-colors">
                        <div className="h-32 bg-gradient-to-br from-red-900 to-red-950 flex items-center justify-center">
                          <span className="text-4xl text-red-400 font-bold">
                            {product.name.charAt(0)}
                          </span>
                        </div>
                        <CardHeader className="flex-1">
                          <CardTitle className="text-white">{product.name}</CardTitle>
                          <CardDescription className="text-slate-400">
                            ${product.price.toFixed(2)}
                          </CardDescription>
                          <p className="text-slate-400 text-sm mt-2">{product.description}</p>
                        </CardHeader>
                        <CardContent>
                          <div className="flex gap-3">
                            <motion.button
                              whileHover={{ scale: 1.02 }}
                              whileTap={{ scale: 0.98 }}
                              onClick={() => handleEditClick(product)}
                              disabled={loading}
                              className={`flex-1 px-4 py-2 rounded-lg font-semibold transition-all flex items-center justify-center gap-2 ${
                                loading
                                  ? 'bg-gray-600 text-gray-300'
                                  : 'bg-blue-600 hover:bg-blue-700 text-white'
                              }`}
                            >
                              {loading && editingId === product.id ? (
                                <>
                                  <Loader2 size={16} className="animate-spin" />
                                  Updating...
                                </>
                              ) : (
                                <>
                                  <Package size={16} />
                                  Update
                                </>
                              )}
                            </motion.button>
                            <motion.button
                              whileHover={{ scale: 1.02 }}
                              whileTap={{ scale: 0.98 }}
                              onClick={() => handleDeleteProduct(product.id)}
                              disabled={deletingId === product.id}
                              className={`flex-1 px-4 py-2 rounded-lg font-semibold transition-all flex items-center justify-center gap-2 ${
                                deletingId === product.id
                                  ? 'bg-gray-600 text-gray-300'
                                  : 'bg-red-600 hover:bg-red-700 text-white'
                              }`}
                            >
                              {deletingId === product.id ? (
                                <>
                                  <Loader2 size={16} className="animate-spin" />
                                  Deleting...
                                </>
                              ) : (
                                <>
                                  <Trash2 size={16} />
                                  Delete
                                </>
                              )}
                            </motion.button>
                          </div>
                        </CardContent>
                      </Card>
                    </motion.div>
                  ))}
                </AnimatePresence>
              </div>

              {products.length === 0 && (
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  className="text-center py-12"
                >
                  <Package className="mx-auto text-slate-600 mb-4" size={48} />
                  <p className="text-slate-400">No products yet. Add one to get started!</p>
                </motion.div>
              )}
            </motion.div>
          )}

          {/* Analysis Tab */}
          {activeTab === 'analysis' && (
            <motion.div
              key="analysis"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <div className="mb-8">
                <h2 className="text-3xl font-bold text-white mb-2">Database Query Analysis</h2>
                <p className="text-slate-400">Monitor and optimize database queries with adaptive indexing</p>
              </div>

              {analysisLoading ? (
                <div className="flex items-center justify-center py-20">
                  <Loader2 className="animate-spin text-red-500" size={40} />
                </div>
              ) : (
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                  {/* Current Indexes */}
                  <motion.div
                    initial={{ opacity: 0, x: -20 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: 0.1 }}
                  >
                    <Card className="bg-slate-800 border-slate-700 h-full">
                      <CardHeader>
                        <CardTitle className="text-white">Current Indexes</CardTitle>
                        <CardDescription className="text-slate-400">
                          {indexes.length} indexes active
                        </CardDescription>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-3 max-h-96 overflow-y-auto">
                          {indexes.length === 0 ? (
                            <p className="text-slate-400 text-sm">No indexes created yet</p>
                          ) : (
                            indexes.map((idx, i) => (
                              <motion.div
                                key={i}
                                initial={{ opacity: 0, x: -10 }}
                                animate={{ opacity: 1, x: 0 }}
                                className="p-3 bg-slate-700/50 rounded-lg border border-slate-600"
                              >
                                <p className="font-semibold text-white text-sm">{idx.index_name}</p>
                                <p className="text-xs text-slate-400">
                                  {idx.table_name}.{idx.column_name}
                                </p>
                                <p className="text-xs text-slate-500 mt-1">
                                  Size: {idx.size} • By: {idx.user_name}
                                </p>
                              </motion.div>
                            ))
                          )}
                        </div>
                      </CardContent>
                    </Card>
                  </motion.div>

                  {/* Recommendations */}
                  <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.2 }}
                    className="lg:col-span-2"
                  >
                    <Card className="bg-slate-800 border-slate-700">
                      <CardHeader>
                        <CardTitle className="text-white">Index Recommendations</CardTitle>
                        <CardDescription className="text-slate-400">
                          {recommendations.filter(r => r.recommend).length} recommended indexes
                        </CardDescription>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-3 max-h-96 overflow-y-auto">
                          {recommendations.length === 0 ? (
                            <p className="text-slate-400 text-sm">No data available yet. Run the workload!</p>
                          ) : recommendations.filter(r => r.recommend).length === 0 ? (
                            <p className="text-slate-400 text-sm">No recommendations at this time</p>
                          ) : (
                            recommendations.filter(r => r.recommend).map((rec, i) => (
                              <motion.button
                                key={i}
                                whileHover={{ scale: 1.02 }}
                                onClick={() => setSelectedRec(rec)}
                                className="w-full p-4 bg-slate-700/50 hover:bg-slate-700 rounded-lg border border-slate-600 hover:border-red-500/50 transition-all text-left"
                              >
                                <div className="flex items-start justify-between">
                                  <div className="flex-1">
                                    <p className="font-semibold text-white">
                                      {rec.table}.{rec.column}
                                    </p>
                                    <p className="text-xs text-slate-400 mt-1">
                                      {rec.calls} calls • {rec.avg_time_ms.toFixed(2)}ms avg
                                    </p>
                                  </div>
                                  <ChevronRight className="text-slate-500" size={18} />
                                </div>
                              </motion.button>
                            ))
                          )}
                        </div>
                      </CardContent>
                    </Card>
                  </motion.div>
                </div>
              )}

              {/* Recommendation Detail Modal */}
              <AnimatePresence>
                {selectedRec && (
                  <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center p-4 z-50"
                    onClick={() => setSelectedRec(null)}
                  >
                    <motion.div
                      initial={{ scale: 0.95 }}
                      animate={{ scale: 1 }}
                      onClick={(e) => e.stopPropagation()}
                      className="bg-slate-800 border border-slate-700 rounded-xl max-w-2xl w-full max-h-96 overflow-y-auto"
                    >
                      <div className="p-6 border-b border-slate-700 flex items-center justify-between">
                        <h3 className="text-2xl font-bold text-white">
                          {selectedRec.table}.{selectedRec.column}
                        </h3>
                        <button
                          onClick={() => setSelectedRec(null)}
                          className="text-slate-400 hover:text-white"
                        >
                          ✕
                        </button>
                      </div>
                      <div className="p-6 space-y-6">
                        <div>
                          <h4 className="text-slate-300 font-semibold mb-2">Sample Query</h4>
                          <pre className="bg-slate-700 p-3 rounded-lg text-slate-200 text-sm overflow-x-auto">
                            {selectedRec.sample_query}
                          </pre>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                          <div className="bg-slate-700/50 p-3 rounded-lg">
                            <p className="text-slate-400 text-xs">Query Calls</p>
                            <p className="text-2xl font-bold text-white">{selectedRec.calls}</p>
                          </div>
                          <div className="bg-slate-700/50 p-3 rounded-lg">
                            <p className="text-slate-400 text-xs">Avg Time</p>
                            <p className="text-2xl font-bold text-white">{selectedRec.avg_time_ms.toFixed(2)}ms</p>
                          </div>
                        </div>

                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => handleApplyIndex(selectedRec)}
                          disabled={applyingIndex || selectedRec.index_exists}
                          className={`w-full px-4 py-3 rounded-lg font-semibold transition-all ${
                            selectedRec.index_exists
                              ? 'bg-green-600/50 text-green-300 cursor-not-allowed'
                              : applyingIndex
                              ? 'bg-gray-600 text-gray-300'
                              : 'bg-red-500 hover:bg-red-600 text-white'
                          }`}
                        >
                          {selectedRec.index_exists ? (
                            '✓ Index Already Exists'
                          ) : applyingIndex ? (
                            <>
                              <Loader2 size={16} className="animate-spin inline mr-2" />
                              Creating Index...
                            </>
                          ) : (
                            'Apply Index'
                          )}
                        </motion.button>
                      </div>
                    </motion.div>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          )}
        </AnimatePresence>
      </main>
    </div>
  );
};

export default AdminDashboard;
