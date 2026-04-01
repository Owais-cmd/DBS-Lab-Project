import { create } from 'zustand';
import axios from 'axios';

export interface Product {
  id: number;
  name: string;
  price: number;
  description: string;
  image_url?: string;
}

export interface CartItem extends Product {
  quantity: number;
}

export interface User {
  id: number;
  email: string;
  role: 'user' | 'admin';
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  signup: (email: string, password: string, role?: string) => Promise<void>;
  logout: () => void;
  setUser: (user: User | null) => void;
}

interface CartState {
  items: CartItem[];
  addToCart: (product: Product, quantity: number) => Promise<void>;
  removeFromCart: (productId: number) => Promise<void>;
  updateQuantity: (productId: number, quantity: number) => Promise<void>;
  clearCart: () => void;
  fetchCart: () => Promise<void>;
}

interface ProductState {
  products: Product[];
  addProduct: (name: string, price: number, description: string, image_url?: string) => Promise<void>;
  updateProduct: (id: number, name: string, price: number, description: string, image_url?: string) => Promise<void>;
  deleteProduct: (id: number) => Promise<void>;
  fetchProducts: () => Promise<void>;
  loading: boolean;
}

const API_BASE = 'http://localhost:8000';

export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  token: localStorage.getItem('token') || null,
  isAuthenticated: !!localStorage.getItem('token'),
  
  login: async (email, password) => {
    try {
      const response = await axios.post(`${API_BASE}/login`, { email, password });
      const { access_token, user } = response.data;
      localStorage.setItem('token', access_token);
      set({ user, token: access_token, isAuthenticated: true });
      axios.defaults.headers.common['Authorization'] = `Bearer ${access_token}`;
    } catch (error) {
      throw error;
    }
  },
  
  signup: async (email, password, role = 'user') => {
    try {
      const response = await axios.post(`${API_BASE}/signup`, { email, password, role });
      const { access_token, user } = response.data;
      localStorage.setItem('token', access_token);
      set({ user, token: access_token, isAuthenticated: true });
      axios.defaults.headers.common['Authorization'] = `Bearer ${access_token}`;
    } catch (error) {
      throw error;
    }
  },
  
  logout: () => {
    localStorage.removeItem('token');
    set({ user: null, token: null, isAuthenticated: false });
    delete axios.defaults.headers.common['Authorization'];
  },
  
  setUser: (user) => set({ user }),
}));

export const useCartStore = create<CartState>((set) => ({
  items: [],
  
  addToCart: async (product, quantity) => {
    try {
      const token = localStorage.getItem('token');
      await axios.post(
        `${API_BASE}/cart`,
        { product_id: product.id, quantity },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      set((state) => {
        const existing = state.items.find(item => item.id === product.id);
        if (existing) {
          return {
            items: state.items.map(item =>
              item.id === product.id
                ? { ...item, quantity: item.quantity + quantity }
                : item
            ),
          };
        }
        return { items: [...state.items, { ...product, quantity }] };
      });
    } catch (error) {
      throw error;
    }
  },
  
  removeFromCart: async (productId) => {
    try {
      const token = localStorage.getItem('token');
      await axios.delete(`${API_BASE}/cart/${productId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      set((state) => ({
        items: state.items.filter(item => item.id !== productId),
      }));
    } catch (error) {
      throw error;
    }
  },
  
  updateQuantity: async (productId, quantity) => {
    try {
      const token = localStorage.getItem('token');
      await axios.put(
        `${API_BASE}/cart/${productId}`,
        { quantity },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      set((state) => ({
        items: state.items.map(item =>
          item.id === productId ? { ...item, quantity } : item
        ),
      }));
    } catch (error) {
      throw error;
    }
  },
  
  clearCart: () => set({ items: [] }),
  
  fetchCart: async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await axios.get(`${API_BASE}/cart`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      set({ items: response.data.items });
    } catch (error) {
      throw error;
    }
  },
}));

export const useProductStore = create<ProductState>((set) => ({
  products: [],
  loading: false,
  
  fetchProducts: async () => {
    set({ loading: true });
    try {
      const response = await axios.get(`${API_BASE}/products`);
      set({ products: response.data, loading: false });
    } catch (error) {
      set({ loading: false });
      throw error;
    }
  },
  
  addProduct: async (name, price, description, image_url) => {
    try {
      const token = localStorage.getItem('token');
      const response = await axios.post(
        `${API_BASE}/products`,
        { name, price, description, image_url },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      set((state) => ({
        products: [...state.products, response.data],
      }));
    } catch (error) {
      throw error;
    }
  },
  
  deleteProduct: async (id) => {
    try {
      const token = localStorage.getItem('token');
      await axios.delete(`${API_BASE}/products/${id}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      set((state) => ({
        products: state.products.filter(p => p.id !== id),
      }));
    } catch (error) {
      throw error;
    }
  },

  updateProduct: async (id, name, price, description, image_url) => {
    try {
      const token = localStorage.getItem('token');
      const response = await axios.put(
        `${API_BASE}/products/${id}`,
        { name, price, description, image_url },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      set((state) => ({
        products: state.products.map(p => p.id === id ? response.data : p),
      }));
    } catch (error) {
      throw error;
    }
  },
}));
