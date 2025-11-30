import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { User, Order, OrderItem } from '../types/api';
import { api } from '../services/api';

interface AppState {
  // Auth state
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  
  // Cart state
  cart: OrderItem[];
  cartOrder: Order | null;
  
  // Actions
  login: (email: string, password: string) => Promise<void>;
  signup: (email: string, password: string, name: string, city: string) => Promise<void>;
  logout: () => Promise<void>;
  checkAuth: () => Promise<void>;
  
  addToCart: (itemId: number, quantity: number) => Promise<void>;
  removeFromCart: (itemId: number) => Promise<void>;
  updateCartItem: (itemId: number, quantity: number) => Promise<void>;
  placeOrder: () => Promise<Order>;
  clearCart: () => void;
  refreshCart: () => Promise<void>;
}

export const useStore = create<AppState>()(
  persist(
    (set, get) => ({
      // Initial state
      user: null,
      isAuthenticated: false,
      isLoading: false,
      cart: [],
      cartOrder: null,

      // Auth actions
      login: async (email: string, password: string) => {
        set({ isLoading: true });
        try {
          await api.auth.login({ email, password });
          const user = await api.auth.getCurrentUser();
          set({ user, isAuthenticated: true, isLoading: false });
          await get().refreshCart();
        } catch (error) {
          set({ isLoading: false });
          throw error;
        }
      },

      signup: async (email: string, password: string, name: string, city: string) => {
        set({ isLoading: true });
        try {
          await api.auth.signup({ email, password, name, city });
          await get().login(email, password);
        } catch (error) {
          set({ isLoading: false });
          throw error;
        }
      },

      logout: async () => {
        try {
          await api.auth.logout();
        } catch (error) {
          console.error('Logout error:', error);
        } finally {
          set({ user: null, isAuthenticated: false, cart: [], cartOrder: null });
        }
      },

      checkAuth: async () => {
        try {
          const user = await api.auth.getCurrentUser();
          set({ user, isAuthenticated: true });
          await get().refreshCart();
        } catch (error) {
          set({ user: null, isAuthenticated: false, cart: [], cartOrder: null });
        }
      },

      // Cart actions
      addToCart: async (itemId: number, quantity: number) => {
        try {
          const order = await api.cart.addToCart({ item_id: itemId, quantity });
          set({ cartOrder: order, cart: order.items });
        } catch (error) {
          throw error;
        }
      },

      removeFromCart: async (itemId: number) => {
        try {
          const order = await api.cart.removeFromCart({ item_id: itemId });
          set({ cartOrder: order, cart: order.items });
        } catch (error) {
          throw error;
        }
      },

      updateCartItem: async (itemId: number, quantity: number) => {
        try {
          const order = await api.cart.updateCartItem({ item_id: itemId, quantity });
          set({ cartOrder: order, cart: order.items });
        } catch (error) {
          throw error;
        }
      },

      placeOrder: async () => {
        const order = await api.orders.placeOrder();
        set({ cart: [], cartOrder: null });
        return order;
      },

      clearCart: () => {
        set({ cart: [], cartOrder: null });
      },

      refreshCart: async () => {
        try {
          const cart = await api.cart.getCart();
          if (cart && cart.items && cart.items.length > 0) {
            set({ cart: cart.items, cartOrder: cart });
          } else {
            set({ cart: [], cartOrder: null });
          }
        } catch (error) {
          console.error('Failed to refresh cart:', error);
          set({ cart: [], cartOrder: null });
        }
      }
    }),
    {
      name: 'app-storage',
      partialize: (state) => ({
        user: state.user,
        isAuthenticated: state.isAuthenticated
      })
    }
  )
);
