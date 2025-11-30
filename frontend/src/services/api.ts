// API Service for Backend Integration
import type {
  User,
  Item,
  Order,
  SignupRequest,
  LoginRequest,
  AddToCartRequest,
  RemoveFromCartRequest,
  UpdateCartRequest,
  ItemCreate,
  ItemUpdate,
  MetricsResponse,
  IndexRecommendation,
  IndexInfo,
  CreateIndexRequest
} from '../types/api';

const API_BASE_URL = 'http://localhost:8000';

class ApiService {
  // Auth methods
  auth = {
    signup: async (data: SignupRequest): Promise<{ msg: string; user_id: number }> => {
      const response = await fetch(`${API_BASE_URL}/auth/signup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Signup failed');
      }
      return response.json();
    },

    login: async (data: LoginRequest): Promise<{ msg: string }> => {
      const response = await fetch(`${API_BASE_URL}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Login failed');
      }
      return response.json();
    },

    logout: async (): Promise<{ msg: string }> => {
      const response = await fetch(`${API_BASE_URL}/auth/logout`, {
        method: 'POST',
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Logout failed');
      return response.json();
    },

    getCurrentUser: async (): Promise<User> => {
      const response = await fetch(`${API_BASE_URL}/users/me`, {
        credentials: 'include'
      });
      if (!response.ok) {
        if (response.status === 401) throw new Error('Not authenticated');
        throw new Error('Failed to fetch user');
      }
      return response.json();
    }
  };

  // Items methods
  items = {
    getAll: async (params?: {
      search?: string;
      category?: string;
      limit?: number;
      offset?: number;
    }): Promise<Item[]> => {
      const queryParams = new URLSearchParams();
      if (params?.search) queryParams.append('search', params.search);
      if (params?.category) queryParams.append('category', params.category);
      if (params?.limit) queryParams.append('limit', params.limit.toString());
      if (params?.offset) queryParams.append('offset', params.offset.toString());
      
      const response = await fetch(`${API_BASE_URL}/items?${queryParams}`);
      if (!response.ok) throw new Error('Failed to fetch items');
      return response.json();
    },

    getById: async (id: number): Promise<Item> => {
      const response = await fetch(`${API_BASE_URL}/items/${id}`);
      if (!response.ok) throw new Error('Failed to fetch item');
      return response.json();
    },

    create: async (data: ItemCreate): Promise<Item> => {
      const response = await fetch(`${API_BASE_URL}/items`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to create item');
      }
      return response.json();
    },

    update: async (id: number, data: ItemUpdate): Promise<Item> => {
      const response = await fetch(`${API_BASE_URL}/items/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to update item');
      }
      return response.json();
    },

    delete: async (id: number): Promise<void> => {
      const response = await fetch(`${API_BASE_URL}/items/${id}`, {
        method: 'DELETE',
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to delete item');
      }
      // 204 No Content - successful deletion
    }
  };

  // Cart methods
  cart = {
    getCart: async (): Promise<Order> => {
      const response = await fetch(`${API_BASE_URL}/cart`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch cart');
      return response.json();
    },

    addToCart: async (data: AddToCartRequest): Promise<Order> => {
      const response = await fetch(`${API_BASE_URL}/cart/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to add to cart');
      }
      return response.json();
    },

    removeFromCart: async (data: RemoveFromCartRequest): Promise<Order> => {
      const response = await fetch(`${API_BASE_URL}/cart/remove`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to remove from cart');
      }
      return response.json();
    },

    updateCartItem: async (data: UpdateCartRequest): Promise<Order> => {
      const response = await fetch(`${API_BASE_URL}/cart/update`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to update cart item');
      }
      return response.json();
    }
  };

  // Orders methods
  orders = {
    getUserOrders: async (limit: number = 5): Promise<Order[]> => {
      const response = await fetch(`${API_BASE_URL}/users/me/orders?limit=${limit}`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch orders');
      return response.json();
    },

    placeOrder: async (): Promise<Order> => {
      const response = await fetch(`${API_BASE_URL}/orders/place`, {
        method: 'POST',
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to place order');
      }
      return response.json();
    },

    cancelOrder: async (orderId: number): Promise<{ msg: string; order_id: number }> => {
      const response = await fetch(`${API_BASE_URL}/orders/${orderId}`, {
        method: 'DELETE',
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to cancel order');
      }
      return response.json();
    },

    markDelivered: async (orderId: number): Promise<Order> => {
      const response = await fetch(`${API_BASE_URL}/orders/${orderId}/delivered`, {
        method: 'PATCH',
        credentials: 'include'
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to mark order as delivered');
      }
      return response.json();
    }
  };

  // Metrics methods (Admin)
  metrics = {
    getMetrics: async (): Promise<MetricsResponse> => {
      const response = await fetch(`${API_BASE_URL}/metrics`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch metrics');
      return response.json();
    }
  };

  // Indexes methods (Admin)
  indexes = {
    getRecommendations: async (): Promise<IndexRecommendation[]> => {
      const response = await fetch(`${API_BASE_URL}/indexes/recommendations`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch recommendations');
      return response.json();
    },

    getIndexes: async (): Promise<IndexInfo[]> => {
      const response = await fetch(`${API_BASE_URL}/indexes/list`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to list indexes');
      return response.json();
    },

    applyIndex: async (data: CreateIndexRequest): Promise<any> => {
      const response = await fetch(`${API_BASE_URL}/indexes/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to create index');
      return response.json();
    },

    getComparison: async (indexName: string): Promise<any> => {
      const response = await fetch(`${API_BASE_URL}/indexes/comparison/${indexName}`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to get index comparison');
      return response.json();
    }
  };
}

export const api = new ApiService();
