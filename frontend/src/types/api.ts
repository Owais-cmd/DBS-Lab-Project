// API Types for Backend Integration

export interface User {
  id: number;
  email: string;
  name: string;
  city: string;
  is_admin: boolean;
  created_at: string;
}

export interface Item {
  id: number;
  name: string;
  description?: string;
  price: number;
  category?: string;
  image_url?: string;
  created_at: string;
}

export interface OrderItem {
  id: number;
  order_id: number;
  item_id: number;
  quantity: number;
  price: number;
}

export interface Order {
  id: number;
  user_id: number;
  status: string;
  total_amount: number;
  created_at: string;
  items: OrderItem[];
}

export interface SignupRequest {
  email: string;
  password: string;
  name: string;
  city: string;
}

export interface LoginRequest {
  email: string;
  password: string;
}

export interface AddToCartRequest {
  item_id: number;
  quantity: number;
}

export interface RemoveFromCartRequest {
  item_id: number;
}

export interface UpdateCartRequest {
  item_id: number;
  quantity: number;
}

export interface ItemCreate {
  name: string;
  description?: string;
  price: number;
  category?: string;
  image_url?: string;
}

export interface ItemUpdate {
  name?: string;
  description?: string;
  price?: number;
  category?: string;
  image_url?: string;
}

export interface MetricMostOrdered {
  item_id: number;
  name: string;
  category?: string;
  total_quantity: number;
  order_count: number;
}

export interface MetricExpensiveOrder {
  order_id: number;
  user_id: number;
  status: string;
  total_amount: number;
  created_at: string;
  item_count: number;
}

export interface CreateIndexRequest {
  table: string;
  column: string;
  force?: boolean;
}

export interface IndexRecommendation {
  table: string;
  column: string;
  calls: number;
  avg_time_ms: number;
  index_exists: boolean;
  recommend: boolean;
  sample_query?: string;
}

export interface IndexInfo {
  index_name: string;
  table_name: string;
  column_name: string;
  created_at: string;
  user_name: string;
  size: string;
}

export interface TopCustomer {
  user_id: number;
  user_name: string;
  user_email: string;
  total_spent: number;
  order_count: number;
}

export interface RecentOrder {
  order_id: number;
  user_name: string;
  user_email: string;
  total_amount: number;
  status: string;
  created_at: string;
}

export interface MetricsResponse {
  total_users: number;
  total_items: number;
  total_orders: number;
  total_revenue: number;
  top_customers: TopCustomer[];
  recent_orders: RecentOrder[];
}
