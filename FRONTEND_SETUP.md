# Frontend Setup & Deployment Guide

## Quick Start

### Prerequisites

- Node.js 16+ and npm installed
- Backend running on http://localhost:8000
- PostgreSQL database setup

### Installation

1. Navigate to frontend directory:

```bash
cd frontend-react
```

2. Install dependencies:

```bash
npm install
```

3. Start development server:

```bash
npm run dev
```

The frontend will be available at `http://localhost:5173`

## Project Structure

```
frontend-react/
├── src/
│   ├── components/          # Reusable UI components
│   ├── pages/              # Page components
│   │   ├── Landing.tsx     # Login/Signup
│   │   ├── Shopping.tsx    # Product listing
│   │   ├── Cart.tsx        # Shopping cart
│   │   └── AdminDashboard.tsx  # Admin panel
│   ├── lib/
│   │   ├── ui/            # Shadcn UI components (Button, Input, etc.)
│   │   └── utils.ts       # Utility functions
│   ├── store/             # Zustand state management
│   │   └── index.ts       # Auth, Cart, Product stores
│   ├── context/           # React context (if needed)
│   ├── hooks/             # Custom React hooks
│   ├── App.tsx            # Main app with routing
│   ├── main.tsx           # React entry point
│   └── index.css          # Global styles
├── public/                # Static assets
├── index.html            # HTML template
├── vite.config.ts        # Vite configuration
├── tailwind.config.js    # Tailwind CSS config
├── tsconfig.json         # TypeScript config
├── postcss.config.js     # PostCSS config
└── package.json          # Dependencies

```

## Development

### Run Development Server

```bash
npm run dev
```

### Build for Production

```bash
npm run build
```

### Preview Production Build

```bash
npm run preview
```

## Features Overview

### 1. Authentication (Landing Page)

- **Login** - Email/password authentication
- **Signup** - Create account with role selection (user/admin)
- **JWT** - Secure token-based authentication
- **Form Validation** - Zod schema validation

### 2. Shopping Page

- **Product Grid** - Browse all products
- **Wishlist** - Mark products as favorites (UI ready)
- **Quantity Control** - Adjust quantity before adding to cart
- **Add to Cart** - Real-time cart updates
- **Micro-interactions** - Smooth animations and transitions

### 3. Shopping Cart

- **Cart Summary** - View all items with prices
- **Quantity Management** - Update or remove items
- **Order Calculation** - Subtotal, tax, and total
- **Checkout** - Proceed to payment (simulation)
- **Continue Shopping** - Easy navigation back to products

### 4. Admin Dashboard

- **Two-Tab Interface**
  - Products: CRUD operations
  - Analysis: Query performance metrics

#### Product Management

- **Add Product** - Form to create new products
- **Delete Product** - Remove products from catalog
- **List View** - All products with details

#### Query Analysis

- **Current Indexes** - View active database indexes
- **Recommendations** - AI-powered index suggestions
- **Apply Index** - Create indexes with one click
- **Performance Metrics** - Query calls and execution time

## Styling & Design

### Tailwind CSS

- Utility-first CSS framework
- Dark mode support (configured)
- Custom color scheme (red-based primary)
- Responsive design (mobile-first)

### Shadcn/UI Components

- **Button** - Multiple variants (default, outline, ghost, etc.)
- **Input** - Form inputs with validation
- **Card** - Reusable card containers
- **Badge** - Status indicators
- **Label** - Form labels

### Color Scheme

```css
Primary: Red (#EF4444)
Secondary: Slate (gray tones)
Accent: Red (#EF4444)
Background: Light/Dark modes supported
```

## Animations & Micro-interactions

### Framer Motion Effects

1. **Page Transitions** - Fade and slide animations
2. **Button Interactions** - Scale on hover/tap
3. **Cart Animation** - Badge counter animation
4. **Loading States** - Spinner during async operations
5. **Product Cards** - Lift effect on hover
6. **Form Errors** - Smooth error message animations
7. **Modal Transitions** - Smooth open/close
8. **List Items** - Staggered entrance animations

### Example Interactions

```typescript
// Hover scale effect
whileHover={{ scale: 1.05 }}
whileTap={{ scale: 0.95 }}

// Fade and slide
initial={{ opacity: 0, y: 20 }}
animate={{ opacity: 1, y: 0 }}

// Staggered list
transition={{ delay: index * 0.1 }}
```

## State Management (Zustand)

### Auth Store

```typescript
useAuthStore()
├── user (User | null)
├── token (string | null)
├── isAuthenticated (boolean)
├── login(email, password)
├── signup(email, password, role)
└── logout()
```

### Cart Store

```typescript
useCartStore()
├── items (CartItem[])
├── addToCart(product, quantity)
├── removeFromCart(productId)
├── updateQuantity(productId, quantity)
├── clearCart()
└── fetchCart()
```

### Product Store

```typescript
useProductStore()
├── products (Product[])
├── loading (boolean)
├── fetchProducts()
├── addProduct(name, price, description)
└── deleteProduct(id)
```

## API Integration

### Base URL

```
http://localhost:8000
```

### Key Endpoints

**Auth**

```
POST /signup     - Register
POST /login      - Login
```

**Products**

```
GET  /products           - List all
POST /products           - Create (admin)
DELETE /products/{id}    - Delete (admin)
```

**Cart**

```
GET  /cart                    - Get user cart
POST /cart                    - Add item
PUT  /cart/{product_id}       - Update quantity
DELETE /cart/{product_id}     - Remove item
```

**Analysis**

```
GET  /recommendations         - Get suggestions
GET  /indexes                 - Get indexes
POST /apply                   - Apply index (admin)
```

## Environment Configuration

### Development (.env)

```env
VITE_API_URL=http://localhost:8000
```

### Production (.env.production)

```env
VITE_API_URL=https://api.yourdomain.com
```

## Building & Deployment

### Build for Production

```bash
npm run build
```

This creates an optimized build in the `dist/` folder.

### Deploy to Vercel

```bash
npm i -g vercel
vercel
```

### Deploy to Netlify

```bash
npm i -g netlify-cli
netlify deploy --prod --dir dist
```

### Docker Deployment

```dockerfile
# Build stage
FROM node:18-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Production stage
FROM node:18-alpine
WORKDIR /app
RUN npm install -g serve
COPY --from=build /app/dist ./dist
EXPOSE 3000
CMD ["serve", "-s", "dist", "-l", "3000"]
```

Build and run:

```bash
docker build -t shophub-frontend .
docker run -p 3000:3000 shophub-frontend
```

## Performance Optimization

1. **Code Splitting** - Automatic with Vite
2. **Lazy Loading** - React.lazy for routes
3. **Image Optimization** - Placeholder gradients
4. **Bundle Analysis** - `npm run build` shows sizes
5. **CSS Purging** - Tailwind removes unused styles

## Testing

### Add Testing (Future Enhancement)

```bash
npm install --save-dev vitest @testing-library/react
```

## Troubleshooting

### Common Issues

**1. API Connection Errors**

- Check backend is running on port 8000
- Verify CORS settings in backend
- Check VITE_API_URL in environment

**2. Authentication Issues**

- Ensure JWT secret key matches backend
- Check token storage in localStorage
- Verify token format in Authorization header

**3. Cart Not Updating**

- Check user is authenticated
- Verify product exists in database
- Check network tab for API errors

**4. Build Errors**

- Clear node_modules: `rm -rf node_modules && npm install`
- Clear cache: `npm cache clean --force`
- Check TypeScript errors: `npx tsc --noEmit`

## Browser DevTools

### Debugging Tips

1. **Network Tab** - Monitor API calls
2. **Application Tab** - Check localStorage for tokens
3. **Console** - View error messages
4. **React DevTools** - Inspect component hierarchy
5. **Redux/Zustand DevTools** - Monitor state changes

## Useful Commands

```bash
# Install dependencies
npm install

# Start dev server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview

# Type check
npx tsc --noEmit

# List all npm scripts
npm run
```

## File Naming Conventions

- **Components**: PascalCase (e.g., `CartItem.tsx`)
- **Pages**: PascalCase (e.g., `Shopping.tsx`)
- **Utilities**: camelCase (e.g., `utils.ts`)
- **Stores**: camelCase with "Store" suffix (e.g., `authStore.ts`)
- **CSS/Tailwind**: Inline in components

## Git Workflow

```bash
# Create feature branch
git checkout -b feature/cart-improvements

# Make changes and commit
git add .
git commit -m "feat: improve cart experience"

# Push to remote
git push origin feature/cart-improvements

# Create pull request
```

## Performance Monitoring

Monitor with:

- Chrome DevTools Lighthouse
- WebPageTest
- GTmetrix

Target metrics:

- First Contentful Paint: < 1.5s
- Largest Contentful Paint: < 2.5s
- Cumulative Layout Shift: < 0.1
- Time to Interactive: < 3.5s

## Security Notes

✅ **Implemented**

- JWT authentication
- Password hashing (backend)
- CORS protection
- XSS protection (React)

⚠️ **For Production**

- Enable HTTPS only
- Use secure cookies
- Implement rate limiting
- Add CSP headers
- Regular security audits

## Contributing

1. Fork repository
2. Create feature branch
3. Make changes with clear commits
4. Push and create pull request
5. Wait for review and merge

## License

MIT - See LICENSE file for details
