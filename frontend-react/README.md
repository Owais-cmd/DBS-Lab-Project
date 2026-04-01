# ShopHub Frontend - React with Shadcn/UI

A modern, sleek e-commerce frontend with micro-interactions, built for the DBS Lab Project with adaptive index advisor integration.

## Features

✨ **Modern UI**

- Built with React 18 + TypeScript
- Shadcn/UI components for consistency
- Tailwind CSS for styling
- Framer Motion for smooth animations
- Responsive design (mobile-first)

🔐 **Authentication**

- JWT-based authentication
- Role-based access control (User/Admin)
- Secure login and signup flows

🛒 **Shopping Features**

- Product browsing and filtering
- Add/remove items from cart
- Real-time cart updates
- Quantity management
- Order summary with tax calculation

👨‍💼 **Admin Dashboard**

- Product management (add/delete)
- Query performance analysis
- Adaptive index recommendations
- Index creation and monitoring

✨ **Micro-interactions**

- Smooth page transitions
- Hover effects on products
- Loading states
- Toast notifications (ready for implementation)
- Animated counters and badges

## Tech Stack

- **Frontend Framework**: React 18
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **UI Components**: Shadcn/UI with Radix UI
- **State Management**: Zustand
- **Routing**: React Router v6
- **Forms**: React Hook Form + Zod
- **Animations**: Framer Motion
- **API Client**: Axios
- **Build Tool**: Vite

## Installation

### Prerequisites

- Node.js 16+
- npm or yarn

### Setup

1. Navigate to the frontend directory:

```bash
cd frontend-react
```

2. Install dependencies:

```bash
npm install
```

3. Create environment file (.env):

```bash
VITE_API_URL=http://localhost:8000
```

## Development

Start the development server:

```bash
npm run dev
```

The app will be available at `http://localhost:5173`

## Building

Build for production:

```bash
npm run build
```

Preview production build:

```bash
npm run preview
```

## Project Structure

```
frontend-react/
├── src/
│   ├── components/       # Reusable components
│   ├── pages/           # Page components
│   │   ├── Landing.tsx     # Login/Signup page
│   │   ├── Shopping.tsx    # Product listing page
│   │   ├── Cart.tsx        # Shopping cart page
│   │   └── AdminDashboard.tsx  # Admin panel
│   ├── lib/
│   │   ├── ui/          # Shadcn UI components
│   │   └── utils.ts     # Utility functions
│   ├── store/           # Zustand store for state management
│   ├── hooks/           # Custom React hooks
│   ├── App.tsx          # Main app component
│   ├── main.tsx         # Entry point
│   └── index.css        # Global styles
├── public/              # Static assets
├── index.html          # HTML template
├── vite.config.ts      # Vite configuration
├── tailwind.config.js  # Tailwind configuration
├── tsconfig.json       # TypeScript configuration
└── package.json        # Dependencies

```

## Key Components

### Pages

**Landing.tsx**

- Login/Signup forms with validation
- Role selection (user/admin)
- JWT token management
- Smooth form transitions

**Shopping.tsx**

- Product grid with smooth animations
- Quantity selector
- Wishlist toggle (placeholder)
- Shopping cart summary
- Add to cart with loading states

**Cart.tsx**

- Cart item management
- Quantity controls
- Order summary with tax calculation
- Checkout process

**AdminDashboard.tsx**

- Product CRUD operations
- Query performance metrics
- Index recommendations
- Index creation and monitoring
- Two-tab interface: Products and Analysis

### State Management (Zustand)

**useAuthStore**

- User authentication state
- JWT token management
- Login/Signup/Logout functions
- Role-based access

**useCartStore**

- Shopping cart state
- Add/remove items
- Update quantities
- Cart persistence

**useProductStore**

- Product listing state
- CRUD operations
- Loading states

## API Integration

The frontend communicates with the FastAPI backend at `http://localhost:8000`:

### Authentication

- `POST /signup` - Create new account
- `POST /login` - User login

### Products

- `GET /products` - Fetch all products
- `POST /products` - Create product (admin)
- `DELETE /products/{id}` - Delete product (admin)

### Cart

- `GET /cart` - Get user's cart
- `POST /cart` - Add item to cart
- `PUT /cart/{product_id}` - Update quantity
- `DELETE /cart/{product_id}` - Remove from cart

### Analysis

- `GET /recommendations` - Get index recommendations
- `GET /indexes` - Get current indexes
- `POST /apply` - Apply index recommendation

## Micro-interactions Implemented

1. **Page Transitions** - Smooth fade and slide animations
2. **Button Feedback** - Scale on hover and tap
3. **Loading States** - Spinner icons during async operations
4. **Cart Badge** - Animated counter showing item count
5. **Form Validation** - Real-time error messages
6. **Product Cards** - Hover lift effect
7. **Quantity Controls** - Smooth increment/decrement
8. **Delete Animations** - Smooth exit animations

## Routing

```
/                    - Landing (Login/Signup)
/shopping           - Shopping page (protected)
/cart               - Shopping cart (protected)
/admin              - Admin dashboard (protected, admin only)
/dashboard          - Redirect to /shopping
```

## Environment Variables

Create `.env` file in `frontend-react/`:

```env
VITE_API_URL=http://localhost:8000
```

## Notes

- JWT tokens are stored in localStorage
- All API calls include authorization headers
- CORS is enabled on the backend for localhost:3000 and localhost:5173
- Cart changes are persisted to the database
- Admin-only routes are protected with role checking

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT
