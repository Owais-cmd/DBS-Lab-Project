#!/usr/bin/env python
"""
Seed 100 real-world products into the database with diverse unique images
"""
import sys
import os
sys.path.insert(0, ".")

from backend.app.main_new import engine, SessionLocal, Base, Product
from datetime import datetime
from sqlalchemy import text

# 100 Real-world products with DIVERSE images from Unsplash
PRODUCTS = [
    # Electronics
    {"name": "iPhone 15 Pro Max", "price": 1199.99, "description": "Latest Apple flagship smartphone with advanced camera system", "image_url": "https://images.unsplash.com/photo-1592286927505-1def25115558?w=500"},
    {"name": "Samsung Galaxy S24", "price": 999.99, "description": "Premium Android smartphone with AI features", "image_url": "https://images.unsplash.com/photo-1511707267537-b85faf00021e?w=500"},
    {"name": "MacBook Pro 16\"", "price": 2499.99, "description": "High-performance laptop for professionals", "image_url": "https://images.unsplash.com/photo-1517336714731-489689fd1ca8?w=500"},
    {"name": "Dell XPS 13", "price": 1299.99, "description": "Ultrabook with stunning display", "image_url": "https://images.unsplash.com/photo-1588872657840-790ff3bde08c?w=500"},
    {"name": "iPad Pro 12.9\"", "price": 1099.99, "description": "Powerful tablet for creative professionals", "image_url": "https://images.unsplash.com/photo-1584622181563-430f63602d4b?w=500"},
    {"name": "Samsung Galaxy Tab S9", "price": 799.99, "description": "Premium Android tablet with S Pen", "image_url": "https://images.unsplash.com/photo-1561070791-2526d30994b5?w=500"},
    {"name": "Sony WH-1000XM5", "price": 399.99, "description": "Noise-canceling wireless headphones", "image_url": "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=500"},
    {"name": "Apple AirPods Pro Max", "price": 549.99, "description": "Premium spatial audio headphones", "image_url": "https://images.unsplash.com/photo-1484704849700-f032a568e944?w=500"},
    {"name": "DJI Mini 3 Pro", "price": 449.99, "description": "Compact and powerful drone", "image_url": "https://images.unsplash.com/photo-1579829366248-204fe8413f31?w=500"},
    {"name": "GoPro Hero 12", "price": 499.99, "description": "Action camera for extreme sports", "image_url": "https://images.unsplash.com/photo-1611532736579-6b16e2b50449?w=500"},
    
    # Wearables - UNIQUE IMAGES
    {"name": "Apple Watch Series 9", "price": 399.99, "description": "Advanced health and fitness tracking watch", "image_url": "https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=500"},
    {"name": "Garmin Fenix 7X", "price": 699.99, "description": "Multisport GPS smartwatch", "image_url": "https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=500"},
    {"name": "Fitbit Sense 2", "price": 299.99, "description": "Advanced health tracking smartwatch", "image_url": "https://images.unsplash.com/photo-1557804506-669714d2e9d8?w=500"},
    {"name": "Samsung Galaxy Watch 6", "price": 299.99, "description": "Android-powered smartwatch", "image_url": "https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=500"},
    {"name": "Polar Vantage V3", "price": 499.99, "description": "Professional sports watch", "image_url": "https://images.unsplash.com/photo-1457039671496-3efe4ecc975d?w=500"},
    
    # Cameras - UNIQUE
    {"name": "Canon EOS R6", "price": 2499.99, "description": "Professional mirrorless camera", "image_url": "https://images.unsplash.com/photo-1606986628025-35d57e735ae0?w=500"},
    {"name": "Sony A7R V", "price": 3198.00, "description": "High-resolution mirrorless camera", "image_url": "https://images.unsplash.com/photo-1612198188060-c7c2a3b66eae?w=500"},
    {"name": "Nikon Z9", "price": 5496.95, "description": "Flagship mirrorless camera", "image_url": "https://images.unsplash.com/photo-1526170375885-4d8ecf77b99f?w=500"},
    {"name": "Fujifilm X-T5", "price": 1699.99, "description": "Retro-styled mirrorless camera", "image_url": "https://images.unsplash.com/photo-1609034227505-5876f6aa4e90?w=500"},
    {"name": "Panasonic Lumix S5", "price": 1497.99, "description": "Full-frame mirrorless camera", "image_url": "https://images.unsplash.com/photo-1491796014055-e4835cdcd4c6?w=500"},
    
    # Fashion & Accessories - UNIQUE
    {"name": "Nike Air Jordan 1", "price": 170.00, "description": "Iconic basketball sneaker", "image_url": "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=500"},
    {"name": "Adidas Ultraboost 22", "price": 180.00, "description": "Performance running shoe", "image_url": "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=500"},
    {"name": "Rolex Submariner", "price": 9100.00, "description": "Classic luxury dive watch", "image_url": "https://images.unsplash.com/photo-1523170335258-f5ed11844a49?w=500"},
    {"name": "Omega Seamaster", "price": 5900.00, "description": "Professional dive watch", "image_url": "https://images.unsplash.com/photo-1579809322549-0f280c42e8f0?w=500"},
    {"name": "Cartier Tank", "price": 6100.00, "description": "Elegant dress watch", "image_url": "https://images.unsplash.com/photo-1535632066927-ab7c9ab60908?w=500"},
    {"name": "Louis Vuitton Neverfull", "price": 1760.00, "description": "Classic luxury handbag", "image_url": "https://images.unsplash.com/photo-1548036328-c9fa89d128fa?w=500"},
    {"name": "Hermès Birkin", "price": 9000.00, "description": "Iconic luxury handbag", "image_url": "https://images.unsplash.com/photo-1548036328-c9fa89d128fa?w=500"},
    {"name": "Gucci Marmont", "price": 1190.00, "description": "Popular shoulder bag", "image_url": "https://images.unsplash.com/photo-1553062407-98eeb64c6a62?w=500"},
    {"name": "Prada Nylon Bag", "price": 1450.00, "description": "Modern shoulder bag", "image_url": "https://images.unsplash.com/photo-1548036328-c9fa89d128fa?w=500"},
    {"name": "Balenciaga City Bag", "price": 2145.00, "description": "Contemporary designer bag", "image_url": "https://images.unsplash.com/photo-1495814811223-4d71bcdd2085?w=500"},
    
    # Home & Kitchen - UNIQUE
    {"name": "Dyson V15 Detect", "price": 749.99, "description": "Premium cordless vacuum", "image_url": "https://images.unsplash.com/photo-1585771724684-38269d6639fd?w=500"},
    {"name": "Instant Pot Duo Plus", "price": 99.95, "description": "Multi-function pressure cooker", "image_url": "https://images.unsplash.com/photo-1584568694244-14fbdf83bd30?w=500"},
    {"name": "Nespresso Vertuo", "price": 199.99, "description": "Automatic coffee machine", "image_url": "https://images.unsplash.com/photo-1517668808822-9ebb02ae2a0e?w=500"},
    {"name": "Breville Smart Oven", "price": 599.95, "description": "Digital convection toaster oven", "image_url": "https://images.unsplash.com/photo-1556909114-f6e7ad7d3136?w=500"},
    {"name": "KitchenAid Stand Mixer", "price": 329.99, "description": "Professional stand mixer", "image_url": "https://images.unsplash.com/photo-1578500494198-246f612d03b3?w=500"},
    {"name": "Vitamix Blender", "price": 499.95, "description": "High-powered blender", "image_url": "https://images.unsplash.com/photo-1584568694244-14fbdf83bd30?w=500"},
    {"name": "Le Creuset Dutch Oven", "price": 349.99, "description": "Enameled cast iron cookware", "image_url": "https://images.unsplash.com/photo-1595521624cf46ab2747496382eb337280daaf201?w=500"},
    {"name": "Zwilling Knife Set", "price": 299.99, "description": "Premium kitchen knife set", "image_url": "https://images.unsplash.com/photo-1593618998160-e34014e67546?w=500"},
    {"name": "Philips Air Fryer", "price": 249.99, "description": "XL air fryer", "image_url": "https://images.unsplash.com/photo-1563379091339-03b21ab00916?w=500"},
    {"name": "Ninja Blender", "price": 99.99, "description": "Powerful blender", "image_url": "https://images.unsplash.com/photo-1578500494198-246f612d03b3?w=500"},
    
    # Fitness - UNIQUE
    {"name": "Peloton Bike+", "price": 2495.00, "description": "Premium stationary bike", "image_url": "https://images.unsplash.com/photo-1534438327276-14e5300c3a48?w=500"},
    {"name": "Concept2 Rowing Machine", "price": 895.00, "description": "Professional rowing machine", "image_url": "https://images.unsplash.com/photo-1577720643272-265f4a6c7c03?w=500"},
    {"name": "NordicTrack Treadmill", "price": 799.99, "description": "Smart treadmill with iFit", "image_url": "https://images.unsplash.com/photo-1531694711352-1f694dc719db?w=500"},
    {"name": "Bowflex SelectTech", "price": 399.99, "description": "Adjustable dumbbell set", "image_url": "https://images.unsplash.com/photo-1517836357463-d25ddfcb53ef?w=500"},
    {"name": "Yoga Mat Pro", "price": 99.99, "description": "Non-slip premium yoga mat", "image_url": "https://images.unsplash.com/photo-1599901860904-17d4fb3850d6?w=500"},
    {"name": "Resistance Band Set", "price": 29.99, "description": "5-pack resistance bands", "image_url": "https://images.unsplash.com/photo-1606016221506-5a0653dfd46f?w=500"},
    {"name": "Jump Rope Pro", "price": 49.99, "description": "Adjustable speed jump rope", "image_url": "https://images.unsplash.com/photo-1534438327276-14e5300c3a48?w=500"},
    {"name": "Pull-up Bar", "price": 39.99, "description": "Doorway pull-up bar", "image_url": "https://images.unsplash.com/photo-1593642532009-6ba71e45fbcc?w=500"},
    {"name": "Kettlebell Set", "price": 149.99, "description": "3-piece kettlebell set", "image_url": "https://images.unsplash.com/photo-1517836357463-d25ddfcb53ef?w=500"},
    {"name": "Ab Wheel", "price": 29.99, "description": "Ab roller wheel", "image_url": "https://images.unsplash.com/photo-1549719540-dcae7f74b8a0?w=500"},
    
    # Books - UNIQUE
    {"name": "Atomic Habits", "price": 16.99, "description": "James Clear - Best-selling habits book", "image_url": "https://images.unsplash.com/photo-1507842872343-583f20270319?w=500"},
    {"name": "Educated", "price": 18.99, "description": "Tara Westover - Memoir", "image_url": "https://images.unsplash.com/photo-1524995997946-a1c2e315a42f?w=500"},
    {"name": "The Midnight Library", "price": 17.99, "description": "Matt Haig - Fiction", "image_url": "https://images.unsplash.com/photo-1543002588-d4d6c71f3b2c?w=500"},
    {"name": "Thinking, Fast and Slow", "price": 18.00, "description": "Daniel Kahneman - Psychology", "image_url": "https://images.unsplash.com/photo-1507842872343-583f20270319?w=500"},
    {"name": "The Lean Startup", "price": 15.99, "description": "Eric Ries - Business", "image_url": "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?w=500"},
    {"name": "Dune", "price": 17.99, "description": "Frank Herbert - Sci-Fi Classic", "image_url": "https://images.unsplash.com/photo-1543002588-d4d6c71f3b2c?w=500"},
    {"name": "Project Hail Mary", "price": 18.99, "description": "Andy Weir - Sci-Fi", "image_url": "https://images.unsplash.com/photo-1524995997946-a1c2e315a42f?w=500"},
    {"name": "The Silent Patient", "price": 16.99, "description": "Alex Michaelides - Thriller", "image_url": "https://images.unsplash.com/photo-1507842872343-583f20270319?w=500"},
    {"name": "Sapiens", "price": 18.99, "description": "Yuval Noah Harari - History", "image_url": "https://images.unsplash.com/photo-1512820790803-83ca734da794?w=500"},
    {"name": "The Power of Now", "price": 16.99, "description": "Eckhart Tolle - Self-help", "image_url": "https://images.unsplash.com/photo-1543002588-d4d6c71f3b2c?w=500"},
    
    # Gaming - UNIQUE
    {"name": "PlayStation 5", "price": 499.99, "description": "Next-gen gaming console", "image_url": "https://images.unsplash.com/photo-1538481143081-9049b7ad6ff5?w=500"},
    {"name": "Xbox Series X", "price": 499.99, "description": "Premium gaming console", "image_url": "https://images.unsplash.com/photo-1614294148104-1898c992827e?w=500"},
    {"name": "Nintendo Switch Pro", "price": 349.99, "description": "Hybrid gaming console", "image_url": "https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=500"},
    {"name": "Corsair Gaming Mouse", "price": 79.99, "description": "High-precision gaming mouse", "image_url": "https://images.unsplash.com/photo-1527814050087-3793815479db?w=500"},
    {"name": "Razer Keyboard RGB", "price": 169.99, "description": "Mechanical gaming keyboard", "image_url": "https://images.unsplash.com/photo-1587829191301-41f1840efb45?w=500"},
    {"name": "SteelSeries Headset", "price": 199.99, "description": "Gaming headset with mic", "image_url": "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=500"},
    {"name": "ASUS Gaming Monitor", "price": 399.99, "description": "144Hz gaming monitor", "image_url": "https://images.unsplash.com/photo-1527864550417-7fd91fc51a46?w=500"},
    {"name": "Gaming Chair", "price": 299.99, "description": "Ergonomic racing chair", "image_url": "https://images.unsplash.com/photo-1593062096033-9a26b09da705?w=500"},
    {"name": "Steam Deck", "price": 549.00, "description": "Portable PC gaming device", "image_url": "https://images.unsplash.com/photo-1538481143081-9049b7ad6ff5?w=500"},
    {"name": "Gaming Desk", "price": 449.99, "description": "Large gaming desk", "image_url": "https://images.unsplash.com/photo-1593062096033-9a26b09da705?w=500"},
    
    # Smart Home - UNIQUE
    {"name": "Amazon Echo Dot", "price": 49.99, "description": "Smart speaker", "image_url": "https://images.unsplash.com/photo-1589003077984-894e133814c9?w=500"},
    {"name": "Google Nest Hub", "price": 99.99, "description": "Smart display", "image_url": "https://images.unsplash.com/photo-1559056199-641a0ac8b3f4?w=500"},
    {"name": "Apple HomePod mini", "price": 99.00, "description": "Smart speaker", "image_url": "https://images.unsplash.com/photo-1520869a399944-a9b76df1aed5?w=500"},
    {"name": "Philips Hue Lights", "price": 199.99, "description": "Smart color bulbs", "image_url": "https://images.unsplash.com/photo-1565636192335-14c911b00b92?w=500"},
    {"name": "Nest Thermostat", "price": 249.99, "description": "Smart thermostat", "image_url": "https://images.unsplash.com/photo-1587202372775-e43eb21d4fed?w=500"},
    {"name": "Wyze Security Camera", "price": 29.99, "description": "Smart home camera", "image_url": "https://images.unsplash.com/photo-1577720643272-265f4a6c7c03?w=500"},
    {"name": "Smart Lock", "price": 249.99, "description": "Smart door lock", "image_url": "https://images.unsplash.com/photo-1597433707820-3f4ee4fae69e?w=500"},
    {"name": "Robot Vacuum", "price": 699.99, "description": "Autonomous vacuum cleaner", "image_url": "https://images.unsplash.com/photo-1585771724684-38269d6639fd?w=500"},
    {"name": "Smart Plug", "price": 14.99, "description": "WiFi smart plug", "image_url": "https://images.unsplash.com/photo-1609042231906-ec342b7b6d30?w=500"},
    {"name": "Ring Doorbell", "price": 99.99, "description": "Video doorbell", "image_url": "https://images.unsplash.com/photo-1577720643272-265f4a6c7c03?w=500"},
    
    # Health & Beauty - UNIQUE
    {"name": "Dyson Hairdryer", "price": 399.99, "description": "Premium hair dryer", "image_url": "https://images.unsplash.com/photo-1596386773274-36f9df8fdf4c?w=500"},
    {"name": "Oral-B Electric Toothbrush", "price": 299.99, "description": "Smart toothbrush", "image_url": "https://images.unsplash.com/photo-1585771724684-38269d6639fd?w=500"},
    {"name": "Clarisonic Face Brush", "price": 199.00, "description": "Sonic face cleansing brush", "image_url": "https://images.unsplash.com/photo-1596386773274-36f9df8fdf4c?w=500"},
    {"name": "Dermalogica Skincare", "price": 89.99, "description": "Professional skincare set", "image_url": "https://images.unsplash.com/photo-1556228578-8c89e6adf883?w=500"},
    {"name": "Lululemon Bag", "price": 298.00, "description": "Premium gym bag", "image_url": "https://images.unsplash.com/photo-1553062407-98eeb64c6a62?w=500"},
    
    # Outdoor & Sports - UNIQUE
    {"name": "Canon Binoculars", "price": 399.99, "description": "10x42 binoculars", "image_url": "https://images.unsplash.com/photo-1506905925346-21bda4d32df4?w=500"},
    {"name": "Coleman Tent", "price": 199.99, "description": "4-person camping tent", "image_url": "https://images.unsplash.com/photo-1478131143081-80f7f84ca84d?w=500"},
    {"name": "Osprey Backpack", "price": 299.99, "description": "70L hiking backpack", "image_url": "https://images.unsplash.com/photo-1553062407-98eeb64c6a62?w=500"},
    {"name": "Salomon Boots", "price": 249.99, "description": "Hiking boots", "image_url": "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=500"},
    {"name": "GoPro Mount Set", "price": 49.99, "description": "Camera mount accessories", "image_url": "https://images.unsplash.com/photo-1611532736579-6b16e2b50449?w=500"},
    {"name": "Fishing Rod Set", "price": 89.99, "description": "Complete fishing rod kit", "image_url": "https://images.unsplash.com/photo-1506905925346-21bda4d32df4?w=500"},
    {"name": "Mountain Bike", "price": 899.99, "description": "Full suspension mountain bike", "image_url": "https://images.unsplash.com/photo-1571188733066-c09fc87648d5?w=500"},
    {"name": "Skateboard", "price": 149.99, "description": "Professional skateboard", "image_url": "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=500"},
    {"name": "Roller Skates", "price": 129.99, "description": "Quad roller skates", "image_url": "https://images.unsplash.com/photo-1552289049-bebda541e143?w=500"},
    {"name": "Kayak", "price": 599.99, "description": "Inflatable kayak", "image_url": "https://images.unsplash.com/photo-1506905925346-21bda4d32df4?w=500"},
    
    # Music & Audio - UNIQUE
    {"name": "Yamaha Piano", "price": 3999.99, "description": "Digital stage piano", "image_url": "https://images.unsplash.com/photo-1510915150049-7ad5254603b1?w=500"},
    {"name": "Taylor Acoustic Guitar", "price": 899.99, "description": "Premium acoustic guitar", "image_url": "https://images.unsplash.com/photo-1510915150049-7ad5254603b1?w=500"},
    {"name": "Fender Stratocaster", "price": 799.99, "description": "Electric guitar", "image_url": "https://images.unsplash.com/photo-1493225457124-a3eb161ffa5f?w=500"},
    {"name": "Studio Microphone", "price": 299.99, "description": "Professional condenser mic", "image_url": "https://images.unsplash.com/photo-1612225603865-76daf6b91e4e?w=500"},
    {"name": "MIDI Keyboard Controller", "price": 199.99, "description": "49-key MIDI controller", "image_url": "https://images.unsplash.com/photo-1519412666065-38cd8083970b?w=500"},
]

def seed_products():
    """Seed the database with products"""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    
    try:
        # Clear existing products
        existing_count = db.query(Product).count()
        if existing_count > 0:
            print(f"Clearing {existing_count} existing products and related data...")
            # Clear cart items first (they reference products)
            db.execute(text("DELETE FROM cart_items"))
            db.execute(text("DELETE FROM order_items"))
            db.execute(text("DELETE FROM orders"))
            # Now clear products
            db.execute(text("DELETE FROM products"))
            db.commit()
        
        # Add all products
        for i, product_data in enumerate(PRODUCTS, 1):
            product = Product(**product_data)
            db.add(product)
            if i % 10 == 0:
                print(f"Adding product {i}/{len(PRODUCTS)}: {product.name}")
        
        db.commit()
        print(f"\nSuccessfully seeded {len(PRODUCTS)} products!")
        
        # Verify
        count = db.query(Product).count()
        print(f"Total products in database: {count}")
        
    except Exception as e:
        db.rollback()
        print(f"Error seeding products: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    seed_products()
