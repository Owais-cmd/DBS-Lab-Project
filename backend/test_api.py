#!/usr/bin/env python3
"""
API Test Script for Adaptive Ordering System
Tests all major endpoints with sample requests
"""

import requests
import json
from typing import Dict, Optional

BASE_URL = "http://localhost:8000"
cookies = {}


def print_response(response: requests.Response, title: str):
    """Pretty print API response."""
    print(f"\n{'='*60}")
    print(f"🔹 {title}")
    print(f"{'='*60}")
    print(f"Status: {response.status_code}")
    try:
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except:
        print(f"Response: {response.text}")
    print(f"{'='*60}")


def test_health():
    """Test root endpoint."""
    response = requests.get(f"{BASE_URL}/")
    print_response(response, "Health Check")
    return response.status_code == 200


def test_signup():
    """Test user signup."""
    data = {
        "email": "testuser@example.com",
        "password": "testpass123",
        "name": "Test User",
        "city": "Mumbai"
    }
    response = requests.post(f"{BASE_URL}/auth/signup", json=data)
    print_response(response, "User Signup")
    return response.status_code in [200, 400]  # 400 if already exists


def test_login():
    """Test user login."""
    global cookies
    data = {
        "email": "testuser@example.com",
        "password": "testpass123"
    }
    response = requests.post(f"{BASE_URL}/auth/login", json=data)
    print_response(response, "User Login")
    
    if response.status_code == 200:
        cookies = response.cookies.get_dict()
        print(f"✅ Cookies received: {cookies}")
    
    return response.status_code == 200


def test_get_profile():
    """Test get current user profile."""
    response = requests.get(f"{BASE_URL}/users/me", cookies=cookies)
    print_response(response, "Get User Profile")
    return response.status_code == 200


def test_list_items():
    """Test listing items."""
    response = requests.get(f"{BASE_URL}/items?limit=5")
    print_response(response, "List Items")
    return response.status_code == 200


def test_search_items():
    """Test searching items by category."""
    response = requests.get(f"{BASE_URL}/items?category=electronics&limit=3")
    print_response(response, "Search Items by Category")
    return response.status_code == 200


def test_add_to_cart():
    """Test adding item to cart."""
    data = {
        "item_id": 1,
        "quantity": 2
    }
    response = requests.post(f"{BASE_URL}/cart/add", json=data, cookies=cookies)
    print_response(response, "Add to Cart")
    return response.status_code == 200


def test_add_more_to_cart():
    """Test adding another item to cart."""
    data = {
        "item_id": 5,
        "quantity": 1
    }
    response = requests.post(f"{BASE_URL}/cart/add", json=data, cookies=cookies)
    print_response(response, "Add Another Item to Cart")
    return response.status_code == 200


def test_place_order():
    """Test placing an order."""
    response = requests.post(f"{BASE_URL}/orders/place", cookies=cookies)
    print_response(response, "Place Order")
    return response.status_code == 200


def test_get_orders():
    """Test getting user's orders."""
    response = requests.get(f"{BASE_URL}/users/me/orders?limit=3", cookies=cookies)
    print_response(response, "Get User Orders")
    return response.status_code == 200


def test_logout():
    """Test logout."""
    response = requests.post(f"{BASE_URL}/auth/logout", cookies=cookies)
    print_response(response, "Logout")
    return response.status_code == 200


def run_all_tests():
    """Run all tests in sequence."""
    print("\n" + "="*60)
    print("🧪 STARTING API TESTS")
    print("="*60)
    
    tests = [
        ("Health Check", test_health),
        ("User Signup", test_signup),
        ("User Login", test_login),
        ("Get Profile", test_get_profile),
        ("List Items", test_list_items),
        ("Search Items", test_search_items),
        ("Add to Cart", test_add_to_cart),
        ("Add More to Cart", test_add_more_to_cart),
        ("Place Order", test_place_order),
        ("Get Orders", test_get_orders),
        ("Logout", test_logout),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {name} - PASSED")
            else:
                failed += 1
                print(f"❌ {name} - FAILED")
        except Exception as e:
            failed += 1
            print(f"❌ {name} - ERROR: {e}")
    
    print("\n" + "="*60)
    print(f"📊 TEST RESULTS")
    print("="*60)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Total:  {len(tests)}")
    print("="*60)


if __name__ == "__main__":
    try:
        run_all_tests()
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
