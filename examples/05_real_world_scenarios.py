import time
import random
from datetime import datetime, timedelta
from ChimeraDB.chimera.chimera_db import ChimeraDB


def ecommerce_scenario():
    db = ChimeraDB("./ecommerce_data")
    db.startup()
    
    try:
        print("1. Product Catalog (Document Engine)")
        
        products = [
            {
                'id': 'prod_001',
                'name': 'Laptop Pro X1',
                'category': 'Electronics',
                'price': 1299.99,
                'currency': 'USD',
                'description': 'High-performance laptop for professionals',
                'specifications': {
                    'cpu': 'Intel i7-12700H',
                    'ram': '16GB DDR4',
                    'storage': '512GB SSD',
                    'display': '15.6" 4K OLED'
                },
                'images': [
                    {'url': '/images/laptop_1.jpg', 'alt': 'Front view'},
                    {'url': '/images/laptop_2.jpg', 'alt': 'Side view'}
                ],
                'reviews': [
                    {'user_id': 'user_001', 'rating': 5, 'comment': 'Excellent performance'},
                    {'user_id': 'user_002', 'rating': 4, 'comment': 'Great value for money'}
                ],
                'inventory': {
                    'available': 15,
                    'reserved': 3,
                    'total': 18
                },
                'tags': ['laptop', 'professional', 'high-end'],
                'created_at': '2024-01-01T10:00:00Z',
                'updated_at': '2024-01-15T14:30:00Z'
            },
            {
                'id': 'prod_002',
                'name': 'Wireless Headphones',
                'category': 'Electronics',
                'price': 199.99,
                'currency': 'USD',
                'description': 'Premium wireless headphones with noise cancellation',
                'specifications': {
                    'battery_life': '30 hours',
                    'connectivity': 'Bluetooth 5.0',
                    'noise_cancellation': True,
                    'water_resistant': True
                },
                'images': [
                    {'url': '/images/headphones_1.jpg', 'alt': 'Product image'}
                ],
                'reviews': [
                    {'user_id': 'user_003', 'rating': 5, 'comment': 'Amazing sound quality'}
                ],
                'inventory': {
                    'available': 25,
                    'reserved': 0,
                    'total': 25
                },
                'tags': ['headphones', 'wireless', 'premium'],
                'created_at': '2024-01-05T09:00:00Z',
                'updated_at': '2024-01-10T16:45:00Z'
            }
        ]
        
        product_result = db.auto_store("products", products, "transactional")
        print(f"   Stored {product_result['items_stored']} products using {product_result['engine_used']} engine")
        
        print("\n2. User Sessions (Key-Value Engine)")
        
        sessions = [
            {
                'session_id': 'sess_abc123',
                'user_id': 'user_001',
                'cart_items': ['prod_001', 'prod_002'],
                'last_activity': '2024-01-15T15:30:00Z',
                'ip_address': '192.168.1.100',
                'user_agent': 'Mozilla/5.0...'
            },
            {
                'session_id': 'sess_def456',
                'user_id': 'user_002',
                'cart_items': ['prod_001'],
                'last_activity': '2024-01-15T14:45:00Z',
                'ip_address': '192.168.1.101',
                'user_agent': 'Mozilla/5.0...'
            }
        ]
        
        session_result = db.auto_store("sessions", sessions, "real-time")
        print(f"   Stored {session_result['items_stored']} sessions using {session_result['engine_used']} engine")
        
        print("\n3. Sales Analytics (Column Engine)")
        
        sales_data = [
            {'date': '2024-01-01', 'product_id': 'prod_001', 'quantity': 5, 'revenue': 6499.95, 'region': 'North America'},
            {'date': '2024-01-01', 'product_id': 'prod_002', 'quantity': 12, 'revenue': 2399.88, 'region': 'North America'},
            {'date': '2024-01-02', 'product_id': 'prod_001', 'quantity': 3, 'revenue': 3899.97, 'region': 'Europe'},
            {'date': '2024-01-02', 'product_id': 'prod_002', 'quantity': 8, 'revenue': 1599.92, 'region': 'Europe'},
            {'date': '2024-01-03', 'product_id': 'prod_001', 'quantity': 7, 'revenue': 9099.93, 'region': 'Asia'},
            {'date': '2024-01-03', 'product_id': 'prod_002', 'quantity': 15, 'revenue': 2999.85, 'region': 'Asia'}
        ]
        
        sales_result = db.auto_store("sales", sales_data, "analytics")
        print(f"   Stored {sales_result['items_stored']} sales records using {sales_result['engine_used']} engine")
        
        print("\n4. Query Examples")
        
        products_result = db.query("products", "document", {'category': 'Electronics'})
        print(f"   Found {len(products_result)} electronics products")
        
        sales_result = db.query("sales", "column", {'revenue': {'$gt': 5000}})
        print(f"   Found {len(sales_result)} high-value sales (>$5000)")
        
    finally:
        db.shutdown()


def iot_monitoring_scenario():
    db = ChimeraDB("./iot_data")
    db.startup()
    
    try:
        print("1. Sensor Data (Time-Series Engine)")
        
        base_time = int(time.time()) - (24 * 3600)
        sensor_data = []
        
        for hour in range(24):
            timestamp = base_time + (hour * 3600)
            
            sensor_data.append({
                'timestamp': timestamp,
                'sensor_id': 'temp_sensor_001',
                'sensor_type': 'temperature',
                'value': 20 + random.uniform(-5, 5),
                'unit': 'celsius',
                'location': 'server_room_1',
                'status': 'active'
            })
            
            sensor_data.append({
                'timestamp': timestamp,
                'sensor_id': 'humidity_sensor_001',
                'sensor_type': 'humidity',
                'value': 50 + random.uniform(-10, 10),
                'unit': 'percent',
                'location': 'server_room_1',
                'status': 'active'
            })
            
            sensor_data.append({
                'timestamp': timestamp,
                'sensor_id': 'power_sensor_001',
                'sensor_type': 'power_consumption',
                'value': 5000 + random.uniform(-500, 500),
                'unit': 'watts',
                'location': 'server_room_1',
                'status': 'active'
            })
        
        sensor_result = db.auto_store("sensor_data", sensor_data, "analytics")
        print(f"   Stored {sensor_result['items_stored']} sensor readings using {sensor_result['engine_used']} engine")
        
        print("\n2. Device Registry (Document Engine)")
        
        devices = [
            {
                'device_id': 'temp_sensor_001',
                'name': 'Temperature Sensor 1',
                'type': 'temperature',
                'model': 'TS-2000',
                'manufacturer': 'SensorCorp',
                'location': {
                    'building': 'Data Center A',
                    'floor': 2,
                    'room': 'server_room_1',
                    'coordinates': {'lat': 40.7128, 'lng': -74.0060}
                },
                'specifications': {
                    'range': '-40 to 85°C',
                    'accuracy': '±0.5°C',
                    'power_consumption': '2.5W',
                    'communication': 'WiFi'
                },
                'maintenance': {
                    'last_calibration': '2024-01-01T00:00:00Z',
                    'next_calibration': '2024-07-01T00:00:00Z',
                    'warranty_expiry': '2025-01-01T00:00:00Z'
                },
                'status': 'active',
                'created_at': '2023-06-01T00:00:00Z'
            },
            {
                'device_id': 'humidity_sensor_001',
                'name': 'Humidity Sensor 1',
                'type': 'humidity',
                'model': 'HS-1500',
                'manufacturer': 'SensorCorp',
                'location': {
                    'building': 'Data Center A',
                    'floor': 2,
                    'room': 'server_room_1',
                    'coordinates': {'lat': 40.7128, 'lng': -74.0060}
                },
                'specifications': {
                    'range': '0 to 100%',
                    'accuracy': '±2%',
                    'power_consumption': '1.8W',
                    'communication': 'WiFi'
                },
                'maintenance': {
                    'last_calibration': '2024-01-01T00:00:00Z',
                    'next_calibration': '2024-07-01T00:00:00Z',
                    'warranty_expiry': '2025-01-01T00:00:00Z'
                },
                'status': 'active',
                'created_at': '2023-06-01T00:00:00Z'
            }
        ]
        
        device_result = db.auto_store("devices", devices, "transactional")
        print(f"   Stored {device_result['items_stored']} devices using {device_result['engine_used']} engine")
        
        print("\n3. Alert Rules (Key-Value Engine)")
        
        alert_rules = [
            {
                'rule_id': 'temp_high_alert',
                'sensor_type': 'temperature',
                'condition': 'value > 25',
                'severity': 'warning',
                'message': 'Temperature above normal range',
                'action': 'send_email',
                'recipients': ['admin@company.com'],
                'enabled': True
            },
            {
                'rule_id': 'humidity_low_alert',
                'sensor_type': 'humidity',
                'condition': 'value < 30',
                'severity': 'critical',
                'message': 'Humidity below safe range',
                'action': 'send_sms',
                'recipients': ['+1234567890'],
                'enabled': True
            },
            {
                'rule_id': 'power_high_alert',
                'sensor_type': 'power_consumption',
                'condition': 'value > 6000',
                'severity': 'critical',
                'message': 'Power consumption above threshold',
                'action': 'send_email',
                'recipients': ['admin@company.com', 'ops@company.com'],
                'enabled': True
            }
        ]
        
        alert_result = db.auto_store("alert_rules", alert_rules, "real-time")
        print(f"   Stored {alert_result['items_stored']} alert rules using {alert_result['engine_used']} engine")
        
        print("\n4. Query Examples")
        
        temp_readings = db.query("sensor_data", "timeseries", {
            'time_range': {
                'start': base_time,
                'end': base_time + (24 * 3600)
            }
        })
        print(f"   Retrieved {len(temp_readings)} sensor readings")
        
        active_devices = db.query("devices", "document", {'status': 'active'})
        print(f"   Found {len(active_devices)} active devices")
        
    finally:
        db.shutdown()


def social_media_scenario():
    db = ChimeraDB("./social_data")
    db.startup()
    
    try:
        print("1. User Profiles (Document Engine)")
        
        users = [
            {
                'user_id': 'user_001',
                'username': 'alice_tech',
                'email': 'alice@example.com',
                'profile': {
                    'display_name': 'Alice Johnson',
                    'bio': 'Software engineer passionate about AI and machine learning',
                    'location': 'San Francisco, CA',
                    'website': 'https://alice-tech.com',
                    'birth_date': '1990-05-15',
                    'joined_date': '2020-03-01T10:00:00Z'
                },
                'preferences': {
                    'theme': 'dark',
                    'language': 'en',
                    'timezone': 'America/Los_Angeles',
                    'notifications': {
                        'email': True,
                        'push': True,
                        'sms': False
                    }
                },
                'stats': {
                    'followers': 1250,
                    'following': 890,
                    'posts': 156,
                    'likes_received': 5430
                },
                'verified': True,
                'status': 'active'
            },
            {
                'user_id': 'user_002',
                'username': 'bob_artist',
                'email': 'bob@example.com',
                'profile': {
                    'display_name': 'Bob Smith',
                    'bio': 'Digital artist and creative director',
                    'location': 'New York, NY',
                    'website': 'https://bob-art.com',
                    'birth_date': '1988-12-03',
                    'joined_date': '2019-11-15T14:30:00Z'
                },
                'preferences': {
                    'theme': 'light',
                    'language': 'en',
                    'timezone': 'America/New_York',
                    'notifications': {
                        'email': True,
                        'push': False,
                        'sms': False
                    }
                },
                'stats': {
                    'followers': 2100,
                    'following': 450,
                    'posts': 89,
                    'likes_received': 8900
                },
                'verified': True,
                'status': 'active'
            }
        ]
        
        user_result = db.auto_store("users", users, "transactional")
        print(f"   Stored {user_result['items_stored']} users using {user_result['engine_used']} engine")
        
        print("\n2. Social Network (Graph Engine)")
        
        connections = [
            {'user_id': 'user_001', 'follows': ['user_002', 'user_003', 'user_004']},
            {'user_id': 'user_002', 'follows': ['user_001', 'user_003', 'user_005']},
            {'user_id': 'user_003', 'follows': ['user_001', 'user_002', 'user_006']},
            {'user_id': 'user_004', 'follows': ['user_001', 'user_007']},
            {'user_id': 'user_005', 'follows': ['user_002', 'user_008']},
            {'user_id': 'user_006', 'follows': ['user_003', 'user_009']}
        ]
        
        connection_result = db.auto_store("connections", connections, "graph_analysis")
        print(f"   Stored {connection_result['items_stored']} connections using {connection_result['engine_used']} engine")
        
        print("\n3. Posts and Content (Document Engine)")
        
        posts = [
            {
                'post_id': 'post_001',
                'user_id': 'user_001',
                'content': 'Just deployed our new AI model to production! 🚀 #AI #MachineLearning',
                'type': 'text',
                'created_at': '2024-01-15T10:30:00Z',
                'updated_at': '2024-01-15T10:30:00Z',
                'metadata': {
                    'hashtags': ['AI', 'MachineLearning'],
                    'mentions': [],
                    'language': 'en',
                    'sentiment': 'positive'
                },
                'engagement': {
                    'likes': 45,
                    'comments': 12,
                    'shares': 8,
                    'views': 1200
                },
                'visibility': 'public',
                'status': 'active'
            },
            {
                'post_id': 'post_002',
                'user_id': 'user_002',
                'content': 'New digital artwork inspired by urban architecture 🎨',
                'type': 'image',
                'media': {
                    'type': 'image',
                    'url': '/media/artwork_001.jpg',
                    'alt_text': 'Digital artwork of urban architecture',
                    'dimensions': {'width': 1920, 'height': 1080}
                },
                'created_at': '2024-01-15T14:15:00Z',
                'updated_at': '2024-01-15T14:15:00Z',
                'metadata': {
                    'hashtags': ['DigitalArt', 'UrbanArchitecture'],
                    'mentions': [],
                    'language': 'en',
                    'sentiment': 'neutral'
                },
                'engagement': {
                    'likes': 89,
                    'comments': 23,
                    'shares': 15,
                    'views': 2100
                },
                'visibility': 'public',
                'status': 'active'
            }
        ]
        
        post_result = db.auto_store("posts", posts, "transactional")
        print(f"   Stored {post_result['items_stored']} posts using {post_result['engine_used']} engine")
        
        print("\n4. Analytics Data (Column Engine)")
        
        analytics_data = [
            {'date': '2024-01-15', 'user_id': 'user_001', 'posts_created': 3, 'likes_given': 15, 'comments_made': 8, 'time_spent_minutes': 45},
            {'date': '2024-01-15', 'user_id': 'user_002', 'posts_created': 1, 'likes_given': 23, 'comments_made': 12, 'time_spent_minutes': 67},
            {'date': '2024-01-14', 'user_id': 'user_001', 'posts_created': 2, 'likes_given': 12, 'comments_made': 6, 'time_spent_minutes': 38},
            {'date': '2024-01-14', 'user_id': 'user_002', 'posts_created': 0, 'likes_given': 18, 'comments_made': 9, 'time_spent_minutes': 52}
        ]
        
        analytics_result = db.auto_store("analytics", analytics_data, "analytics")
        print(f"   Stored {analytics_result['items_stored']} analytics records using {analytics_result['engine_used']} engine")
        
        print("\n5. Query Examples")
        
        verified_users = db.query("users", "document", {'verified': True})
        print(f"   Found {len(verified_users)} verified users")
        
        high_engagement = db.query("posts", "document", {'engagement.likes': {'$gt': 50}})
        print(f"   Found {len(high_engagement)} posts with >50 likes")
        
    finally:
        db.shutdown()


def run_all_scenarios():
    scenarios = [
        ("E-commerce Application", ecommerce_scenario),
        ("IoT Monitoring System", iot_monitoring_scenario),
        ("Social Media Platform", social_media_scenario)
    ]
    
    for name, scenario_func in scenarios:
        print(f"Running: {name}")
        try:
            scenario_func()
        except Exception as e:
            print(f"Error in {name}: {e}")
        print("-" * 50)


if __name__ == "__main__":
    run_all_scenarios()