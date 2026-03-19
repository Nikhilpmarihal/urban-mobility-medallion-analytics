import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()
import os

fake = Faker()

# ─────────────────────────────────────────────────────────────
#  INDIAN NAME POOLS  — replaces fake.name() (Western names)
# ─────────────────────────────────────────────────────────────
INDIAN_FIRST_NAMES = [
    # Male — North India
    'Aarav', 'Aakash', 'Abhishek', 'Aditya', 'Ajay', 'Akash', 'Alok', 'Amit',
    'Amitabh', 'Anand', 'Aniket', 'Anil', 'Ankur', 'Ankit', 'Anshul', 'Arjun',
    'Arnav', 'Arun', 'Arvind', 'Ashish', 'Ashok', 'Atharv', 'Atul', 'Ayush',
    'Bhuvan', 'Chetan', 'Chirag', 'Darshan', 'Deepak', 'Dev', 'Dhruv', 'Dinesh',
    'Gaurav', 'Girish', 'Gopal', 'Govind', 'Harish', 'Hemant', 'Hitesh',
    'Ishaan', 'Jagdish', 'Jayesh', 'Kamlesh', 'Kartik', 'Keshav', 'Kishan',
    'Kiran', 'Krishna', 'Kuldeep', 'Lalit', 'Lokesh', 'Madhav', 'Mahesh',
    'Manish', 'Manoj', 'Mohit', 'Mohan', 'Mukesh', 'Nakul', 'Naresh',
    'Naveen', 'Nikhil', 'Nilesh', 'Nishant', 'Nitesh', 'Omkar', 'Pankaj',
    'Parth', 'Piyush', 'Pradeep', 'Prakash', 'Pranav', 'Prateek', 'Praveen',
    'Puneet', 'Rahul', 'Raj', 'Rajesh', 'Rakesh', 'Ramesh', 'Ravi', 'Rishabh',
    'Rohit', 'Rohan', 'Sachin', 'Sahil', 'Sanjay', 'Sanket', 'Saurabh',
    'Shivam', 'Shubham', 'Siddharth', 'Sudhir', 'Sunil', 'Suresh', 'Tarun',
    'Tushar', 'Umesh', 'Vaibhav', 'Vikas', 'Vijay', 'Vikash', 'Vikram',
    'Vinay', 'Vipin', 'Vishal', 'Vivek', 'Yash', 'Yogesh',
    # Male — South India
    'Abishek', 'Ajith', 'Aravind', 'Balaji', 'Bharath', 'Ganesh', 'Gokul',
    'Hariharan', 'Karthik', 'Krishnamurthy', 'Madan', 'Madhan', 'Murali',
    'Nagaraj', 'Naveen', 'Prabhu', 'Prasad', 'Prithvi', 'Rajkumar', 'Ramu',
    'Ranjith', 'Santosh', 'Senthil', 'Shiva', 'Srinivas', 'Sudhir', 'Surya',
    'Venkat', 'Venkatesh', 'Vijayakumar', 'Vinod', 'Vishnu',
    # Female — North India
    'Aastha', 'Aditi', 'Aishwarya', 'Akanksha', 'Amrita', 'Ananya', 'Anjali',
    'Ankita', 'Anushka', 'Arushi', 'Bhavna', 'Deepa', 'Deepika', 'Divya',
    'Garima', 'Geeta', 'Gunjan', 'Harpreet', 'Himani', 'Isha', 'Jyoti',
    'Kajal', 'Kavita', 'Kavya', 'Khushi', 'Komal', 'Kritika', 'Lakshmi',
    'Latika', 'Laxmi', 'Madhuri', 'Mansi', 'Meera', 'Megha', 'Monika',
    'Muskan', 'Nandini', 'Neha', 'Nidhi', 'Nisha', 'Nishtha', 'Poonam',
    'Pooja', 'Pragya', 'Priyanka', 'Priya', 'Radha', 'Ranu', 'Rashmi',
    'Raveena', 'Rekha', 'Ritu', 'Riya', 'Sakshi', 'Saloni', 'Sangeeta',
    'Seema', 'Shikha', 'Shreya', 'Shruti', 'Simran', 'Sneha', 'Sonam',
    'Sunita', 'Swati', 'Tanvi', 'Tanya', 'Usha', 'Vandana', 'Varsha',
    'Vidya', 'Yamini', 'Zara',
    # Female — South India
    'Akhila', 'Amala', 'Ambika', 'Anitha', 'Archana', 'Aswini', 'Bhagyalakshmi',
    'Brinda', 'Chitra', 'Gayathri', 'Geetha', 'Hema', 'Indira', 'Jayalakshmi',
    'Kalpana', 'Kamala', 'Kasthuri', 'Keerthi', 'Komala', 'Latha', 'Lavanya',
    'Malathi', 'Mallika', 'Nithya', 'Padma', 'Pavithra', 'Preethi', 'Ramya',
    'Rohini', 'Saranya', 'Saraswathi', 'Savitha', 'Shanthi', 'Shobha',
    'Sirisha', 'Sreedevi', 'Sridevi', 'Suganya', 'Sujatha', 'Sumathi',
    'Supriya', 'Swarna', 'Uma', 'Usha', 'Vaishnavi', 'Vasantha', 'Vijayalakshmi',
]

INDIAN_LAST_NAMES = [
    # Pan-India
    'Agarwal', 'Arora', 'Banerjee', 'Bhatt', 'Bhat', 'Bhattacharya', 'Biswal',
    'Chakraborty', 'Chatterjee', 'Chaudhary', 'Chopra', 'Das', 'Dasgupta',
    'Desai', 'Deshpande', 'Doshi', 'Dubey', 'Dutta', 'Dwivedi',
    # Karnataka / South
    'Gowda', 'Hegde', 'Hiremath', 'Hosamane', 'Hugar',
    # Pan-India cont.
    'Ghosh', 'Gupta', 'Iyer', 'Iyengar', 'Jain', 'Jha', 'Joshi',
    'Kapoor', 'Kaur', 'Khan', 'Khanna', 'Krishnamurthy', 'Krishnan', 'Kulkarni',
    'Kumar', 'Lal', 'Malhotra', 'Mehta', 'Menon', 'Mishra', 'Mukherjee',
    'Murthy', 'Naidu', 'Nair', 'Naik', 'Nath', 'Pandey', 'Parikh',
    'Patel', 'Patil', 'Pillai', 'Prasad', 'Rao',
    # Rajasthan / Gujarat
    'Rastogi', 'Rathi', 'Rawat',
    # Pan-India cont.
    'Reddy', 'Roy', 'Saha', 'Saxena', 'Sen', 'Shah', 'Sharma', 'Shastri',
    'Shetty', 'Shukla', 'Singh', 'Sinha', 'Srivastava', 'Subramanian',
    # Karnataka / South cont.
    'Swamy', 'Narayanan', 'Venkataraman', 'Venkatesh',
    # Pan-India cont.
    'Tiwari', 'Trivedi', 'Tyagi', 'Upadhyay', 'Varma', 'Verma',
    'Vyas', 'Yadav',
]

def _indian_name() -> str:
    """Returns a random Indian full name."""
    return f"{random.choice(INDIAN_FIRST_NAMES)} {random.choice(INDIAN_LAST_NAMES)}"

def _indian_email(full_name: str) -> str:
    """Derives an Indian-style email from a full name."""
    first, last = full_name.lower().split()[:2]
    return f"{first}.{last}@example.in"


# ─────────────────────────────────────────────────────────────
#  MAPPING TABLES
# ─────────────────────────────────────────────────────────────

VEHICLE_TYPE_MAPPING = [
    {'vehicle_type_id': 1, 'vehicle_type': 'Uber Go',      'description': 'Compact',       'base_rate': 50.0,  'per_km': 12.0, 'per_minute': 1.5},
    {'vehicle_type_id': 2, 'vehicle_type': 'Uber Premier', 'description': 'Sedan',         'base_rate': 70.0,  'per_km': 15.0, 'per_minute': 2.0},
    {'vehicle_type_id': 3, 'vehicle_type': 'Uber Auto',    'description': 'Three Wheeler', 'base_rate': 30.0,  'per_km': 10.0, 'per_minute': 1.0},
    {'vehicle_type_id': 4, 'vehicle_type': 'Uber Moto',    'description': 'Bike',          'base_rate': 20.0,  'per_km': 6.0,  'per_minute': 0.5},
    {'vehicle_type_id': 5, 'vehicle_type': 'Uber XL',      'description': 'SUV',           'base_rate': 100.0, 'per_km': 20.0, 'per_minute': 3.0},
]

PAYMENT_METHOD_MAPPING = [
    {'payment_method_id': 1, 'payment_method': 'UPI',         'is_card': False, 'requires_auth': True},
    {'payment_method_id': 2, 'payment_method': 'Cash',        'is_card': False, 'requires_auth': False},
    {'payment_method_id': 3, 'payment_method': 'Credit Card', 'is_card': True,  'requires_auth': True},
    {'payment_method_id': 4, 'payment_method': 'Debit Card',  'is_card': True,  'requires_auth': True},
]

RIDE_STATUS_MAPPING = [
    {'ride_status_id': 1, 'ride_status': 'Completed', 'is_completed': True},
    {'ride_status_id': 2, 'ride_status': 'Cancelled',  'is_completed': False},
]

VEHICLE_MAKE_MAPPING = [
    {'vehicle_make_id': 1, 'vehicle_make': 'Maruti Suzuki'},
    {'vehicle_make_id': 2, 'vehicle_make': 'Hyundai'},
    {'vehicle_make_id': 3, 'vehicle_make': 'Tata Motors'},
    {'vehicle_make_id': 4, 'vehicle_make': 'Toyota'},
    {'vehicle_make_id': 5, 'vehicle_make': 'Mahindra'},
    {'vehicle_make_id': 6, 'vehicle_make': 'Honda'},
    {'vehicle_make_id': 7, 'vehicle_make': 'Bajaj'},
]

CITY_MAPPING = [
    {'city_id': 1,  'city_name': 'Indiranagar',     'latitude': 12.9784, 'longitude': 77.6408, 'region': 'East Bengaluru',    'state': 'Karnataka'},
    {'city_id': 2,  'city_name': 'Koramangala',     'latitude': 12.9352, 'longitude': 77.6245, 'region': 'South Bengaluru',   'state': 'Karnataka'},
    {'city_id': 3,  'city_name': 'Whitefield',      'latitude': 12.9698, 'longitude': 77.7500, 'region': 'East Bengaluru',    'state': 'Karnataka'},
    {'city_id': 4,  'city_name': 'MG Road',         'latitude': 12.9756, 'longitude': 77.6067, 'region': 'Central Bengaluru', 'state': 'Karnataka'},
    {'city_id': 5,  'city_name': 'Jayanagar',       'latitude': 12.9307, 'longitude': 77.5838, 'region': 'South Bengaluru',   'state': 'Karnataka'},
    {'city_id': 6,  'city_name': 'HSR Layout',      'latitude': 12.9121, 'longitude': 77.6446, 'region': 'South Bengaluru',   'state': 'Karnataka'},
    {'city_id': 7,  'city_name': 'Electronic City', 'latitude': 12.8452, 'longitude': 77.6632, 'region': 'South Bengaluru',   'state': 'Karnataka'},
    {'city_id': 8,  'city_name': 'Hebbal',          'latitude': 13.0354, 'longitude': 77.5988, 'region': 'North Bengaluru',   'state': 'Karnataka'},
    {'city_id': 9,  'city_name': 'Marathahalli',    'latitude': 12.9569, 'longitude': 77.7011, 'region': 'East Bengaluru',    'state': 'Karnataka'},
    {'city_id': 10, 'city_name': 'Rajajinagar',     'latitude': 12.9901, 'longitude': 77.5525, 'region': 'West Bengaluru',    'state': 'Karnataka'},
]

CANCELLATION_REASON_MAPPING = [
    {'cancellation_reason_id': 1, 'cancellation_reason': 'Driver cancelled'},
    {'cancellation_reason_id': 2, 'cancellation_reason': 'Passenger cancelled'},
    {'cancellation_reason_id': 3, 'cancellation_reason': 'No show'},
    {'cancellation_reason_id': 4, 'cancellation_reason': None},  # Completed rides
]

# ─────────────────────────────────────────────────────────────
#  LOOKUP MAPS
# ─────────────────────────────────────────────────────────────

VEHICLE_MAKES_LIST    = [m['vehicle_make']    for m in VEHICLE_MAKE_MAPPING]
VEHICLE_MAKE_ID_MAP   = {m['vehicle_make']:    m['vehicle_make_id']   for m in VEHICLE_MAKE_MAPPING}

VEHICLE_TYPES_LIST    = [t['vehicle_type']    for t in VEHICLE_TYPE_MAPPING]
VEHICLE_TYPE_ID_MAP   = {t['vehicle_type']:    t['vehicle_type_id']   for t in VEHICLE_TYPE_MAPPING}

PAYMENT_METHODS_LIST  = [p['payment_method']  for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_ID_MAP = {p['payment_method']:  p['payment_method_id'] for p in PAYMENT_METHOD_MAPPING}

RIDE_STATUSES_LIST    = [s['ride_status']     for s in RIDE_STATUS_MAPPING]
RIDE_STATUS_ID_MAP    = {s['ride_status']:     s['ride_status_id']    for s in RIDE_STATUS_MAPPING}

CITY_LIST             = [c['city_name']       for c in CITY_MAPPING]
CITY_ID_MAP           = {c['city_name']:       c['city_id']           for c in CITY_MAPPING}

# ✅ Only map non-None cancellation reasons — avoids KeyError on None key
CANCELLATION_REASON_ID_MAP = {
    c['cancellation_reason']: c['cancellation_reason_id']
    for c in CANCELLATION_REASON_MAPPING
    if c['cancellation_reason'] is not None
}


# ─────────────────────────────────────────────────────────────
#  GENERATOR
# ─────────────────────────────────────────────────────────────

def generate_uber_ride_confirmation() -> dict:

    # ── Timestamps ───────────────────────────────────────────
    pickup_time      = datetime.now() - timedelta(
                           days=random.randint(0, 30),
                           hours=random.randint(0, 23))
    duration_minutes = random.randint(5, 120)
    dropoff_time     = pickup_time + timedelta(minutes=duration_minutes)
    booking_time     = pickup_time - timedelta(minutes=random.randint(1, 10))

    # ── Distance ─────────────────────────────────────────────
    distance_km = round(random.uniform(2.5, 30.0), 2)

    # ── Vehicle type & pricing ───────────────────────────────
    vehicle_type    = random.choice(VEHICLE_TYPES_LIST)
    vehicle_type_id = VEHICLE_TYPE_ID_MAP[vehicle_type]
    vt              = next(v for v in VEHICLE_TYPE_MAPPING if v['vehicle_type'] == vehicle_type)

    base_fare        = float(vt['base_rate'])                                   # ✅ explicit float
    per_km_rate      = float(vt['per_km'])
    per_minute_rate  = float(vt['per_minute'])
    surge_multiplier = float(random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 1.75, 2.0]))

    distance_fare = round(distance_km * per_km_rate, 2)
    time_fare     = round(duration_minutes * per_minute_rate, 2)
    subtotal      = round((base_fare + distance_fare + time_fare) * surge_multiplier, 2)

    # ✅ tip_amount explicitly float — was int, caused DoubleType nulls in silver
    tip_amount    = float(random.choice([0, 0, 0, 20, 30, 50, 100]))
    total_fare    = round(subtotal + tip_amount, 2)

    # ── Cities ───────────────────────────────────────────────
    pickup_city  = random.choice(CITY_LIST)
    dropoff_city = random.choice(CITY_LIST)

    pickup_city_data  = next(c for c in CITY_MAPPING if c['city_name'] == pickup_city)
    dropoff_city_data = next(c for c in CITY_MAPPING if c['city_name'] == dropoff_city)

    delta       = 2.0 / 111.0
    pickup_lat  = round(pickup_city_data['latitude']   + random.uniform(-delta, delta), 6)
    pickup_lng  = round(pickup_city_data['longitude']  + random.uniform(-delta, delta), 6)
    dropoff_lat = round(dropoff_city_data['latitude']  + random.uniform(-delta, delta), 6)
    dropoff_lng = round(dropoff_city_data['longitude'] + random.uniform(-delta, delta), 6)

    # ── Vehicle make ─────────────────────────────────────────
    vehicle_make    = random.choice(VEHICLE_MAKES_LIST)
    vehicle_make_id = VEHICLE_MAKE_ID_MAP[vehicle_make]

    # ── Cancellation ─────────────────────────────────────────
    # ✅ cancellation_reason_id = 4 set directly for completed rides
    #    — never does a dict lookup on None key (was KeyError risk)
    is_cancelled           = random.random() < 0.1
    cancellation_reason_id = 4  # default: completed
    if is_cancelled:
        reason                 = random.choice(['Driver cancelled', 'Passenger cancelled', 'No show'])
        cancellation_reason_id = CANCELLATION_REASON_ID_MAP[reason]

    # ── Payment & status ─────────────────────────────────────
    payment_method    = random.choice(PAYMENT_METHODS_LIST)
    payment_method_id = PAYMENT_METHOD_ID_MAP[payment_method]

    ride_status    = random.choice(['Completed', 'Completed', 'Cancelled'])
    ride_status_id = RIDE_STATUS_ID_MAP[ride_status]

    # ── Rating ───────────────────────────────────────────────
    # ✅ Was: random.choice([None, random.randint(1,5)])
    #    Bug: randint was evaluated ONCE when the list literal was created,
    #         so every call got the same fixed number or None.
    #    Fix: evaluate randint lazily inside the conditional each call.
    rating = float(random.randint(1, 5)) if random.random() > 0.3 else None

    # ── Resolve city IDs before building the dict ────────────
    pickup_city_id  = CITY_ID_MAP[pickup_city]
    dropoff_city_id = CITY_ID_MAP[dropoff_city]

    # ── Build record ─────────────────────────────────────────
    ride_confirmation = {

        # Identifiers
        'ride_id':             str(uuid.uuid4()),
        'confirmation_number': fake.bothify('UB??###-####'),
        'passenger_id':        str(uuid.uuid4()),
        'driver_id':           str(uuid.uuid4()),
        'vehicle_id':          str(uuid.uuid4()),
        'pickup_location_id':  str(uuid.uuid4()),
        'dropoff_location_id': str(uuid.uuid4()),

        # Foreign Keys
        'vehicle_type_id':         vehicle_type_id,
        'vehicle_make_id':         vehicle_make_id,
        'payment_method_id':       payment_method_id,
        'ride_status_id':          ride_status_id,
        'pickup_city_id':          pickup_city_id,
        'dropoff_city_id':         dropoff_city_id,
        'cancellation_reason_id':  cancellation_reason_id,

        # Passenger
        'passenger_name':  (passenger_name := _indian_name()),
        'passenger_email': _indian_email(passenger_name),
        'passenger_phone': f"+91 {random.randint(6000000000, 9999999999)}",

        # Driver
        'driver_name':    _indian_name(),
        'driver_rating':  round(random.uniform(3.5, 5.0), 2),
        'driver_phone':   f"+91 {random.randint(6000000000, 9999999999)}",
        'driver_license': fake.bothify('??## ###########'),

        # Vehicle
        'vehicle_model': fake.word().capitalize(),
        'vehicle_color': random.choice(['White', 'Silver', 'Black', 'Grey', 'Red', 'Blue', 'Brown', 'Gold']),
        'license_plate': fake.bothify('?? ## ?? ####'),

        # Locations
        'pickup_address':    f"Near {pickup_city}, Bengaluru, Karnataka",
        'pickup_latitude':   pickup_lat,
        'pickup_longitude':  pickup_lng,
        'dropoff_address':   f"Near {dropoff_city}, Bengaluru, Karnataka",
        'dropoff_latitude':  dropoff_lat,
        'dropoff_longitude': dropoff_lng,

        # Ride metrics
        'distance_km':      distance_km,
        'duration_minutes': duration_minutes,

        # ✅ Timestamps as ISO strings — silver.py casts to TimestampType
        'booking_timestamp': booking_time.isoformat(),
        'pickup_timestamp':  pickup_time.isoformat(),
        'dropoff_timestamp': dropoff_time.isoformat(),

        # Pricing — all explicitly float to prevent int serialisation
        'base_fare':        base_fare,
        'distance_fare':    distance_fare,
        'time_fare':        time_fare,
        'surge_multiplier': surge_multiplier,
        'subtotal':         subtotal,
        'tip_amount':       tip_amount,       # ✅ was int
        'total_fare':       total_fare,

        # Rating — ✅ lazily evaluated float or None
        'rating': rating,
    }

    return ride_confirmation