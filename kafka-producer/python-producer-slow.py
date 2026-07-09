import random
import json
import time
import math
from kafka import KafkaProducer


# helpers
def compute_fault(current, power_kw, voltage_fluct, prev_power_kw):
    """
    Assign fault indicator based on rules:
    0 → normal
    1 → voltage drop
    2 → overload
    3 → blackout risk
    """
    faults = ['normal', 'unstable', 'bad', 'blackout']
    if prev_power_kw is None:
        if current > MAX_CURRENT:
            fault_num = 2
            return fault_num, faults[fault_num]
        else: 
            fault_num = 0
            return fault_num, faults[fault_num]

    power_spike = (abs(power_kw - prev_power_kw) / max(prev_power_kw, 1)) * 100

    fault_num = 0
    # Voltage drop
    if -10 <= voltage_fluct < -5:
        fault_num = 1
    # Current overload
    if current > MAX_CURRENT:
        fault_num = 2
    # Blackout / extreme power imbalance
    if power_spike > 50 or voltage_fluct < -10 :
        fault_num = 3
    return fault_num, faults[fault_num]


def generate_voltage():
    # Normal fluctuation
    voltage = random.gauss(NOMINAL_VOLTAGE, 0.04 * NOMINAL_VOLTAGE)

    # Rare sag or surge
    if random.random() < 0.05:
        voltage *= random.uniform(0.75, 0.9)   # sag
    elif random.random() < 0.02:
        voltage *= random.uniform(1.1, 1.2)    # surge

    return round(voltage, 2)


def solar_output(prev_solar):
    hour = time.localtime().tm_hour

    # Daylight curve (0 at night, 1 at noon)
    daylight = max(0, math.cos((hour - 12) * math.pi / 12))

    # Cloud cover with persistence
    cloud = random.uniform(0.5, 1.0)

    base = MAX_SOLAR * daylight * cloud

    # Faster ramp-up/down
    solar_kw = 0.6 * prev_solar + 0.4 * base

    return round(max(0, min(solar_kw, MAX_SOLAR)), 2)


def wind_output(prev_wind):
    mean = 0.6 * MAX_WIND
    volatility = 0.2 * MAX_WIND
    reversion = 0.1

    gust = random.gauss(0, volatility)

    wind_kw = prev_wind + reversion * (mean - prev_wind) + gust

    return round(max(0, min(wind_kw, MAX_WIND)), 2)


def temperature_output(prev_temp):
    hour = time.localtime().tm_hour

    daily_cycle = math.sin(math.pi * (hour - 5) / 12)

    base_temp = 24 + 9 * daily_cycle   # wider range

    # Weather drift
    drift = random.uniform(-0.5, 0.5)

    temp = 0.85 * prev_temp + 0.15 * (base_temp + drift)

    return round(temp, 1)


def humidity_output(prev_humidity, temp_c):
    base_humidity = 90 - (temp_c - 18) * 2.0

    noise = random.uniform(-4, 4)

    humidity = 0.8 * prev_humidity + 0.2 * (base_humidity + noise)

    return round(max(25, min(humidity, 98)), 1)


def price_output(prev_price, power_kw, solar_kw, wind_kw):
    base_price = 0.11

    # Demand pressure
    demand_factor = power_kw / 400

    # Renewable relief
    renewable_factor = (solar_kw + wind_kw) / 250

    # Peak hours
    hour = time.localtime().tm_hour
    peak_factor = 0.04 if 17 <= hour <= 22 else 0

    # Market noise
    market_noise = random.uniform(-0.015, 0.02)

    target_price = (
        base_price
        + 0.08 * demand_factor
        - 0.06 * renewable_factor
        + peak_factor
        + market_noise
    )

    price = 0.85 * prev_price + 0.15 * target_price

    return round(max(0.06, min(price, 0.35)), 3)

# Nominal system values
NOMINAL_VOLTAGE = 230  # Volts
MAX_CURRENT = 500      # Amperes
MAX_SOLAR = 300        # kW
MAX_WIND = 200         # kW

# initial states
prev_power_kw = None
prev_solar_kw = 0.0
prev_wind_kw = MAX_WIND * 0.5
prev_temp = 24.0
prev_humidity = 60.0
prev_price = 0.12


kafka_nodes = "broker:29092"
topic = 'smartgrid'

prod = KafkaProducer(
    bootstrap_servers=kafka_nodes,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


def gen_data():
    global prev_power_kw,prev_power_kw, prev_solar_kw,prev_wind_kw, prev_temp, prev_humidity, prev_price

    # Voltage with small random fluctuations
    voltage = generate_voltage()

    # Current random but can cause overload sometimes
    current = round(random.uniform(0.5*MAX_CURRENT, 1.1*MAX_CURRENT), 2)

    # Power factor between 0.8 and 1.0
    pf = round(random.uniform(0.8, 1.0), 2)

    # Power consumption respects physical law: P = V * I * PF / 1000 for kW
    power_kw = round(voltage * current * pf / 1000, 2)

    # Reactive power (kVAR) = sqrt(S^2 - P^2), S approx apparent power V*I/1000
    s = voltage * current / 1000
    reactive_power = round((s**2 - power_kw**2)**0.5, 2) if s > power_kw else 0.0

    # Solar / wind generation
    solar_kw = solar_output(prev_solar_kw)
    wind_kw = wind_output(prev_wind_kw)

    # Grid IN and OUT
    grid_in = round(max(power_kw - (solar_kw + wind_kw), 0), 2)
    grid_out = round(max((solar_kw + wind_kw) - power_kw, 0), 2)    

    # Voltage fluctuation (%)
    voltage_fluct = round((voltage - NOMINAL_VOLTAGE)/NOMINAL_VOLTAGE * 100, 2)

    # Fault indicator
    fault_num, fault = compute_fault(current, power_kw, voltage_fluct, prev_power_kw)

    # Temperature & humidity
    temp_c = temperature_output(prev_temp)
    humidity = humidity_output(prev_humidity, temp_c)

    # Electricity price (£/kWh)
    price = price_output(prev_price, power_kw, solar_kw, wind_kw)


    # Update previous power/solar/wind
    prev_power_kw = power_kw
    prev_solar_kw = solar_kw
    prev_wind_kw = wind_kw
    prev_temp = temp_c
    prev_humidity = humidity
    prev_price = price

    # e.g. {"timestamp": 1765642790.4662578, "voltage_v": 231.96, 
    # "current_a": 397.19, "power_kw": 86.6, "reactive_power_kvar": 31.44, 
    # "power_factor": 0.94, "solar_kw": 114.84, "wind_kw": 128.9, 
    # "grid_in_kw": 0, "grid_out_kw": 0, 
    # "voltage_fluct_%": 0.85, "fault_indicator": 0, 
    # "temperature_c": 26.1, "humidity_%": 74.9, 
    # "electricity_price_gbp_per_kwh": 0.091}

    data = {
        "timestamp": time.time(),
        "voltage_v": voltage,
        "current_a": current,
        "power_kw": power_kw,
        "reactive_power_kvar": reactive_power,
        "power_factor": pf,
        "solar_kw": solar_kw,
        "wind_kw": wind_kw,
        "grid_in_kw": grid_in,
        "grid_out_kw": grid_out,
        "voltage_fluct_%": voltage_fluct,
        "fault_indicator": fault,
        "fault_num": fault_num,
        "temperature_c": temp_c,
        "humidity_%": humidity,
        "electricity_price_gbp_per_kwh": price
    }

    # data  = {
    #     "city": random.choice(['london', 'delhi', 'nyc']),
    #     "temperature": random.uniform(80, 100)
    # }

    print(json.dumps(data))

    prod.send(topic=topic, value=data)
    prod.flush()

if __name__ == "__main__":
    while True:
        gen_data()
        time.sleep(5)
     
