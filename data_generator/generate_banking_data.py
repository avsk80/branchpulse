#!/usr/bin/env python3
"""
Generate synthetic banking batch data for the S3 raw zone.

The script uses only Python standard libraries so it can run on any laptop.
It creates realistic CSV files without requiring a database or external API.
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


FIRST_NAMES = ["Aarav", "Vivaan", "Aditya", "Isha", "Anaya", "Diya", "Rohan", "Neha", "Kabir", "Mira"]
LAST_NAMES = ["Sharma", "Patel", "Gupta", "Singh", "Reddy", "Iyer", "Khan", "Mehta", "Joshi", "Nair"]
CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad"]
STATES = ["MH", "DL", "KA", "TG", "TN", "WB", "GJ"]
CHANNELS = ["ATM", "UPI", "POS", "NETBANKING", "MOBILE", "BRANCH"]
TXN_TYPES = ["DEBIT", "CREDIT"]
CATEGORIES = ["GROCERY", "TRAVEL", "FUEL", "DINING", "SHOPPING", "UTILITIES", "CASH", "SALARY"]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY", "NRI"]
KYC_STATUS = ["VERIFIED", "PENDING", "REJECTED"]


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_branches(count):
    rows = []
    for i in range(1, count + 1):
        rows.append(
            {
                "branch_id": f"BR{i:05d}",
                "branch_name": f"{random.choice(CITIES)} Main Branch {i}",
                "city": random.choice(CITIES),
                "state": random.choice(STATES),
                "region": random.choice(["WEST", "NORTH", "SOUTH", "EAST"]),
            }
        )
    return rows


def make_merchants(count):
    rows = []
    for i in range(1, count + 1):
        rows.append(
            {
                "merchant_id": f"M{i:06d}",
                "merchant_name": f"Merchant {i}",
                "merchant_category": random.choice(CATEGORIES),
                "risk_level": random.choice(["LOW", "LOW", "MEDIUM", "HIGH"]),
            }
        )
    return rows


def make_customers(count, branches):
    rows = []
    for i in range(1, count + 1):
        join_date = datetime(2016, 1, 1) + timedelta(days=random.randint(0, 3650))
        rows.append(
            {
                "customer_id": f"C{i:08d}",
                "first_name": random.choice(FIRST_NAMES),
                "last_name": random.choice(LAST_NAMES),
                "city": random.choice(CITIES),
                "state": random.choice(STATES),
                "kyc_status": random.choice(KYC_STATUS),
                "customer_segment": random.choice(["RETAIL", "PREMIUM", "WEALTH", "SME"]),
                "home_branch_id": random.choice(branches)["branch_id"],
                "created_at": join_date.strftime("%Y-%m-%d"),
            }
        )
    return rows


def make_accounts(count, customers, branches):
    rows = []
    for i in range(1, count + 1):
        opened_date = datetime(2017, 1, 1) + timedelta(days=random.randint(0, 3200))
        rows.append(
            {
                "account_id": f"A{i:09d}",
                "customer_id": random.choice(customers)["customer_id"],
                "branch_id": random.choice(branches)["branch_id"],
                "account_type": random.choice(ACCOUNT_TYPES),
                "account_status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT", "CLOSED"]),
                "open_date": opened_date.strftime("%Y-%m-%d"),
                "current_balance": round(random.uniform(500, 2000000), 2),
            }
        )
    return rows


def make_cards(accounts):
    rows = []
    for i, account in enumerate(accounts, start=1):
        if random.random() < 0.7:
            rows.append(
                {
                    "card_id": f"CARD{i:09d}",
                    "account_id": account["account_id"],
                    "card_type": random.choice(["DEBIT", "CREDIT", "PREPAID"]),
                    "card_status": random.choice(["ACTIVE", "ACTIVE", "BLOCKED"]),
                    "issued_date": account["open_date"],
                }
            )
    return rows


def make_transactions(count, accounts, merchants, batch_date):
    rows = []
    batch_start = datetime.strptime(batch_date, "%Y-%m-%d")
    for i in range(1, count + 1):
        account = random.choice(accounts)
        amount = round(random.uniform(10, 250000), 2)
        txn_time = batch_start + timedelta(seconds=random.randint(0, 86399))
        rows.append(
            {
                "transaction_id": f"T{batch_date.replace('-', '')}{i:010d}",
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "merchant_id": random.choice(merchants)["merchant_id"],
                "transaction_ts": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_type": random.choice(TXN_TYPES),
                "channel": random.choice(CHANNELS),
                "category": random.choice(CATEGORIES),
                "amount": amount,
                "currency": "INR",
                "status": random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "REVERSED"]),
                "is_suspected_fraud": random.choice([False, False, False, False, True]),
            }
        )
    return rows


def make_loan_applications(customers, batch_date, count):
    rows = []
    for i in range(1, count + 1):
        customer = random.choice(customers)
        rows.append(
            {
                "application_id": f"L{batch_date.replace('-', '')}{i:08d}",
                "customer_id": customer["customer_id"],
                "application_date": batch_date,
                "loan_type": random.choice(["HOME", "AUTO", "PERSONAL", "BUSINESS"]),
                "requested_amount": round(random.uniform(50000, 10000000), 2),
                "application_status": random.choice(["SUBMITTED", "APPROVED", "REJECTED", "UNDER_REVIEW"]),
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="sample_data/generated")
    parser.add_argument("--batch-date", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--customers", type=int, default=100000)
    parser.add_argument("--accounts", type=int, default=150000)
    parser.add_argument("--transactions", type=int, default=1000000)
    parser.add_argument("--branches", type=int, default=1000)
    parser.add_argument("--merchants", type=int, default=10000)
    parser.add_argument("--loan-applications", type=int, default=50000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    batch_dir = Path(args.output_dir) / f"batch_date={args.batch_date}"

    branches = make_branches(args.branches)
    merchants = make_merchants(args.merchants)
    customers = make_customers(args.customers, branches)
    accounts = make_accounts(args.accounts, customers, branches)
    cards = make_cards(accounts)
    transactions = make_transactions(args.transactions, accounts, merchants, args.batch_date)
    loans = make_loan_applications(customers, args.batch_date, args.loan_applications)

    write_csv(batch_dir / "branches.csv", branches[0].keys(), branches)
    write_csv(batch_dir / "merchants.csv", merchants[0].keys(), merchants)
    write_csv(batch_dir / "customers.csv", customers[0].keys(), customers)
    write_csv(batch_dir / "accounts.csv", accounts[0].keys(), accounts)
    write_csv(batch_dir / "cards.csv", cards[0].keys(), cards)
    write_csv(batch_dir / "transactions.csv", transactions[0].keys(), transactions)
    write_csv(batch_dir / "loan_applications.csv", loans[0].keys(), loans)

    print(f"Generated banking data under: {batch_dir}")


if __name__ == "__main__":
    main()
