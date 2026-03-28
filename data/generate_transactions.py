#!/usr/bin/env python3
"""
generate_transactions.py — Synthetic UPI Transaction Data Generator

Generates four CSV files for the Kavach digital-arrest scam detection platform:
  1. transactions.csv  (~100,000 rows)  — UPI transactions with scam labels
  2. accounts.csv      ( ~10,000 rows)  — User/merchant accounts
  3. calls.csv         ( ~10,000 rows)  — Phone/video calls linked to scam episodes
  4. complaints.csv    (  ~1,000 rows)  — Consumer complaint narratives

Usage:  python generate_transactions.py
Output: data/ directory (same directory as this script)
Seed:   42 for full reproducibility
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
SEED = 42
np.random.seed(SEED)
random.seed(SEED)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = SCRIPT_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_ACCOUNTS      = 10_000
MULE_FRACTION     = 0.03
NUM_SCAM_EPISODES = 500
NUM_TOTAL_TXNS    = 100_000
NUM_TOTAL_CALLS   = 10_000
NUM_COMPLAINTS    = 1_000
DATE_START        = datetime(2024, 1, 1)
DATE_END          = datetime(2024, 12, 31)

SCAM_AMOUNTS      = [25_000, 50_000, 100_000, 200_000, 500_000]
ROUND_NORMAL_AMTS = [100, 200, 500, 1000, 2000, 5000, 10_000, 15_000, 20_000]

# ═══════════════════════════════════════════════════════════════════════════════
# REFERENCE DATA
# ═══════════════════════════════════════════════════════════════════════════════
BANKS = [
    "State Bank of India", "HDFC Bank", "ICICI Bank", "Axis Bank",
    "Punjab National Bank", "Bank of Baroda", "Kotak Mahindra Bank",
    "Yes Bank", "IndusInd Bank", "Union Bank of India",
    "Canara Bank", "Bank of India", "IDBI Bank", "Federal Bank",
]

BANK_SUFFIX = {
    "State Bank of India": "@sbi", "HDFC Bank": "@okhdfcbank",
    "ICICI Bank": "@okicici", "Axis Bank": "@okaxis",
    "Punjab National Bank": "@pnb", "Bank of Baroda": "@bob",
    "Kotak Mahindra Bank": "@kotak", "Yes Bank": "@ybl",
    "IndusInd Bank": "@ibl", "Union Bank of India": "@unionbank",
    "Canara Bank": "@cnrb", "Bank of India": "@boi",
    "IDBI Bank": "@idbi", "Federal Bank": "@federal",
}

FIRST_NAMES = [
    "Rahul", "Priya", "Amit", "Sunita", "Raj", "Neha", "Vikram", "Anita",
    "Suresh", "Pooja", "Arun", "Kavita", "Deepak", "Meena", "Sanjay",
    "Rekha", "Manoj", "Shweta", "Rajesh", "Divya", "Ashok", "Sneha",
    "Vijay", "Ritu", "Pankaj", "Swati", "Nitin", "Komal", "Rohit", "Nisha",
    "Gaurav", "Anjali", "Mukesh", "Preeti", "Harish", "Sapna", "Vivek",
    "Jyoti", "Sunil", "Aarti", "Ramesh", "Geeta", "Rakesh", "Anu",
    "Naveen", "Pallavi", "Sandeep", "Bhavna", "Kiran", "Manju",
    "Arjun", "Shreya", "Varun", "Megha", "Siddharth", "Prachi",
    "Abhishek", "Tanvi", "Rishabh", "Ananya",
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Singh", "Kumar", "Patel", "Reddy",
    "Nair", "Joshi", "Mehta", "Rao", "Das", "Pillai", "Iyer",
    "Mishra", "Chauhan", "Yadav", "Pandey", "Saxena", "Agarwal",
    "Bhat", "Kulkarni", "Deshmukh", "Patil", "Banerjee", "Mukherjee",
    "Chatterjee", "Ghosh", "Sen", "Malik", "Kapoor", "Arora",
    "Tiwari", "Dubey", "Srivastava",
]

INDIAN_CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata",
    "Pune", "Ahmedabad", "Jaipur", "Lucknow", "Chandigarh", "Bhopal",
    "Patna", "Indore", "Nagpur", "Coimbatore", "Visakhapatnam",
    "Thiruvananthapuram", "Guwahati", "Ranchi", "Dehradun", "Surat",
    "Vadodara", "Nashik", "Agra", "Varanasi", "Kochi", "Mysuru",
    "Noida", "Gurgaon", "Faridabad", "Thane", "Navi Mumbai", "Kanpur",
    "Ludhiana", "Amritsar", "Raipur", "Bhubaneswar", "Jodhpur", "Mangalore",
]

TXN_TYPES        = ["P2P", "P2M", "collect", "autopay"]
TXN_TYPE_WEIGHTS = [0.6, 0.25, 0.1, 0.05]
CALL_TYPES       = ["voice", "video", "whatsapp_video"]
COMPLAINT_STATUS = ["registered", "under_investigation", "resolved", "escalated", "closed"]

AGENCIES = [
    "CBI", "Mumbai Cyber Crime", "Delhi Police Cyber Cell",
    "Narcotics Control Bureau", "Enforcement Directorate",
    "Income Tax Department", "Reserve Bank of India",
    "TRAI", "Customs Department", "Crime Branch",
]

PROFESSIONS = [
    "teacher", "professor", "doctor", "engineer",
    "retired government servant", "businessman", "retired army officer",
    "homemaker", "bank employee", "IT professional", "chartered accountant",
]

# ═══════════════════════════════════════════════════════════════════════════════
# NARRATIVE TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════
DIGITAL_ARREST_NARRATIVES = [
    "I received a call from someone claiming to be a {agency} officer. They said my Aadhaar was linked to a money laundering case and I needed to transfer funds to a 'secure RBI account' for verification. They kept me on {call_type} for {hours} hours and threatened me with immediate arrest. Under extreme fear, I transferred Rs {amount} to {n_accounts} different UPI IDs.",

    "A person called me saying they were from {agency}. They told me a SIM card registered in my name was used to send threatening messages linked to a terrorism case. They said I would be arrested within 2 hours if I didn't cooperate. They made me stay on video call continuously and I transferred Rs {amount} to multiple accounts.",

    "Someone posing as a {agency} officer called and said a parcel booked in my name was intercepted at Mumbai customs containing illegal drugs and fake passports. They transferred the call to a fake 'NCB officer' who showed me a forged arrest warrant on video call. I was terrified and transferred Rs {amount} over {hours} hours.",

    "I got a call from a person who identified himself as Inspector {fake_name} from {agency}. He said my bank account was used in a hawala transaction worth crores. He made me download Skype and kept me on video call for {hours} hours, saying I was under 'digital surveillance'. He made me transfer Rs {amount} for 'account verification'.",

    "A woman called saying she was from TRAI and my mobile number would be disconnected in 2 hours due to illegal activity. She transferred the call to a fake 'CBI officer' who showed me a fake FIR with my name. They said I was under 'digital arrest' and could not disconnect the video call. I transferred Rs {amount} to {n_accounts} accounts out of fear.",

    "Received an automated call saying my phone number is linked to illegal activities. A man in police uniform appeared on WhatsApp video call with a fake ID card. He showed me forged documents with my name and photo. He said only way to avoid arrest was transferring money for investigation. Lost Rs {amount}.",

    "Someone called claiming to be from {agency} and said that an account opened with my PAN card was involved in money laundering of Rs 48 crore. They kept me on Skype video call for {hours} hours and made me transfer Rs {amount} saying it would be returned after clearance.",

    "Sir/Madam, I am a {profession} aged {victim_age}. I received a call from someone who said he was from {agency}. He said my Aadhaar was misused and a case was being filed against me. He kept me on video call overnight and made me transfer my savings of Rs {amount}. I am devastated.",

    "A call came from an unknown number. The caller said he was from Mumbai Cyber Crime Cell. He said my identity was used to open 17 bank accounts for drug money. He said I must cooperate secretly and not tell anyone or I would be arrested for obstruction. Over {hours} hours, I transferred Rs {amount}.",

    "I was contacted on WhatsApp by someone from {agency}. They said a case was registered against me under PMLA for transactions worth Rs 2.6 crore. They created a fake 'Supreme Court hearing' on Skype with someone pretending to be a judge. I was forced to transfer Rs {amount} to 'court-mandated' accounts.",

    "I got a call saying I had an unpaid customs duty on a parcel from Taiwan. The call was transferred to a 'narcotics officer' who said MDMA was found inside. He placed me under 'digital arrest' via video call and said I'd be jailed for 7 years. In panic, I transferred Rs {amount} across {n_accounts} UPI IDs over {hours} hours.",

    "My father (aged {victim_age}) received a call from someone impersonating a {agency} officer. They told him his son was caught in a drug case and would be arrested. They kept him on video call for {hours} hours and extracted Rs {amount}. By the time we found out, the money was already moved.",

    "I am a {profession} from {city}. Someone called me posing as a police officer. He accused me of being involved in a financial fraud case. He made me join a Skype call where another person dressed as a judge 'ordered' me to deposit money for bail. I lost Rs {amount} in {hours} hours. I couldn't even call my family as they threatened me.",

    "A WhatsApp call came from an unknown number. Caller said he's from cyber crime wing. Accused me of opening fake accounts. Made me sit in front of camera on video call for the entire day. Said if I move or call anyone, police will come. Total transferred: Rs {amount} to {n_accounts} different accounts.",

    "Got an IVR call - 'Press 1 to connect to customer care'. Was connected to someone who said my FD was linked to a fraud case. They transferred me to a 'senior officer' on video call in police uniform. They demanded money for 'clearing my name'. Lost Rs {amount}. Very scared and ashamed to report.",
]

OTHER_FRAUD_NARRATIVES = {
    "KYC Fraud": [
        "I received an SMS saying my bank account would be blocked if I didn't update my KYC. The link looked like my bank's website. After entering my details and UPI PIN, Rs {amount} was debited.",
        "Got a WhatsApp message with a link to update KYC for {bank}. I clicked the link and entered my UPI PIN. Within minutes, Rs {amount} was transferred out.",
        "A person called saying he was from {bank} and my account would be frozen for not completing KYC. He sent an APK file to install. After installing, Rs {amount} was debited without my knowledge.",
    ],
    "QR Code Scam": [
        "I was selling furniture on OLX. The buyer said he'd send money via QR code. When I scanned and entered my PIN, Rs {amount} got deducted from my account instead of being credited.",
        "Someone wanted to buy my laptop. He sent a QR code for 'payment'. When I scanned it, it turned out to be a collect request. Lost Rs {amount} immediately.",
        "A person on Facebook Marketplace asked me to scan a QR code for payment. I scanned it and entered UPI PIN, but Rs {amount} was deducted. The person then blocked me.",
    ],
    "Lottery/Prize Scam": [
        "I received a message saying I won Rs 25 lakh in a KBC lottery. They asked me to pay Rs {amount} as processing fee through UPI. After paying, they kept asking for more money.",
        "Got a WhatsApp message saying I won an Amazon gift card worth Rs 50,000. They asked me to transfer Rs {amount} as registration fee. I paid and they disappeared.",
        "A call from someone saying I'd won Rs 10 lakh in a Jio lucky draw. They needed Rs {amount} for processing. I transferred the amount and never heard back.",
    ],
    "Online Marketplace Fraud": [
        "I ordered a mobile phone from a social media seller for Rs {amount}. They sent a tracking number but the parcel never arrived. The seller's account was deleted.",
        "I paid Rs {amount} for a second-hand iPhone on OLX. The seller took the money via UPI and gave me a box with a brick inside.",
        "Found a deal for a laptop on Instagram. Transferred Rs {amount} via UPI. Seller said 3-day delivery. No delivery, phone number switched off.",
    ],
    "Refund Fraud": [
        "Someone called saying I had a failed transaction and Rs {amount} refund was pending. They asked me to install AnyDesk. After giving access, they transferred Rs {amount} from my account.",
        "Received a call from someone claiming to be from the electricity board. They said I was overcharged and eligible for refund. They sent a QR code. Money got deducted instead. Lost Rs {amount}.",
        "A person called saying my online order was cancelled and I'd get a refund. They sent a UPI collect request instead of refund. I accepted without reading and lost Rs {amount}.",
    ],
    "Investment Scam": [
        "I was added to a Telegram group for stock tips with guaranteed 300% returns. I transferred Rs {amount} via UPI. Initially they showed fake profits but when I tried to withdraw, they demanded more money.",
        "Someone on WhatsApp offered crypto trading with daily returns. I invested Rs {amount} via UPI. The app showed fake profits. When I asked for withdrawal, they said I need to pay tax first.",
        "A friend recommended a 'task-based earning' app. They asked me to invest Rs {amount} for premium tasks. After paying, the app stopped working.",
    ],
    "Loan Fraud": [
        "I applied for a personal loan through an app. They approved Rs 5 lakh but said I need to pay Rs {amount} as processing fee first via UPI. No loan was disbursed. The app disappeared.",
        "Got a call offering instant loan at low interest. They asked for Rs {amount} as stamp duty and insurance fee via UPI. After paying, they kept asking for more charges.",
    ],
    "Job Fraud": [
        "Found a work-from-home job on Telegram. They asked Rs {amount} as registration fee. After payment, they gave few small tasks and then asked for more money for 'higher paying tasks'.",
        "Applied for a data entry job online. Company asked for Rs {amount} as security deposit. They promised to return it with first salary but the company doesn't exist.",
    ],
    "Impersonation Fraud": [
        "Someone hacked my friend's WhatsApp and messaged me asking for Rs {amount} urgently saying they were in hospital. I transferred the money. Later found out the account was hacked.",
        "I received a call from someone pretending to be my cousin. He said he met with an accident and needed Rs {amount} immediately. I transferred the money. My cousin knew nothing about it.",
    ],
}

MODUS_TO_CATEGORY = {
    "digital_arrest": "Digital Arrest Scam",
    "fake_kyc_update": "KYC Fraud",
    "qr_code_scam": "QR Code Scam",
    "lottery_scam": "Lottery/Prize Scam",
    "olx_marketplace_fraud": "Online Marketplace Fraud",
    "fake_refund": "Refund Fraud",
    "investment_scam": "Investment Scam",
    "loan_fraud": "Loan Fraud",
    "job_scam": "Job Fraud",
    "impersonation_scam": "Impersonation Fraud",
}


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def make_upi_id(first, last, idx, bank):
    suffix = BANK_SUFFIX.get(bank, "@upi")
    variant = idx % 4
    if variant == 0:
        return f"{first.lower()}{last.lower()}{idx % 100}{suffix}"
    elif variant == 1:
        return f"{first.lower()}.{last.lower()}{suffix}"
    elif variant == 2:
        return f"{first.lower()}{random.randint(1, 999)}{suffix}"
    else:
        return f"{first[0].lower()}{last.lower()}{idx % 1000}{suffix}"


def make_device_id():
    return "".join(random.choices("0123456789abcdef", k=16))


def make_phone():
    prefix = random.choice([
        "70", "72", "73", "74", "76", "77", "78", "80", "81",
        "82", "83", "85", "86", "87", "88", "89", "90", "91",
        "92", "93", "94", "95", "96", "97", "98", "99",
    ])
    return f"+91{prefix}{''.join(random.choices('0123456789', k=8))}"


def random_ts(start, end):
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.random() * delta)


def make_txn_id():
    return f"UPI{random.randint(100_000_000_000, 999_999_999_999)}"


# ═══════════════════════════════════════════════════════════════════════════════
# 1. GENERATE ACCOUNTS
# ═══════════════════════════════════════════════════════════════════════════════
def generate_accounts():
    print("[1/5] Generating accounts...")
    n_mule = int(NUM_ACCOUNTS * MULE_FRACTION)
    n_legit = NUM_ACCOUNTS - n_mule
    rows = []

    for i in range(NUM_ACCOUNTS):
        is_mule = i >= n_legit
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        bank = random.choice(BANKS)
        upi_id = make_upi_id(first, last, i, bank)
        device_id = make_device_id()

        if is_mule:
            if random.random() < 0.7:  # 70% brand-new
                creation_date = DATE_END - timedelta(days=random.randint(1, 30))
                is_dormant = False
                days_inactive = random.randint(0, 5)
            else:  # 30% reactivated dormant
                creation_date = DATE_START - timedelta(days=random.randint(200, 800))
                is_dormant = True
                days_inactive = random.randint(90, 400)
            acct_type = "personal"
        else:
            creation_date = DATE_END - timedelta(days=random.randint(180, 2000))
            is_dormant = random.random() < 0.05
            days_inactive = (random.randint(0, 30) if not is_dormant
                             else random.randint(60, 365))
            acct_type = random.choices(["personal", "business"],
                                       weights=[0.85, 0.15])[0]

        account_age = (DATE_END - creation_date).days
        rows.append({
            "account_id": f"ACC{i:06d}",
            "upi_id": upi_id,
            "device_id": device_id,
            "phone": make_phone(),
            "creation_date": creation_date.strftime("%Y-%m-%d"),
            "account_type": acct_type,
            "bank_name": bank,
            "is_dormant": is_dormant,
            "days_since_last_activity": days_inactive,
            "account_age_days": account_age,
            "is_mule": is_mule,
        })

    df = pd.DataFrame(rows)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    print(f"  ✓ {len(df):,} accounts ({n_mule} mule)")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. GENERATE SCAM EPISODES  (transactions + calls)
# ═══════════════════════════════════════════════════════════════════════════════
def generate_scam_episodes(accounts_df):
    print("[2/5] Generating scam episodes...")
    mules = accounts_df[accounts_df["is_mule"]].reset_index(drop=True)
    legit = accounts_df[~accounts_df["is_mule"]].reset_index(drop=True)

    scam_txns, scam_calls, episodes_meta = [], [], []
    victim_indices = np.random.choice(len(legit), size=NUM_SCAM_EPISODES,
                                      replace=True)

    for ep in range(NUM_SCAM_EPISODES):
        episode_id = f"EP{ep:04d}"
        victim = legit.iloc[victim_indices[ep]]

        n_mules_ep = random.randint(3, 8)
        mule_idx = np.random.choice(len(mules), size=n_mules_ep, replace=False)
        ep_mules = mules.iloc[mule_idx]

        ep_start = random_ts(DATE_START, DATE_END - timedelta(hours=8))
        ep_hours = random.uniform(1, 6)
        n_txns = random.randint(5, 15)

        offsets = sorted(np.random.uniform(0, ep_hours * 3600, size=n_txns))
        mule_upi_set = set()

        for t_i, offset in enumerate(offsets):
            ts = ep_start + timedelta(seconds=offset)
            mule_row = ep_mules.iloc[t_i % n_mules_ep]

            if random.random() < 0.85:
                amount = random.choice(SCAM_AMOUNTS)
            else:
                amount = random.choice(SCAM_AMOUNTS) + random.randint(-500, 500)

            scam_txns.append({
                "txn_id": make_txn_id(),
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "sender_upi_id": victim["upi_id"],
                "receiver_upi_id": mule_row["upi_id"],
                "amount": amount,
                "txn_type": "P2P",
                "sender_bank": victim["bank_name"],
                "receiver_bank": mule_row["bank_name"],
                "sender_device_id": victim["device_id"],
                "receiver_device_id": mule_row["device_id"],
                "receiver_account_age_days": mule_row["account_age_days"],
                "is_first_txn_for_receiver": int(mule_row["upi_id"]
                                                  not in mule_upi_set),
                "is_scam_episode": 1,
                "scam_episode_id": episode_id,
            })
            mule_upi_set.add(mule_row["upi_id"])

        # ── Calls for this episode ──
        n_calls = random.randint(2, 5)
        first_txn_ts = ep_start + timedelta(seconds=offsets[0])
        call_start = first_txn_ts - timedelta(
            minutes=random.uniform(30, 60))
        caller_phone = make_phone()

        for c_i in range(n_calls):
            call_ts = call_start + timedelta(
                minutes=c_i * random.uniform(5, 30))
            scam_calls.append({
                "call_id": f"CALL{len(scam_calls):06d}",
                "timestamp": call_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "caller_number": caller_phone,
                "callee_number": victim["phone"],
                "duration_seconds": random.randint(1800, 7200),
                "call_type": random.choices(
                    CALL_TYPES, weights=[0.3, 0.3, 0.4])[0],
                "is_international": random.random() < 0.15,
            })

        total_amt = sum(t["amount"] for t in scam_txns
                        if t["scam_episode_id"] == episode_id)
        episodes_meta.append({
            "episode_id": episode_id,
            "victim_upi_id": victim["upi_id"],
            "victim_phone": victim["phone"],
            "victim_city": random.choice(INDIAN_CITIES),
            "victim_age": random.randint(22, 78),
            "n_mules": n_mules_ep,
            "n_txns": n_txns,
            "total_amount": total_amt,
            "mule_upi_ids": ";".join(sorted(mule_upi_set)),
            "episode_start": ep_start.strftime("%Y-%m-%d %H:%M:%S"),
        })

    print(f"  ✓ {NUM_SCAM_EPISODES} episodes → "
          f"{len(scam_txns):,} scam txns, {len(scam_calls):,} scam calls")
    return scam_txns, scam_calls, episodes_meta


# ═══════════════════════════════════════════════════════════════════════════════
# 3. GENERATE NORMAL TRANSACTIONS  (vectorised)
# ═══════════════════════════════════════════════════════════════════════════════
def generate_normal_transactions(accounts_df, target_count):
    print(f"[3/5] Generating {target_count:,} normal transactions...")
    legit = accounts_df[~accounts_df["is_mule"]].reset_index(drop=True)
    n = len(legit)

    s_idx = np.random.randint(0, n, size=target_count)
    r_idx = np.random.randint(0, n, size=target_count)
    same = s_idx == r_idx
    r_idx[same] = (r_idx[same] + 1) % n

    # Log-normal amounts centred ~₹1800  (ln(1800) ≈ 7.495)
    amounts = np.random.lognormal(mean=7.495, sigma=1.0, size=target_count)
    amounts = np.clip(amounts, 1, 500_000)
    round_mask = np.random.random(target_count) < 0.08
    amounts[round_mask] = np.random.choice(ROUND_NORMAL_AMTS,
                                            size=round_mask.sum())
    amounts = np.round(amounts, 2)

    total_secs = (DATE_END - DATE_START).total_seconds()
    offsets = np.random.uniform(0, total_secs, size=target_count)
    base = pd.Timestamp(DATE_START)
    timestamps = base + pd.to_timedelta(offsets, unit="s")

    txn_types = np.random.choice(TXN_TYPES, size=target_count,
                                  p=TXN_TYPE_WEIGHTS)
    txn_nums = np.random.randint(100_000_000_000, 999_999_999_999,
                                  size=target_count, dtype=np.int64)

    senders = legit.iloc[s_idx]
    receivers = legit.iloc[r_idx]

    df = pd.DataFrame({
        "txn_id": ["UPI" + str(x) for x in txn_nums],
        "timestamp": timestamps.strftime("%Y-%m-%d %H:%M:%S"),
        "sender_upi_id": senders["upi_id"].values,
        "receiver_upi_id": receivers["upi_id"].values,
        "amount": amounts,
        "txn_type": txn_types,
        "sender_bank": senders["bank_name"].values,
        "receiver_bank": receivers["bank_name"].values,
        "sender_device_id": senders["device_id"].values,
        "receiver_device_id": receivers["device_id"].values,
        "receiver_account_age_days": receivers["account_age_days"].values,
        "is_first_txn_for_receiver": 0,
        "is_scam_episode": 0,
        "scam_episode_id": "",
    })

    print(f"  ✓ {len(df):,} normal transactions")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 4. GENERATE CALLS  (fill remaining with normal calls)
# ═══════════════════════════════════════════════════════════════════════════════
def generate_normal_calls(existing_count):
    target = NUM_TOTAL_CALLS - existing_count
    print(f"[4/5] Generating {target:,} normal calls...")
    rows = []
    for i in range(target):
        ts = random_ts(DATE_START, DATE_END)
        rows.append({
            "call_id": f"CALL{existing_count + i:06d}",
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "caller_number": make_phone(),
            "callee_number": make_phone(),
            "duration_seconds": random.randint(30, 300),
            "call_type": random.choices(
                CALL_TYPES, weights=[0.7, 0.15, 0.15])[0],
            "is_international": random.random() < 0.03,
        })
    print(f"  ✓ {len(rows):,} normal calls")
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# 5. GENERATE COMPLAINTS
# ═══════════════════════════════════════════════════════════════════════════════
def _fill_narrative(template, **kwargs):
    """Fill template placeholders, ignoring missing keys."""
    result = template
    for k, v in kwargs.items():
        result = result.replace("{" + k + "}", str(v))
    return result


def generate_complaints(episodes_meta):
    print("[5/5] Generating complaints...")
    n_digital = int(NUM_COMPLAINTS * 0.40)   # ~40%
    n_other   = NUM_COMPLAINTS - n_digital    # ~60%

    rows = []
    complaint_id = 0

    # ── Digital arrest complaints ──
    for i in range(n_digital):
        ep = episodes_meta[i % len(episodes_meta)]
        template = random.choice(DIGITAL_ARREST_NARRATIVES)
        call_type_str = random.choice([
            "WhatsApp video call", "Skype video call", "video call"])
        fake_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

        narrative = _fill_narrative(
            template,
            agency=random.choice(AGENCIES),
            call_type=call_type_str,
            hours=str(random.randint(2, 8)),
            amount=f"{ep['total_amount']:,.0f}",
            n_accounts=str(ep["n_mules"]),
            fake_name=fake_name,
            victim_age=str(ep["victim_age"]),
            profession=random.choice(PROFESSIONS),
            city=ep["victim_city"],
            date=ep["episode_start"][:10],
            bank=random.choice(BANKS),
        )

        ts = datetime.strptime(ep["episode_start"], "%Y-%m-%d %H:%M:%S") \
             + timedelta(hours=random.randint(1, 72))

        rows.append({
            "complaint_id": f"CMP{complaint_id:05d}",
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "category": "Digital Arrest Scam",
            "narrative": narrative,
            "amount_lost": ep["total_amount"],
            "accused_upi_ids": ep["mule_upi_ids"],
            "victim_city": ep["victim_city"],
            "victim_age": ep["victim_age"],
            "modus_operandi": "digital_arrest",
            "status": random.choice(COMPLAINT_STATUS),
        })
        complaint_id += 1

    # ── Other fraud complaints ──
    other_types = list(OTHER_FRAUD_NARRATIVES.keys())
    for i in range(n_other):
        fraud_type = random.choice(other_types)
        template = random.choice(OTHER_FRAUD_NARRATIVES[fraud_type])
        amount = random.choice([500, 1000, 2000, 5000, 8000, 10_000,
                                15_000, 20_000, 25_000, 50_000, 75_000])
        narrative = _fill_narrative(
            template,
            amount=f"{amount:,}",
            bank=random.choice(BANKS),
        )
        ts = random_ts(DATE_START, DATE_END)

        # Find matching modus operandi key
        modus = [k for k, v in MODUS_TO_CATEGORY.items()
                 if v == fraud_type][0]

        # Generate 1-3 random accused UPI IDs
        n_accused = random.randint(1, 3)
        accused = [make_upi_id(random.choice(FIRST_NAMES),
                                random.choice(LAST_NAMES),
                                random.randint(0, 9999),
                                random.choice(BANKS))
                   for _ in range(n_accused)]

        rows.append({
            "complaint_id": f"CMP{complaint_id:05d}",
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "category": fraud_type,
            "narrative": narrative,
            "amount_lost": amount,
            "accused_upi_ids": ";".join(accused),
            "victim_city": random.choice(INDIAN_CITIES),
            "victim_age": random.randint(18, 75),
            "modus_operandi": modus,
            "status": random.choice(COMPLAINT_STATUS),
        })
        complaint_id += 1

    df = pd.DataFrame(rows)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    print(f"  ✓ {len(df):,} complaints ({n_digital} digital arrest, "
          f"{n_other} other)")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  KAVACH — Synthetic UPI Transaction Data Generator")
    print("=" * 60)

    # 1. Accounts
    accounts_df = generate_accounts()

    # 2. Scam episodes → scam txns + scam calls
    scam_txns, scam_calls, episodes_meta = generate_scam_episodes(accounts_df)

    # 3. Normal transactions (fill to ~100k total)
    n_normal = NUM_TOTAL_TXNS - len(scam_txns)
    normal_txns_df = generate_normal_transactions(accounts_df, n_normal)

    # 4. Calls (fill to ~10k total)
    normal_calls = generate_normal_calls(len(scam_calls))

    # 5. Complaints
    complaints_df = generate_complaints(episodes_meta)

    # ── Combine & save ────────────────────────────────────────────────────
    print("\nSaving CSV files...")

    # Transactions
    scam_txns_df = pd.DataFrame(scam_txns)
    txns_df = pd.concat([normal_txns_df, scam_txns_df], ignore_index=True)
    txns_df = txns_df.sort_values("timestamp").reset_index(drop=True)
    txns_path = os.path.join(OUTPUT_DIR, "transactions.csv")
    txns_df.to_csv(txns_path, index=False)
    print(f"  ✓ transactions.csv  — {len(txns_df):,} rows")

    # Accounts (output subset of columns matching spec)
    acct_out = accounts_df[[
        "account_id", "creation_date", "account_type", "bank_name",
        "is_dormant", "days_since_last_activity", "is_mule"
    ]]
    acct_path = os.path.join(OUTPUT_DIR, "accounts.csv")
    acct_out.to_csv(acct_path, index=False)
    print(f"  ✓ accounts.csv      — {len(acct_out):,} rows")

    # Calls
    all_calls = scam_calls + normal_calls
    calls_df = pd.DataFrame(all_calls)
    calls_df = calls_df.sort_values("timestamp").reset_index(drop=True)
    calls_out = calls_df[[
        "call_id", "timestamp", "caller_number", "callee_number",
        "duration_seconds", "call_type", "is_international"
    ]]
    calls_path = os.path.join(OUTPUT_DIR, "calls.csv")
    calls_out.to_csv(calls_path, index=False)
    print(f"  ✓ calls.csv         — {len(calls_out):,} rows")

    # Complaints
    complaints_path = os.path.join(OUTPUT_DIR, "complaints.csv")
    complaints_df.to_csv(complaints_path, index=False)
    print(f"  ✓ complaints.csv    — {len(complaints_df):,} rows")

    # ── Summary stats ─────────────────────────────────────────────────────
    scam_pct = len(scam_txns_df) / len(txns_df) * 100
    avg_ep_amt = np.mean([e["total_amount"] for e in episodes_meta])
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Transactions : {len(txns_df):>10,}  "
          f"(scam: {len(scam_txns_df):,} = {scam_pct:.1f}%)")
    print(f"  Accounts     : {len(acct_out):>10,}  "
          f"(mule: {accounts_df['is_mule'].sum()})")
    print(f"  Calls        : {len(calls_out):>10,}  "
          f"(scam-linked: {len(scam_calls):,})")
    print(f"  Complaints   : {len(complaints_df):>10,}")
    print(f"  Episodes     : {len(episodes_meta):>10,}  "
          f"(avg amount: ₹{avg_ep_amt:,.0f})")
    print(f"\n  Output dir   : {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
