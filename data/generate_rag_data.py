#!/usr/bin/env python3
"""
generate_rag_data.py — Synthetic Data Generator for RAG Pipeline

Generates two CSV files:
1. complaints_rag.csv (500 rows): Rich, 100-300 word narratives 
   with specified categories and attributes.
2. government_texts.csv (~50 rows): Public text excerpts from PIB, MHA, 
   Wikipedia, and NCRB to serve as factual RAG context.

Usage:  python generate_rag_data.py
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta

# Configuration
SEED = 42
random.seed(SEED)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
NUM_COMPLAINTS = 500

CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune", 
    "Chennai", "Kolkata", "Ahmedabad", "Lucknow", "Jaipur"
]

AGENCIES = ["CBI", "ED", "TRAI", "RBI", "Customs", "Police"]
CALL_TYPES = ["video call", "WhatsApp video call", "Skype call", "Zoom call"]
TRANSFER_METHODS = ["UPI", "bank transfer", "IMPS/NEFT", "crypto exchange transfer", "RTGS"]

# ==============================================================================
# 1. NARRATIVE TEMPLATES (For complaints_rag.csv)
# ==============================================================================

# Digital Arrest Templates (Need to be verbose, 100-300 words)
DA_TEMPLATES = [
    "On {date}, I was at my residence in {city} when I received an unexpected phone call at around {time}. The caller identified themselves as a senior investigating officer from the {agency}. The person sounded extremely authoritative and informed me that my Aadhaar card and PAN card details had been found implicated in a major money laundering and contraband smuggling case involving millions of rupees. I am a {age}-year-old citizen with no criminal record, and I was completely terrified by this sudden allegation. They immediately escalated the situation by ordering me to isolate myself in an empty room and forcing me to join a {call_type}. Once connected, I saw individuals dressed in official-looking uniforms sitting in a room that closely resembled a real police station, complete with national emblems and official insignia. They declared that I was under 'digital arrest' and strictly prohibited me from muting the microphone, turning off the camera, or contacting any family members or legal counsel. They displayed highly realistic forged documents bearing my name, photograph, and official government stamps, claiming an arrest warrant had already been authorized by the Supreme Court. I was kept under constant surveillance on this harrowing call for over {duration} hours without any break. Exploiting my extreme fear, confusion, and panic, the impersonators instructed me to transfer all my personal savings to a 'secret secure {agency} investigation account' to verify my innocence. They promised the entire amount would be reversed back to my account within 24 hours once my financial audit was clear. Out of sheer panic, I initiated multiple transactions totaling Rs. {amount} via {transfer_method}. It was only after the perpetrators abruptly disconnected the call the next evening that I contacted the actual local police station and realized I had been the victim of an elaborate and devastating psychological scam.",
    
    "I am filing this formal complaint regarding a highly sophisticated cyber fraud that occurred on {date}. I am a {age}-year-old resident of {city}. I received an automated Interactive Voice Response (IVR) call claiming that a mobile number registered in my name was being used to send illegal threat messages and that my cellular services would be blocked in two hours. I pressed '9' to speak to an executive and was connected to a man claiming to represent {agency}. He transferred my call to what he described as the 'cyber crime investigation branch'. I was then manipulated into logging on to a {call_type} for formal questioning. The individuals on the screen were wearing uniforms and sitting behind heavy wooden desks with the {agency} logo prominently displayed in the background. They informed me that multiple bank accounts had been opened using my PAN card to fund illicit activities across international borders. They convinced me that I was a prime suspect and placed me under a so-called 'digital arrest'. They claimed that my local physical movements were currently being tracked and any attempt to log off would result in immediate physical detention by a local raid team. Under incredible mental distress and duress spanning {duration} hours, they convinced me that the only way to avoid formal imprisonment was to deposit a 'security clearance bond'. Desperate to defend my reputation and believing their threats were completely legitimate, I gathered funds by liquidating my fixed deposits and transferred Rs. {amount} using {transfer_method} to the accounts they dictated. The scammers assured me the money was just held in an escrow account. I am left financially ruined and deeply traumatized by this impersonation scam.",
    
    "My nightmare began on the morning of {date} in {city}. I received an urgent call from someone pretending to be a customs official at the international airport. They claimed that a parcel addressed to me contained illegal narcotics and multiple expired passports. When I denied any knowledge of the parcel, they stated the matter was being escalated to the {agency} and forced me to immediately download an application to initiate a {call_type}. I am {age} years old and not very tech-savvy, so I blindly followed their instructions out of fear of legal trouble. Upon joining the call, a person dressed as a senior {agency} director interrogated me aggressively. They digitally projected a fake FIR document onto the screen, which had my exact residential address and identity details. They told me I could not leave the camera frame and declared me under 'digital arrest'. I was held hostage virtually for {duration} straight hours. Whenever I requested to drink water or use the restroom, they threatened to send local constables to publicly arrest me in front of my neighbors. They convinced me to temporarily transfer Rs. {amount} to an 'auditor verification ledger' via {transfer_method} to prove that my funds were legally acquired and not linked to the intercepted parcel. Believing my life and career were on the line, I complied with all their demands. After the initial transfer, they demanded further payments, at which point I became suspicious and disconnected. I immediately checked with my bank, but the funds had already been moved."
]

# Other Categories Templates
OTHER_TEMPLATES = {
    "upi_phishing": [
        "On {date}, I was trying to sell some old furniture online. A buyer contacted me and agreed to the price. However, instead of sending money, they sent a QR code and a link via WhatsApp. I clicked the link, and it asked for my UPI PIN. Thinking it was required to receive the payment, I entered my PIN. Instantly, Rs. {amount} was deducted from my account. I live in {city} and I'm {age} years old.",
        "I received an SMS stating that my electricity bill for my {city} residence was unpaid and power would be disconnected tonight. The message contained a payment link. I clicked it and was redirected to a payment gateway that looked legitimate. I entered my UPI details, and Rs. {amount} was withdrawn automatically. The sender's number is now switched off."
    ],
    "qr_code_scam": [
        "While visiting a local shop in {city}, I tried to pay using a QR code pasted on the wall. After scanning, I made a payment of Rs. {amount}. Later, the shopkeeper said he didn't receive it. It turns out someone had pasted a fake QR code sticker over the original one. I lost my money and had to pay the shopkeeper again.",
        "A person approached me saying he had urgent cash but needed an online transfer. He gave me Rs. 2000 in physical cash and asked me to scan his QR code to transfer the equivalent amount. I scanned and authorized Rs. {amount}, but soon realized the cash he handed me were counterfeit notes. I am a {age}-year-old resident of {city}."
    ],
    "fake_customer_care": [
        "I was facing issues with my broadband connection in {city}. I googled the customer care number and called the first result. The person on the other end asked me to install a remote access app called AnyDesk to fix the router settings. Within minutes of granting access, they opened my banking app and transferred Rs. {amount} out of my account. I am {age} years old.",
        "My flight was cancelled and I was looking for an airline refund. I found a toll-free number online. The 'agent' told me to pay a small processing fee of Rs. 10 to receive the full refund. They sent a link, but the transaction actually debited Rs. {amount} from my savings. I reported it to the local police in {city}."
    ],
    "investment_fraud": [
        "I was added to a Telegram group promising 300% returns on crypto investments. The members shared fake screenshots of huge profits. I am {age} years old and seeking financial stability in {city}. I invested an initial sum, which showed a profit on their fake portal. Encouraged, I invested a total of Rs. {amount}. When I tried to withdraw, they blocked my account and vanished.",
        "An Instagram influencer promoted a stock trading platform that uses 'AI algorithms' for guaranteed profits. I created an account and transferred Rs. {amount} via bank transfer. Over a few weeks, the dashboard showed my portfolio tripling in value. However, any withdrawal request prompted a demand for 30% 'tax payments' upfront. It was an elaborate Ponzi scheme."
    ],
    "loan_app_fraud": [
        "I downloaded a quick loan app from the Play Store to cover a medical emergency in {city}. I applied for a 5,000 rupee loan but they only disbursed 3,000, claiming the rest was processing fees. A week later, they demanded Rs. {amount} and threatened to send morphed obscene photos of me to all my contacts using the contact list permission I had originally granted. The harassment was unbearable.",
        "I am a {age}-year-old from {city}. I took a small loan from a digital lending app. I repaid it on time, but they claimed the payment failed and added exorbitant late fees. They started calling my relatives and colleagues, accusing me of being a thief. Forced by the immense social humiliation and continuous threats, I ended up paying Rs. {amount} to these blackmailers."
    ]
}

def generate_rag_complaints():
    rows = []
    
    # 1. Digital Arrest (200 rows)
    for i in range(200):
        template = random.choice(DA_TEMPLATES)
        city = random.choice(CITIES)
        age = random.randint(25, 70)
        agency = random.choice(AGENCIES)
        call_type = random.choice(CALL_TYPES)
        transfer_method = random.choice(TRANSFER_METHODS)
        amount = random.randint(50_000, 50_00_000)
        duration = random.randint(2, 48)
        
        # Random date in 2024
        start_date = datetime(2024, 1, 1)
        r_dt = start_date + timedelta(days=random.randint(0, 360))
        date_str = r_dt.strftime("%B %d, %Y")
        time_str = f"{random.randint(8, 20):02d}:{random.randint(0, 59):02d}"
        
        narrative = template.format(
            date=date_str, city=city, time=time_str, agency=agency,
            age=age, call_type=call_type, duration=duration,
            amount=f"{amount:,}", transfer_method=transfer_method
        )
        
        tags = f"digital_arrest, impersonation, {agency.lower()}, {call_type.replace(' ', '_').lower()}, coercion"
        
        rows.append({
            "complaint_id": f"RAG_CMP_{len(rows)+1:04d}",
            "category": "digital_arrest",
            "narrative": narrative,
            "amount_lost": amount,
            "victim_city": city,
            "modus_operandi_tags": tags
        })

    # 2. Other Categories (300 rows, ~60 each)
    other_cats = ["upi_phishing", "qr_code_scam", "fake_customer_care", "investment_fraud", "loan_app_fraud"]
    for cat in other_cats:
        for _ in range(60):
            template = random.choice(OTHER_TEMPLATES[cat])
            city = random.choice(CITIES)
            age = random.randint(20, 75)
            amount = random.randint(1_000, 5_00_000)
            
            start_date = datetime(2024, 1, 1)
            r_dt = start_date + timedelta(days=random.randint(0, 360))
            date_str = r_dt.strftime("%B %d, %Y")
            
            narrative = template.format(date=date_str, city=city, age=age, amount=f"{amount:,}")
            tags = f"{cat}, financial_loss, cybercrime"
            
            rows.append({
                "complaint_id": f"RAG_CMP_{len(rows)+1:04d}",
                "category": cat,
                "narrative": narrative,
                "amount_lost": amount,
                "victim_city": city,
                "modus_operandi_tags": tags
            })
            
    random.shuffle(rows)
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "complaints_rag.csv"), index=False)
    print(f"Generated complaints_rag.csv with {len(df)} rows.")

# ==============================================================================
# 2. GOVERNMENT TEXTS (For government_texts.csv)
# ==============================================================================

BASE_GOVERNMENT_TEXTS = [
    {
        "source": "PIB Press Release",
        "title": "Ministry of Home Affairs Issues Advisory on 'Digital Arrest' Scams",
        "template": "The Ministry of Home Affairs (MHA) has issued a nationwide alert regarding a surge in cybercrimes termed as 'Digital Arrests'. Fraudsters are impersonating officials from law enforcement agencies, including the {agency}. These criminals typically contact victims via {call_type}, falsely claiming that the victim's identity has been used in illegal activities such as money laundering or drug trafficking. The scammers use intimidating tactics, including forged arrest warrants and fake police station backgrounds, to coerce victims into compliance."
    },
    {
        "source": "MHA Cybercrime Advisory",
        "title": "Citizen Advisory: Avoiding Video Call Intimidation Tactics",
        "template": "The Indian Cyber Crime Coordination Centre (I4C) under the MHA has noted an alarming trend. Citizens are receiving unsolicited video calls where imposters, dressed in uniforms resembling the {agency}, claim the citizen is under 'digital arrest'. The MHA strongly reiterates that NO Indian law enforcement agency utilizes {call_type} to interrogate suspects, demand instant financial transfers via {transfer_method}, or place individuals under virtual confinement. Citizens are advised to immediately report such instances on the National Cyber Crime Reporting Portal (www.cybercrime.gov.in) or by dialing the helpline number 1930."
    },
    {
        "source": "Wikipedia Excerpt",
        "title": "Digital Arrest (Cybercrime)",
        "template": "A 'Digital Arrest' refers to an extortion and cyber fraud modus operandi primarily observed in South Asia, notably India. In this scheme, perpetrators utilize social engineering to convince victims they are subjects of an active criminal investigation. A common technique involves a prolonged {call_type} where the victim is psychologically pressured by actors posing as personnel from {agency}. The ultimate goal is financial extortion, usually demanding funds to be transferred via {transfer_method} to fictitious 'secure accounts' managed by mule operators."
    },
    {
        "source": "NCRB Report Summary",
        "title": "Annual Cybercrime Trends: Rise in Impersonation Fraud",
        "template": "The National Crime Records Bureau (NCRB) has documented a {percent}% year-on-year increase in sophisticated cyber-impersonation cases. The data highlights that financial hubs such as {city} and surrounding metropolitan areas are prime targets. A significant portion of the financial losses, often transferred through {transfer_method}, are attributed to criminals posing as {agency} officials. The NCRB emphasizes the need for extensive public awareness campaigns, particularly focusing on the illegality of conceptual 'digital arrests' executed over {call_type} mediums."
    },
    {
        "source": "PIB Press Release",
        "title": "RBI Clarification on 'Secret Audits' and Virtual Escrow Accounts",
        "template": "The Reserve Bank of India (RBI) categorically refutes claims made by fraudsters regarding 'secret audit accounts' or 'RBI clearance ledgers'. It has come to the central bank's notice that scammers, falsely identifying themselves as officers of the {agency}, are forcing individuals into {call_type}s. They demand immediate deposits via {transfer_method} to clear their names from fabricated charges. The RBI clarifies that it NEVER contacts citizens directly demanding money or personal financial details under the guise of legal or regulatory audits in {city} or anywhere else."
    }
]

def generate_government_texts():
    rows = []
    
    # We will generate 50 rows by cycling through the base texts and randomizing the placeholders
    for i in range(50):
        base = BASE_GOVERNMENT_TEXTS[i % len(BASE_GOVERNMENT_TEXTS)]
        
        agency = random.choice(["Central Bureau of Investigation (CBI)", "Enforcement Directorate (ED)", "Telecom Regulatory Authority of India (TRAI)", "Narcotics Control Bureau (NCB)", "Customs Department"])
        call_type = random.choice(["Skype video calls", "WhatsApp video communications", "virtual meeting platforms", "direct phone and video contact"])
        transfer_method = random.choice(["Unified Payments Interface (UPI)", "Immediate Payment Service (IMPS)", "Real Time Gross Settlement (RTGS)", "cryptocurrency channels"])
        city = random.choice(CITIES)
        percent = random.randint(15, 85)
        
        text = base["template"].format(
            agency=agency, call_type=call_type, percent=percent, 
            transfer_method=transfer_method, city=city
        )
        
        doc_id = f"GOV_DOC_{i+1:03d}"
        
        rows.append({
            "doc_id": doc_id,
            "source": base["source"],
            "title": base["title"] + (f" (Update {i+1})" if i >= len(BASE_GOVERNMENT_TEXTS) else ""),
            "text": text
        })
        
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "government_texts.csv"), index=False)
    print(f"Generated government_texts.csv with {len(df)} rows.")


if __name__ == "__main__":
    generate_rag_complaints()
    generate_government_texts()
    print("Done!")
