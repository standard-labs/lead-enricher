"""
Streamlit app to enrich leads from a CSV upload using the internal enrichment API.

Required environment variables:
    API_BASE_URL: Base URL of the internal tools API (e.g. https://my-api.run.app)
    ADMIN_KEY: Admin key for API authentication

Run with: streamlit run enrich_leads.py
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import streamlit as st

API_BASE_URL = st.secrets.get("API_BASE_URL", "").rstrip("/")
ADMIN_KEY = st.secrets.get("ADMIN_KEY", "")

HEADERS = {"token": ADMIN_KEY}
ENRICH_PHONE_URL = f"{API_BASE_URL}/intent/enrich/phone"
ENRICH_EMAIL_URL = f"{API_BASE_URL}/intent/enrich/email"


def enrich_row(row: dict) -> dict:
    """Try to enrich a single row via phone then email. Returns enrichment fields or empty dict."""
    phone = str(row.get("Phone", "")).strip()
    email = str(row.get("Email", "")).strip()

    # Try phone enrichment first
    if phone:
        try:
            print(f"📞 Requesting phone enrichment for: {phone}")
            resp = requests.post(ENRICH_PHONE_URL, json={"phone": phone}, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                result = resp.json()["data"]
                print(f"✅ Phone enrichment successful for {phone}: {len(result)} fields returned")
                return result
            else:
                print(f"❌ Phone enrichment failed for {phone}: Status {resp.status_code}")
        except requests.RequestException as e:
            print(f"❌ Phone enrichment error for {phone}: {str(e)}")
            pass

    # Fall back to email enrichment
    if email:
        try:
            print(f"📧 Requesting email enrichment for: {email}")
            resp = requests.post(ENRICH_EMAIL_URL, json={"email": email}, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                result = resp.json()["data"]
                print(f"✅ Email enrichment successful for {email}: {len(result)} fields returned")
                return result
            else:
                print(f"❌ Email enrichment failed for {email}: Status {resp.status_code}")
        except requests.RequestException as e:
            print(f"❌ Email enrichment error for {email}: {str(e)}")
            pass

    print(f"⚠️  No enrichment data found for phone={phone}, email={email}")
    return {}


def flatten_pii(pii: dict) -> dict:
    """Flatten a PII response dict into simple columns for the CSV export."""
    if not pii:
        return {}

    flat: dict = {}

    for key, value in pii.items():
        # Flatten email array
        if key == "emails":
            for i, em in enumerate(value[:3], start=1):
                flat[f"enriched_email_{i}"] = em
            continue

        # Flatten mobile phones list
        if key == "mobile_phones":
            for i, phone_obj in enumerate(value[:3], start=1):
                if isinstance(phone_obj, dict):
                    flat[f"enriched_phone_{i}"] = phone_obj.get("phone", "")
                    flat[f"enriched_phone_{i}_dnc"] = phone_obj.get("do_not_call", "")
                else:
                    flat[f"enriched_phone_{i}"] = str(phone_obj)
            continue

        # Skip the internal id
        if key == "id":
            continue

        flat[f"enriched_{key}"] = value

    return flat


# ---- Streamlit UI ----

st.set_page_config(page_title="Lead Enrichment", layout="wide")
st.title("📇 Lead Enrichment")

if not API_BASE_URL or not ADMIN_KEY:
    st.error("Set **API_BASE_URL** and **ADMIN_KEY** environment variables before running.")
    st.stop()

uploaded_file = st.file_uploader("Upload a CSV with **Phone** and **Email** columns", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, dtype=str).fillna("")

    missing = [col for col in ("Phone", "Email") if col not in df.columns]
    if missing:
        st.error(f"CSV is missing required column(s): **{', '.join(missing)}**")
        st.stop()

    st.subheader("Preview (first 5 rows)")
    st.dataframe(df.head())

    if st.button("🚀 Enrich Leads"):
        rows = df.to_dict(orient="records")
        enriched: list[dict] = [{}] * len(rows)
        progress = st.progress(0, text="Enriching...")
        done = 0

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_idx = {executor.submit(enrich_row, row): idx for idx, row in enumerate(rows)}

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    enriched[idx] = flatten_pii(future.result())
                except Exception as exc:
                    enriched[idx] = {"enriched_error": str(exc)}

                done += 1
                progress.progress(done / len(rows), text=f"Enriching... {done}/{len(rows)}")

        progress.progress(1.0, text="Done!")

        enriched_df = pd.DataFrame(enriched)
        result_df = pd.concat([df, enriched_df], axis=1)

        matched = enriched_df.apply(lambda r: any(v != "" and pd.notna(v) for v in r), axis=1).sum()
        st.success(f"Enrichment complete — **{matched}** of **{len(rows)}** rows matched.")

        st.subheader("Enriched Data")
        st.dataframe(result_df)

        csv_bytes = result_df.to_csv(index=False).encode()
        st.download_button("⬇️ Download Enriched CSV", csv_bytes, "enriched_leads.csv", "text/csv")
