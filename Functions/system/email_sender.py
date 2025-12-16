import os
import base64
import requests
from dotenv import load_dotenv
import msal

import logging
logging.getLogger("msal").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("uvicorn").setLevel(logging.INFO)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)



# ==========================================
# 🔧 Load environment variables
# ==========================================
ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)
ENV_PATH = os.path.join(ROOT_DIR, ".env")

load_dotenv(ENV_PATH)
print("Loaded ENV from:", ENV_PATH)


OUTLOOK_SENDER_EMAIL = os.getenv("OUTLOOK_SENDER_EMAIL")
OUTLOOK_CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
OUTLOOK_TENANT_ID = os.getenv("OUTLOOK_TENANT_ID")
OUTLOOK_CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")

print("───────────────────────────────────────────────")
print(OUTLOOK_SENDER_EMAIL)
print(OUTLOOK_CLIENT_ID)
print(OUTLOOK_TENANT_ID)
print(OUTLOOK_CLIENT_SECRET)
print("hallo")

authority = f"https://login.microsoftonline.com/{OUTLOOK_TENANT_ID}"
scopes = ["https://graph.microsoft.com/.default"]

# ==========================================
# 🌍 Environment & globale recipients
# ==========================================
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
IS_LOCAL = ENVIRONMENT in ["dev", "development", "local"]

if IS_LOCAL:
    RECIPIENTS = ["jovan@treecap.nl"]
    print("🧪 LOCAL MODE – mails gaan alleen naar jovan@treecap.nl")
else:
    RECIPIENTS = None  # later overschrijven met DB-resultaten
    print("🚀 PRODUCTION MODE – mails gaan naar echte ontvangers")


# ==========================================
# 📧 Outlook e-mail functie
# ==========================================
async def send_outlook_email(
    subject: str,
    body: str,
    recipients: list[str] | None = None,
    cc: list[str] | None = None,
    bcc: list[str] | None = None,
    attachments: list[str] | None = None,
    inline_images: dict[str, str] | None = None,
):
    """
    📧 Verstuur Outlook e-mails waarbij *iedereen* individueel gemaild wordt
    (volledige BCC-simulatie: niemand ziet elkaars adres).
    """

    # 👀 Gebruik globale recipients als er geen lijst is doorgegeven
    global RECIPIENTS
    if recipients is None:
        recipients = RECIPIENTS or []

    # 🔒 Local override
    if IS_LOCAL:
        print("🧪 LOCAL MODE actief – alleen mail naar jovan@treecap.nl")
        recipients = ["jovan@treecap.nl"]
        cc = None
        bcc = None

    everyone = (recipients or []) + (cc or []) + (bcc or [])
    if not everyone:
        print("⚠️ Geen ontvangers opgegeven, e-mail wordt overgeslagen.")
        return

    # 🔐 Auth via Microsoft Graph
    app = msal.ConfidentialClientApplication(
        client_id=OUTLOOK_CLIENT_ID,
        client_credential=OUTLOOK_CLIENT_SECRET,
        authority=authority,
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise Exception(result.get("error_description", "Geen geldig token ontvangen."))
    access_token = result["access_token"]

    # 🧱 Functie om een mail te bouwen
    def build_message(single_recipient):
        msg = {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            # ⛔ Geen mail meer naar jezelf — alleen BCC!
            "toRecipients": [],
            "bccRecipients": [{"emailAddress": {"address": single_recipient}}],
        }

        # Inline images
        if inline_images:
            msg.setdefault("attachments", [])
            for cid, path in inline_images.items():
                if os.path.exists(path):
                    with open(path, "rb") as img:
                        encoded = base64.b64encode(img.read()).decode("utf-8")
                    msg["attachments"].append({
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": os.path.basename(path),
                        "contentBytes": encoded,
                        "isInline": True,
                        "contentId": cid,
                    })

        # Bijlagen
        if attachments:
            msg.setdefault("attachments", [])
            for path in attachments:
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        encoded = base64.b64encode(f.read()).decode("utf-8")
                    msg["attachments"].append({
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": os.path.basename(path),
                        "contentBytes": encoded,
                    })
        return msg

    url = f"https://graph.microsoft.com/v1.0/users/{OUTLOOK_SENDER_EMAIL}/sendMail"

    # 🚀 Verstuur naar iedereen individueel
    for email_addr in everyone:
        msg = build_message(email_addr)
        r = requests.post(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            json={"message": msg, "saveToSentItems": "false"},
        )
        if r.status_code in [200, 202]:
            print(f"📩 Mail verstuurd naar {email_addr}")
        else:
            print(f"⚠️ Fout bij {email_addr}: {r.text}")

    print(f"✅ Alle mails verzonden: totaal {len(everyone)} ontvangers.")
